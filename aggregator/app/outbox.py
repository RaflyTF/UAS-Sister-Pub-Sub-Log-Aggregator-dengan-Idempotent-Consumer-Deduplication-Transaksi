"""
outbox.py — Outbox Pattern dengan duplicate detection.
write_to_outbox() sekarang return bool: True=baru, False=duplikat.
"""

from __future__ import annotations

import asyncio
import json
import logging

import aiosqlite

from .database import get_db

logger = logging.getLogger(__name__)


async def write_to_outbox(event: dict, db: aiosqlite.Connection) -> bool:
    """
    Tulis event ke outbox. Return True jika baru, False jika duplikat.
    Dipanggil di dalam transaksi caller (tidak ada BEGIN/COMMIT di sini).
    """
    # Cek dulu apakah event_id sudah ada (SELECT lebih reliable daripada changes())
    cur = await db.execute(
        "SELECT 1 FROM outbox WHERE event_id = ?",
        (event["event_id"],),
    )
    already_exists = await cur.fetchone() is not None

    if not already_exists:
        await db.execute(
            "INSERT INTO outbox (event_id, raw_payload) VALUES (?, ?)",
            (event["event_id"], json.dumps(event, default=str)),
        )
        return True   # event baru
    else:
        return False  # duplikat


async def outbox_processor_loop(broker) -> None:
    """Background task: poll outbox, publish ke Redis Streams setiap 100ms."""
    logger.info("outbox_processor_started")
    db = await get_db()

    try:
        while True:
            try:
                await _flush_pending(broker, db)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("outbox_processor_error")
                try:
                    await db.execute("ROLLBACK")
                except Exception:
                    pass
            await asyncio.sleep(0.1)

    except asyncio.CancelledError:
        logger.info("outbox_processor_stopping")
    finally:
        await db.close()
        logger.info("outbox_processor_stopped")


async def _flush_pending(broker, db: aiosqlite.Connection) -> None:
    """Ambil max 50 pending event, XADD ke Redis, update status='done'."""
    await db.execute("BEGIN IMMEDIATE")

    cur = await db.execute(
        """
        SELECT id, event_id, raw_payload, retries
        FROM   outbox
        WHERE  status = 'pending'
        ORDER  BY id ASC
        LIMIT  50
        """
    )
    rows = await cur.fetchall()

    if not rows:
        await db.execute("ROLLBACK")
        return

    ok = 0
    for row in rows:
        oid, eid, raw, retries = (
            row["id"], row["event_id"], row["raw_payload"], row["retries"]
        )
        try:
            await broker.xadd(json.loads(raw))
            await db.execute(
                "UPDATE outbox SET status='done', processed_at=datetime('now') WHERE id=?",
                (oid,),
            )
            ok += 1
        except Exception as exc:
            new_status = "failed" if retries >= 4 else "pending"
            await db.execute(
                "UPDATE outbox SET retries=retries+1, status=? WHERE id=?",
                (new_status, oid),
            )
            logger.warning(
                "outbox_publish_failed",
                extra={"event_id": eid, "retries": retries + 1, "error": str(exc)},
            )

    await db.execute("COMMIT")
    if ok:
        logger.debug("outbox_flushed", extra={"count": ok})