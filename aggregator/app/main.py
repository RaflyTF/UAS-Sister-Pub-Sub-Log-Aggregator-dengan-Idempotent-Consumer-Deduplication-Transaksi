"""
main.py — FastAPI application.

Perbaikan duplicate_dropped:
  write_to_outbox() return True=baru / False=duplikat.
  Jika False → increment duplicate_dropped langsung di sini,
  karena event tidak akan pernah mencapai consumer worker.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query

from .broker         import RedisBroker
from .consumer       import active_workers, start_all_workers
from .database       import get_db, init_database
from .dedup          import write_audit_log
from .logging_config import setup_logging
from .models         import BatchPublishRequest
from .outbox         import outbox_processor_loop, write_to_outbox

setup_logging()
logger = logging.getLogger(__name__)

broker     = RedisBroker()
start_time = time.time()
TESTING    = os.environ.get("TESTING", "false").lower() == "true"


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("aggregator_starting")
    await init_database()
    if not TESTING:
        await broker.connect()
        ct = asyncio.create_task(start_all_workers(broker),     name="consumers")
        ot = asyncio.create_task(outbox_processor_loop(broker), name="outbox")
    logger.info("aggregator_ready")
    yield
    logger.info("aggregator_shutting_down")
    if not TESTING:
        ct.cancel()
        ot.cancel()
        await asyncio.gather(ct, ot, return_exceptions=True)
        await broker.disconnect()
    logger.info("aggregator_stopped")


app = FastAPI(
    title="Pub-Sub Log Aggregator",
    description=(
        "Distributed log aggregator dengan idempotent consumer, deduplication, "
        "transaksi/kontrol konkurensi. Referensi: Coulouris et al., Bab 1-13."
    ),
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health", tags=["observability"])
async def health():
    broker_ok = await broker.ping() if not TESTING else True
    return {
        "status":         "healthy" if broker_ok else "degraded",
        "broker":         "up"      if broker_ok else "down",
        "workers_active": len(active_workers),
        "uptime_seconds": round(time.time() - start_time, 1),
    }


@app.post("/publish", status_code=201, tags=["events"])
async def publish(request: BatchPublishRequest):
    """
    Terima batch event secara transaksional.

    LOGIKA COUNTER (setelah fix):
      received          → +1 untuk SETIAP event masuk (termasuk duplikat)
      duplicate_dropped → +1 jika event_id sudah ada di outbox (ditangkap di sini)
      unique_processed  → +1 oleh consumer worker (via process_event_idempotent)

    Invariant yang harus terpenuhi (eventual):
      received == unique_processed + duplicate_dropped ✓
    """
    db = await get_db()
    accepted: list[str] = []

    try:
        await db.execute("BEGIN IMMEDIATE")

        for event in request.events:
            ed = event.model_dump()

            # Selalu increment received (untuk semua event, termasuk duplikat)
            await db.execute(
                "UPDATE stats SET value = value + 1 WHERE key = 'received'"
            )

            # write_to_outbox: True = event baru, False = duplikat
            is_new = await write_to_outbox(ed, db)

            if is_new:
                # Event baru: catat ke audit log, akan diproses worker
                await write_audit_log(db, event.event_id, "RECEIVED", worker_id=None)
                logger.debug("event_received", extra={"event_id": event.event_id})
            else:
                # Duplikat: event_id sudah ada di outbox
                # Consumer worker TIDAK akan pernah melihat event ini,
                # jadi kita harus increment counter di sini
                await db.execute(
                    "UPDATE stats SET value = value + 1 WHERE key = 'duplicate_dropped'"
                )
                await write_audit_log(db, event.event_id, "DUPLICATE_DROPPED", worker_id=None)
                logger.info(
                    "duplicate_dropped_at_api",
                    extra={"event_id": event.event_id, "topic": event.topic},
                )

            accepted.append(event.event_id)

        await db.execute("COMMIT")

    except Exception as exc:
        try:
            await db.execute("ROLLBACK")
        except Exception:
            pass
        logger.exception("publish_failed", extra={"count": len(request.events)})
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        await db.close()

    logger.info("publish_accepted", extra={"count": len(accepted)})
    return {"status": "accepted", "count": len(accepted), "event_ids": accepted}


@app.get("/events", tags=["events"])
async def get_events(
    topic:  str | None = Query(None, description="Filter berdasarkan topik"),
    limit:  int        = Query(100,  ge=1, le=10_000),
    offset: int        = Query(0,    ge=0),
):
    db = await get_db()
    try:
        if topic:
            cur = await db.execute(
                """SELECT id, topic, event_id, source, payload,
                          received_at, processed_at, worker_id
                   FROM   processed_events
                   WHERE  topic = ?
                   ORDER  BY processed_at DESC
                   LIMIT  ? OFFSET ?""",
                (topic, limit, offset),
            )
        else:
            cur = await db.execute(
                """SELECT id, topic, event_id, source, payload,
                          received_at, processed_at, worker_id
                   FROM   processed_events
                   ORDER  BY processed_at DESC
                   LIMIT  ? OFFSET ?""",
                (limit, offset),
            )
        rows = await cur.fetchall()
        events = [
            {
                "id":           row["id"],
                "topic":        row["topic"],
                "event_id":     row["event_id"],
                "source":       row["source"],
                "payload":      json.loads(row["payload"]),
                "received_at":  row["received_at"],
                "processed_at": row["processed_at"],
                "worker_id":    row["worker_id"],
            }
            for row in rows
        ]
        return {"events": events, "count": len(events), "topic_filter": topic}
    finally:
        await db.close()


@app.get("/stats", tags=["observability"])
async def get_stats():
    db = await get_db()
    try:
        cur  = await db.execute("SELECT key, value FROM stats")
        raw  = {r["key"]: r["value"] for r in await cur.fetchall()}

        cur2   = await db.execute(
            "SELECT DISTINCT topic FROM processed_events ORDER BY topic"
        )
        topics = [r["topic"] for r in await cur2.fetchall()]

        uptime = time.time() - start_time
        h, rem = divmod(int(uptime), 3600)
        m, s   = divmod(rem, 60)

        return {
            "received":          raw.get("received",          0),
            "unique_processed":  raw.get("unique_processed",  0),
            "duplicate_dropped": raw.get("duplicate_dropped", 0),
            "topics":            topics,
            "topic_count":       len(topics),
            "workers_active":    len(active_workers),
            "uptime_seconds":    round(uptime, 1),
            "uptime_human":      f"{h:02d}:{m:02d}:{s:02d}",
        }
    finally:
        await db.close()