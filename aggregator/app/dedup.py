"""
dedup.py
────────
Inti idempotency & deduplication sistem.

Tujuan:
  Menjamin bahwa event (topic, event_id) yang sama HANYA diproses SATU
  kali, bahkan jika dikirim berkali-kali atau diproses oleh worker berbeda
  secara bersamaan.

Strategi (sesuai rubrik Bab 8-9):
  1. BEGIN IMMEDIATE  → acquire write lock segera, cegah race condition
  2. INSERT OR IGNORE → atomik: berhasil insert = event baru; ter-ignore = duplikat
  3. SELECT changes() → deteksi apakah insert berhasil (1) atau diabaikan (0)
  4. UPDATE stats     → counter transaksional, bebas lost-update
  5. INSERT audit_log → rekam jejak setiap aksi
  6. COMMIT           → semua atomik

Race condition scenario (2 worker, event sama):
  Worker A: BEGIN IMMEDIATE → INSERT → changes()=1 → PROCESSED  → COMMIT
  Worker B: BEGIN IMMEDIATE → (tunggu busy_timeout karena A pegang lock)
         → INSERT OR IGNORE → changes()=0 → DUPLICATE_DROPPED → COMMIT
  Hasil: ZERO double-processing ✓

Isolation level:
  SQLite WAL + BEGIN IMMEDIATE efektif setara READ COMMITTED dengan
  serialisasi write. Tidak ada phantom reads karena setiap write transaction
  memegang exclusive write lock.
"""

from __future__ import annotations

import json
import logging

import aiosqlite

logger = logging.getLogger(__name__)

# Konstanta return value — gunakan ini di test untuk assertion
PROCESSED         = "PROCESSED"
DUPLICATE_DROPPED = "DUPLICATE_DROPPED"


async def process_event_idempotent(
    event: dict,
    worker_id: str,
    db: aiosqlite.Connection,
) -> str:
    """
    Proses satu event dengan jaminan idempotency penuh.

    Args:
        event:     dict event dengan field topic, event_id, source, timestamp, payload
        worker_id: ID string worker yang memanggil (untuk audit log)
        db:        koneksi aiosqlite dengan isolation_level=None (manual transaction)

    Returns:
        PROCESSED         — event baru, berhasil disimpan
        DUPLICATE_DROPPED — duplikat, diabaikan dengan aman

    Raises:
        Exception — jika terjadi error database (di-rollback otomatis)
    """
    topic    = event.get("topic",    "")
    event_id = event.get("event_id", "")

    try:
        # BEGIN IMMEDIATE: acquire write lock segera.
        # Jika worker lain sedang menulis, KITA AKAN MENUNGGU (busy_timeout=5000ms)
        # bukan langsung error. Ini mencegah "database is locked" di bawah beban.
        await db.execute("BEGIN IMMEDIATE")

        # INSERT OR IGNORE:
        # - Jika (topic, event_id) BELUM ada → INSERT berhasil → changes() = 1
        # - Jika (topic, event_id) SUDAH ada → silently ignored → changes() = 0
        # TIDAK ada exception, TIDAK ada partial state.
        await db.execute(
            """
            INSERT OR IGNORE INTO processed_events
                (topic, event_id, source, payload, received_at, worker_id)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                topic,
                event_id,
                event.get("source",    "unknown"),
                json.dumps(event.get("payload", {}), default=str),
                event.get("timestamp", ""),
                worker_id,
            ),
        )

        # changes() mengembalikan jumlah baris yang BENAR-BENAR berubah
        # dalam statement terakhir di koneksi ini.
        cur = await db.execute("SELECT changes()")
        row = await cur.fetchone()
        inserted = row[0]

        if inserted == 1:
            # ── Event baru ─────────────────────────────────────────────
            # Update stats dan audit_log dalam transaksi yang SAMA
            await db.execute(
                "UPDATE stats SET value = value + 1 WHERE key = 'unique_processed'"
            )
            await write_audit_log(db, event_id, "PROCESSED", worker_id)
            await db.execute("COMMIT")

            logger.info(
                "event_processed",
                extra={"event_id": event_id, "topic": topic, "worker": worker_id},
            )
            return PROCESSED

        else:
            # ── Duplikat — abaikan dengan aman ─────────────────────────
            await db.execute(
                "UPDATE stats SET value = value + 1 WHERE key = 'duplicate_dropped'"
            )
            await write_audit_log(db, event_id, "DUPLICATE_DROPPED", worker_id)
            await db.execute("COMMIT")

            logger.info(
                "duplicate_dropped",
                extra={"event_id": event_id, "topic": topic, "worker": worker_id},
            )
            return DUPLICATE_DROPPED

    except Exception:
        # Rollback agar DB tidak tertinggal dalam keadaan setengah-jadi
        try:
            await db.execute("ROLLBACK")
        except Exception:
            pass
        logger.exception(
            "transaction_failed",
            extra={"event_id": event_id, "worker": worker_id},
        )
        raise


async def write_audit_log(
    db: aiosqlite.Connection,
    event_id: str,
    action: str,
    worker_id: str | None = None,
) -> None:
    """
    Tulis ke tabel audit_log DALAM transaksi yang sedang berjalan.
    TIDAK memanggil COMMIT — caller yang bertanggung jawab commit/rollback.

    action: "RECEIVED" | "PROCESSED" | "DUPLICATE_DROPPED"
    """
    await db.execute(
        "INSERT INTO audit_log (event_id, action, worker_id) VALUES (?, ?, ?)",
        (event_id, action, worker_id),
    )
