from __future__ import annotations

import logging
import os
from pathlib import Path

import aiosqlite

logger = logging.getLogger(__name__)


def _get_db_path() -> str:
    return os.environ.get("DATABASE_PATH", "/var/lib/aggregator/events.db")


async def init_database() -> None:
    """Inisialisasi database: buat folder, aktifkan WAL, buat semua tabel."""
    db_path = _get_db_path()
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    async with aiosqlite.connect(db_path, isolation_level=None, timeout=30) as db:
        # ── PRAGMA performa & safety ──────────────────────────────────
        await db.execute("PRAGMA journal_mode = WAL")
        await db.execute("PRAGMA synchronous  = NORMAL")
        await db.execute("PRAGMA foreign_keys = ON")
        await db.execute("PRAGMA busy_timeout = 10000")
        # Optimasi performa (penting untuk Windows):
        await db.execute("PRAGMA cache_size   = -65536")   # 64MB memory cache
        await db.execute("PRAGMA temp_store   = MEMORY")   # temp table di RAM
        await db.execute("PRAGMA mmap_size    = 268435456") # 256MB memory-mapped I/O

        await db.execute("BEGIN")

        await db.execute("""
            CREATE TABLE IF NOT EXISTS processed_events (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                topic        TEXT    NOT NULL,
                event_id     TEXT    NOT NULL,
                source       TEXT    NOT NULL,
                payload      TEXT    NOT NULL,
                received_at  TEXT    NOT NULL,
                processed_at TEXT    NOT NULL DEFAULT (datetime('now')),
                worker_id    TEXT    NOT NULL,
                CONSTRAINT uq_event UNIQUE (topic, event_id)
            )
        """)
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_pe_topic    ON processed_events(topic)"
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_pe_event_id ON processed_events(event_id)"
        )

        await db.execute("""
            CREATE TABLE IF NOT EXISTS outbox (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id     TEXT    NOT NULL UNIQUE,
                raw_payload  TEXT    NOT NULL,
                status       TEXT    NOT NULL DEFAULT 'pending',
                retries      INTEGER NOT NULL DEFAULT 0,
                created_at   TEXT    NOT NULL DEFAULT (datetime('now')),
                processed_at TEXT
            )
        """)
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox(status)"
        )

        await db.execute("""
            CREATE TABLE IF NOT EXISTS audit_log (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id  TEXT NOT NULL,
                action    TEXT NOT NULL,
                worker_id TEXT,
                timestamp TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_audit_event ON audit_log(event_id)"
        )

        await db.execute("""
            CREATE TABLE IF NOT EXISTS stats (
                key   TEXT PRIMARY KEY,
                value INTEGER NOT NULL DEFAULT 0
            )
        """)
        await db.execute("INSERT OR IGNORE INTO stats(key,value) VALUES('received',          0)")
        await db.execute("INSERT OR IGNORE INTO stats(key,value) VALUES('unique_processed',  0)")
        await db.execute("INSERT OR IGNORE INTO stats(key,value) VALUES('duplicate_dropped', 0)")

        await db.execute("COMMIT")

    logger.info("db_initialized", extra={"path": db_path})


async def get_db() -> aiosqlite.Connection:
    """
    Buka koneksi SQLite baru.
    timeout=30        → tunggu 30 detik di level Python jika DB terkunci
    cache_size -65536 → 64MB memory cache per koneksi
    temp_store MEMORY → operasi temporary di RAM
    """
    db_path = _get_db_path()
    db = await aiosqlite.connect(db_path, isolation_level=None, timeout=30)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode = WAL")
    await db.execute("PRAGMA synchronous  = NORMAL")
    await db.execute("PRAGMA busy_timeout = 10000")
    await db.execute("PRAGMA cache_size   = -65536")    # 64MB cache
    await db.execute("PRAGMA temp_store   = MEMORY")    # temp di RAM
    await db.execute("PRAGMA mmap_size    = 268435456") # 256MB mmap
    return db
