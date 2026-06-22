"""
test_05_persistence.py  (T14–T15)
───────────────────────────────────
Persistensi data.
Membuktikan bahwa data tersimpan di file SQLite (named volume)
dan tetap ada setelah koneksi ditutup dan dibuka ulang
(simulasi restart container).
"""

import os
import aiosqlite
import pytest

from aggregator.app.dedup import process_event_idempotent, PROCESSED, DUPLICATE_DROPPED


class TestPersistence:

    async def test_T14_dedup_survives_reconnect(self, db_path, sample_event):
        """
        T14: Setelah koneksi ditutup dan dibuka ulang (simulasi restart),
        dedup masih bekerja — event yang sama dikenali sebagai duplikat.

        Ini membuktikan: data di named volume TIDAK hilang saat container restart.
        """
        os.environ["DATABASE_PATH"] = db_path
        from aggregator.app.database import init_database

        # ── Sesi 1: proses event ───────────────────────────────────────
        await init_database()
        db1 = await aiosqlite.connect(db_path, isolation_level=None)
        db1.row_factory = aiosqlite.Row
        await db1.execute("PRAGMA journal_mode = WAL")
        await db1.execute("PRAGMA busy_timeout = 5000")

        r1 = await process_event_idempotent(sample_event, "worker-0", db1)
        await db1.close()   # ← tutup koneksi = simulasi container shutdown
        assert r1 == PROCESSED

        # ── Sesi 2: koneksi BARU ke file yang SAMA ────────────────────
        db2 = await aiosqlite.connect(db_path, isolation_level=None)
        db2.row_factory = aiosqlite.Row
        await db2.execute("PRAGMA journal_mode = WAL")
        await db2.execute("PRAGMA busy_timeout = 5000")

        r2 = await process_event_idempotent(sample_event, "worker-0", db2)
        await db2.close()

        assert r2 == DUPLICATE_DROPPED, (
            "Data dari sesi sebelumnya harus masih ada — dedup harus bekerja setelah reconnect"
        )

    async def test_T15_stats_persist_across_reconnect(self, db_path, sample_event):
        """
        T15: Counter stats (unique_processed) harus persisten setelah reconnect.
        Nilai tidak reset ke 0 saat koneksi baru dibuka.
        """
        os.environ["DATABASE_PATH"] = db_path
        from aggregator.app.database import init_database, get_db

        await init_database()

        # Sesi 1: proses event → unique_processed jadi 1
        db1 = await aiosqlite.connect(db_path, isolation_level=None)
        db1.row_factory = aiosqlite.Row
        await db1.execute("PRAGMA journal_mode = WAL")
        await db1.execute("PRAGMA busy_timeout = 5000")
        await process_event_idempotent(sample_event, "worker-0", db1)
        await db1.close()

        # Sesi 2: baca stats dari koneksi baru
        db2 = await get_db()
        cur = await db2.execute("SELECT value FROM stats WHERE key='unique_processed'")
        row = await cur.fetchone()
        await db2.close()

        assert row["value"] == 1, (
            f"unique_processed harus 1 setelah reconnect, dapat {row['value']}"
        )
