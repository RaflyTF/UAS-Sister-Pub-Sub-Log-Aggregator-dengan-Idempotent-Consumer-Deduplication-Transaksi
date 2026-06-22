"""
test_04_concurrency.py  (T11–T13)
──────────────────────────────────
Race condition & konkurensi.
Membuktikan bahwa BEGIN IMMEDIATE + UNIQUE constraint mencegah double-processing
bahkan ketika banyak worker berjalan bersamaan.
"""

import asyncio
import os
import uuid

import aiosqlite
import pytest

from aggregator.app.dedup import process_event_idempotent, PROCESSED, DUPLICATE_DROPPED


async def _make_conn(db_path: str) -> aiosqlite.Connection:
    """
    Helper: buka koneksi SQLite dengan konfigurasi standar.
    timeout=30 → tunggu 30 detik jika write lock sedang dipegang worker lain.
    Ini FIX untuk Windows di mana PRAGMA busy_timeout kurang reliable.
    """
    conn = await aiosqlite.connect(db_path, isolation_level=None, timeout=30)
    conn.row_factory = aiosqlite.Row
    await conn.execute("PRAGMA journal_mode = WAL")
    await conn.execute("PRAGMA busy_timeout = 10000")
    return conn


class TestConcurrencyControl:

    async def test_T11_three_workers_same_event_no_double_process(self, db_path, sample_event):
        """
        T11: 3 worker asyncio mengirim event YANG SAMA bersamaan.
        Hanya SATU harus sukses (PROCESSED), dua lainnya DUPLICATE_DROPPED.
        """
        os.environ["DATABASE_PATH"] = db_path
        from aggregator.app.database import init_database
        await init_database()

        conns = [await _make_conn(db_path) for _ in range(3)]
        tasks = [
            process_event_idempotent(sample_event, f"worker-{i}", conns[i])
            for i in range(3)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for conn in conns:
            await conn.close()

        processed = sum(1 for r in results if r == PROCESSED)
        dropped   = sum(1 for r in results if r == DUPLICATE_DROPPED)
        errors    = sum(1 for r in results if isinstance(r, Exception))

        assert errors    == 0, f"Tidak boleh ada exception: {[r for r in results if isinstance(r, Exception)]}"
        assert processed == 1, f"Harus tepat 1 PROCESSED, dapat {processed}"
        assert dropped   == 2, f"Harus tepat 2 DUPLICATE_DROPPED, dapat {dropped}"

    async def test_T12_concurrent_unique_events_all_processed(self, db_path):
        """
        T12: 100 event UNIK diproses oleh 5 worker bersamaan.
        unique_processed di stats harus tepat 100.
        """
        os.environ["DATABASE_PATH"] = db_path
        from aggregator.app.database import init_database, get_db
        await init_database()

        events = [
            {"topic": "test/concurrent", "event_id": str(uuid.uuid4()),
             "timestamp": "2024-01-15T10:00:00Z", "source": "test", "payload": {}}
            for _ in range(100)
        ]

        async def worker(wid: str, evs: list):
            conn = await _make_conn(db_path)
            try:
                for ev in evs:
                    await process_event_idempotent(ev, wid, conn)
            finally:
                await conn.close()

        chunk = 20
        await asyncio.gather(*[
            worker(f"worker-{i}", events[i*chunk:(i+1)*chunk])
            for i in range(5)
        ])

        db = await get_db()
        cur = await db.execute("SELECT value FROM stats WHERE key='unique_processed'")
        row = await cur.fetchone()
        await db.close()

        assert row["value"] == 100, f"unique_processed harus 100, dapat {row['value']}"

    async def test_T13_ten_workers_same_event_zero_errors(self, db_path):
        """
        T13: 10 worker mengirim event SAMA secara bersamaan.
        Hasil: 1 PROCESSED, 9 DUPLICATE_DROPPED, 0 error.
        """
        os.environ["DATABASE_PATH"] = db_path
        from aggregator.app.database import init_database
        await init_database()

        event = {
            "topic":     "test/race",
            "event_id":  "race-condition-final-boss",
            "timestamp": "2024-01-15T10:00:00Z",
            "source":    "test",
            "payload":   {},
        }
        conns   = [await _make_conn(db_path) for _ in range(10)]
        tasks   = [process_event_idempotent(event, f"w-{i}", conns[i]) for i in range(10)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for conn in conns:
            await conn.close()

        errors    = [r for r in results if isinstance(r, Exception)]
        processed = sum(1 for r in results if r == PROCESSED)

        assert len(errors) == 0, f"Harus 0 exception, dapat: {errors}"
        assert processed   == 1, f"Harus 1 PROCESSED, dapat {processed}"