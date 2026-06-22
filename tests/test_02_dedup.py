"""
test_02_dedup.py  (T04–T07)
────────────────────────────
Idempotency & deduplication menggunakan UNIQUE constraint SQLite.
Tujuan: membuktikan event yang sama HANYA diproses SATU kali.
"""

import pytest
from aggregator.app.dedup import process_event_idempotent, PROCESSED, DUPLICATE_DROPPED


class TestDeduplication:

    async def test_T04_first_occurrence_processed(self, db, sample_event):
        """T04: Event pertama harus diproses (return PROCESSED)."""
        result = await process_event_idempotent(sample_event, "worker-0", db)
        assert result == PROCESSED

    async def test_T05_second_occurrence_dropped(self, db, sample_event):
        """T05: Event yang sama dikirim dua kali → hanya pertama PROCESSED, kedua DUPLICATE_DROPPED."""
        r1 = await process_event_idempotent(sample_event, "worker-0", db)
        r2 = await process_event_idempotent(sample_event, "worker-0", db)
        assert r1 == PROCESSED
        assert r2 == DUPLICATE_DROPPED

    async def test_T05b_ten_duplicates_all_dropped(self, db, sample_event):
        """T05b: Event yang sama dikirim 10 kali → hanya 1 PROCESSED, 9 DUPLICATE_DROPPED."""
        results = []
        for _ in range(10):
            r = await process_event_idempotent(sample_event, "worker-0", db)
            results.append(r)
        assert results.count(PROCESSED) == 1
        assert results.count(DUPLICATE_DROPPED) == 9

    async def test_T06_different_event_ids_both_processed(self, db, sample_event, sample_event_b):
        """T06: Dua event dengan event_id berbeda harus keduanya diproses."""
        r1 = await process_event_idempotent(sample_event,   "worker-0", db)
        r2 = await process_event_idempotent(sample_event_b, "worker-0", db)
        assert r1 == PROCESSED
        assert r2 == PROCESSED

        # Verifikasi 2 baris di DB
        cur = await db.execute("SELECT COUNT(*) as cnt FROM processed_events")
        row = await cur.fetchone()
        assert row["cnt"] == 2

    async def test_T07_audit_log_records_both_actions(self, db, sample_event):
        """T07: Audit log harus mencatat PROCESSED untuk event pertama
        dan DUPLICATE_DROPPED untuk yang kedua."""
        await process_event_idempotent(sample_event, "worker-0", db)
        await process_event_idempotent(sample_event, "worker-1", db)

        cur  = await db.execute(
            "SELECT action FROM audit_log WHERE event_id = ? ORDER BY id",
            (sample_event["event_id"],),
        )
        actions = [r["action"] for r in await cur.fetchall()]
        assert "PROCESSED"         in actions
        assert "DUPLICATE_DROPPED" in actions

    async def test_T07b_no_duplicate_rows_in_db(self, db, sample_event):
        """T07b: UNIQUE constraint memastikan tidak ada 2 baris dengan (topic, event_id) sama."""
        await process_event_idempotent(sample_event, "worker-0", db)
        await process_event_idempotent(sample_event, "worker-1", db)
        await process_event_idempotent(sample_event, "worker-2", db)

        cur = await db.execute(
            "SELECT COUNT(*) as cnt FROM processed_events WHERE event_id = ?",
            (sample_event["event_id"],),
        )
        row = await cur.fetchone()
        assert row["cnt"] == 1, "UNIQUE constraint harus memastikan hanya 1 baris per (topic,event_id)"
