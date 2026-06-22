"""
test_03_api.py  (T08–T10)
──────────────────────────
API endpoint tests menggunakan httpx AsyncClient + ASGI transport.
Tidak memerlukan Redis (TESTING=true).
"""

import os
import pytest


class TestAPIEndpoints:

    async def test_T08_post_publish_returns_201(self, test_client, sample_event):
        """T08: POST /publish dengan event valid harus return 201 dan body yang benar."""
        resp = await test_client.post(
            "/publish",
            json={"events": [sample_event]},
        )
        assert resp.status_code == 201
        body = resp.json()
        assert body["status"] == "accepted"
        assert body["count"]  == 1
        assert sample_event["event_id"] in body["event_ids"]

    async def test_T08b_post_batch_returns_correct_count(self, test_client, sample_event, sample_event_b):
        """T08b: Batch 2 event → count = 2 di response."""
        resp = await test_client.post(
            "/publish",
            json={"events": [sample_event, sample_event_b]},
        )
        assert resp.status_code == 201
        assert resp.json()["count"] == 2

    async def test_T09_get_events_filters_by_topic(self, test_client, db_path):
        """
        T09: GET /events?topic=... hanya mengembalikan event dengan topic tersebut.
        Kita insert langsung ke DB (tanpa Redis) untuk simulasi consumer worker.
        """
        os.environ["DATABASE_PATH"] = db_path

        ev1 = {"topic": "logs/app",    "event_id": "T09-001", "timestamp": "2024-01-15T10:00:00Z", "source": "svc-a", "payload": {}}
        ev2 = {"topic": "metrics/cpu", "event_id": "T09-002", "timestamp": "2024-01-15T10:01:00Z", "source": "svc-b", "payload": {}}

        # Simulasi consumer worker: process langsung ke DB
        from aggregator.app.database import get_db
        from aggregator.app.dedup    import process_event_idempotent
        db = await get_db()
        await process_event_idempotent(ev1, "test-worker", db)
        await process_event_idempotent(ev2, "test-worker", db)
        await db.close()

        # Test filter
        resp = await test_client.get("/events", params={"topic": "logs/app"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["count"] >= 1
        for ev in body["events"]:
            assert ev["topic"] == "logs/app", "Hanya event dengan topic logs/app yang boleh muncul"

    async def test_T10_get_stats_required_fields_exist(self, test_client):
        """T10: GET /stats harus memiliki semua field yang dibutuhkan rubrik."""
        resp = await test_client.get("/stats")
        assert resp.status_code == 200
        body = resp.json()
        required = [
            "received", "unique_processed", "duplicate_dropped",
            "topics", "uptime_seconds", "workers_active",
        ]
        for field in required:
            assert field in body, f"Field '{field}' tidak ditemukan di /stats"

    async def test_T10b_post_invalid_schema_returns_422(self, test_client):
        """T10b: POST dengan schema tidak lengkap harus return 422 Unprocessable Entity."""
        resp = await test_client.post(
            "/publish",
            json={"events": [{"topic": "test"}]},   # missing event_id, timestamp, source
        )
        assert resp.status_code == 422

    async def test_T10c_health_endpoint_returns_200(self, test_client):
        """T10c: GET /health harus return 200 dengan field status."""
        resp = await test_client.get("/health")
        assert resp.status_code == 200
        body = resp.json()
        assert "status"         in body
        assert "uptime_seconds" in body
