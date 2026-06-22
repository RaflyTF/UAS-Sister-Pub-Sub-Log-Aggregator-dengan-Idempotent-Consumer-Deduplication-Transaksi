"""
test_01_schema.py  (T01–T03)
────────────────────────────
Validasi schema event menggunakan Pydantic.
Tujuan: membuktikan bahwa sistem menolak event invalid sebelum masuk ke DB.
"""

import pytest
from pydantic import ValidationError
from aggregator.app.models import Event, BatchPublishRequest


class TestSchemaValidation:

    def test_T01_valid_event_accepted(self, sample_event):
        """T01: Event dengan semua field valid harus diterima tanpa error."""
        ev = Event(**sample_event)
        assert ev.topic    == sample_event["topic"]
        assert ev.event_id == sample_event["event_id"]
        assert ev.source   == sample_event["source"]

    def test_T02_missing_event_id_rejected(self, sample_event):
        """T02: Event tanpa event_id wajib harus ditolak (ValidationError)."""
        bad = {k: v for k, v in sample_event.items() if k != "event_id"}
        with pytest.raises(ValidationError) as exc:
            Event(**bad)
        assert "event_id" in str(exc.value)

    def test_T02b_missing_topic_rejected(self, sample_event):
        """T02b: Event tanpa topic wajib harus ditolak."""
        bad = {k: v for k, v in sample_event.items() if k != "topic"}
        with pytest.raises(ValidationError):
            Event(**bad)

    def test_T03_invalid_timestamp_rejected(self, sample_event):
        """T03: Timestamp non-ISO8601 harus ditolak."""
        bad = {**sample_event, "timestamp": "15-01-2024 10:30"}
        with pytest.raises(ValidationError) as exc:
            Event(**bad)
        errors = str(exc.value).lower()
        assert "timestamp" in errors or "iso8601" in errors

    def test_T03b_whitespace_event_id_rejected(self, sample_event):
        """T03b: event_id berisi hanya whitespace harus ditolak."""
        bad = {**sample_event, "event_id": "   "}
        with pytest.raises(ValidationError):
            Event(**bad)

    def test_T03c_batch_multiple_events_accepted(self, sample_event, sample_event_b):
        """T03c: BatchPublishRequest dengan dua event valid diterima."""
        req = BatchPublishRequest(events=[sample_event, sample_event_b])
        assert len(req.events) == 2
        assert req.events[0].event_id == sample_event["event_id"]
        assert req.events[1].event_id == sample_event_b["event_id"]
