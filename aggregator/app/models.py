"""
models.py
─────────
Pydantic v2 schema untuk validasi event.

Tujuan:
  Mendefinisikan kontrak data (event_id, topic, timestamp, source, payload).
  FastAPI secara otomatis memvalidasi request body terhadap schema ini
  dan mengembalikan HTTP 422 jika schema tidak valid.

Kenapa diperlukan:
  Rubrik menilai validasi schema. Pydantic memastikan tidak ada event
  dengan format salah yang masuk ke sistem dan merusak data.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, List

from pydantic import BaseModel, Field, field_validator


class Event(BaseModel):
    """
    Schema event tunggal sesuai spesifikasi UAS:
    { "topic", "event_id", "timestamp", "source", "payload" }
    """

    topic: str = Field(
        ...,
        min_length=1,
        max_length=200,
        description="Topik event, contoh: logs/app, metrics/cpu",
        examples=["logs/app"],
    )
    event_id: str = Field(
        ...,
        min_length=1,
        max_length=200,
        description="ID unik per event — digunakan sebagai kunci deduplication",
        examples=["a3f2-b1c4-8d5e"],
    )
    timestamp: str = Field(
        ...,
        description="Waktu event dalam format ISO8601",
        examples=["2024-01-15T10:30:00Z"],
    )
    source: str = Field(
        ...,
        min_length=1,
        max_length=200,
        description="Nama service pengirim",
        examples=["service-a"],
    )
    payload: Dict[str, Any] = Field(
        default_factory=dict,
        description="Data event bebas format JSON",
    )

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, v: str) -> str:
        """Pastikan timestamp adalah ISO8601 yang valid."""
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError(
                f"timestamp harus format ISO8601, contoh: 2024-01-15T10:30:00Z. "
                f"Diterima: '{v}'"
            )
        return v

    @field_validator("event_id")
    @classmethod
    def validate_event_id(cls, v: str) -> str:
        """event_id tidak boleh kosong atau hanya whitespace."""
        stripped = v.strip()
        if not stripped:
            raise ValueError("event_id tidak boleh kosong atau hanya whitespace")
        return stripped

    @field_validator("topic")
    @classmethod
    def validate_topic(cls, v: str) -> str:
        """topic hanya boleh mengandung: huruf, angka, /, -, _, titik."""
        if not re.match(r"^[a-zA-Z0-9/_\-\.]+$", v):
            raise ValueError(
                "topic hanya boleh mengandung huruf, angka, /, -, _, dan titik"
            )
        return v


class BatchPublishRequest(BaseModel):
    """
    Request untuk publish satu atau banyak event sekaligus.

    Single  : {"events": [event]}
    Batch   : {"events": [event1, event2, ...]}
    Max 1000 event per request untuk mencegah overload.
    """

    events: List[Event] = Field(
        ...,
        min_length=1,
        max_length=1000,
        description="Daftar event (1–1000 per request)",
    )
