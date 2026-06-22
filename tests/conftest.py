"""
tests/conftest.py
─────────────────
Fixture bersama untuk seluruh test suite.

Strategi testing:
  - Unit tests (test_01 s/d test_06): SQLite di tmp_path, tanpa Redis.
    Bisa dijalankan tanpa Docker sama sekali.
  - TESTING=true menonaktifkan broker/workers di main.py (lifespan).

Fixture utama:
  db_path    → path string ke SQLite sementara (fresh setiap test)
  db         → koneksi aiosqlite yang sudah di-init
  test_client → httpx AsyncClient terhubung ke FastAPI (tanpa Redis)
  sample_event, sample_event_b → event dummy untuk test
"""

from __future__ import annotations

import os

import aiosqlite
import pytest
import pytest_asyncio


# ── Set env defaults sebelum apapun diimport ─────────────────────────────
os.environ.setdefault("TESTING",     "true")
os.environ.setdefault("LOG_LEVEL",   "WARNING")
os.environ.setdefault("NUM_WORKERS", "1")


@pytest_asyncio.fixture
async def db_path(tmp_path) -> str:
    """Path ke SQLite sementara (fresh per test, dihapus otomatis)."""
    return str(tmp_path / "test_events.db")


@pytest_asyncio.fixture
async def db(db_path):
    """
    Koneksi SQLite yang sudah di-init dengan semua tabel.
    Fresh untuk setiap test — tidak ada state dari test sebelumnya.
    """
    os.environ["DATABASE_PATH"] = db_path

    from aggregator.app.database import init_database
    await init_database()

    conn = await aiosqlite.connect(db_path, isolation_level=None)
    conn.row_factory = aiosqlite.Row
    await conn.execute("PRAGMA journal_mode = WAL")
    await conn.execute("PRAGMA busy_timeout = 5000")

    yield conn

    await conn.close()


@pytest_asyncio.fixture
async def test_client(db_path):
    """
    HTTP test client dengan ASGI transport.
    TESTING=true → lifespan tidak start Redis/workers.
    Database di db_path → fresh, terisolasi per test.
    """
    os.environ["DATABASE_PATH"] = db_path
    os.environ["TESTING"]       = "true"

    from aggregator.app.database import init_database
    await init_database()

    # Re-import app setelah env diset agar lifespan menggunakan db_path
    from httpx import AsyncClient, ASGITransport
    from aggregator.app.main import app

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        yield client


@pytest.fixture
def sample_event() -> dict:
    return {
        "topic":     "logs/test",
        "event_id":  "test-event-001",
        "timestamp": "2024-01-15T10:30:00Z",
        "source":    "test-service",
        "payload":   {"message": "test event", "level": "INFO"},
    }


@pytest.fixture
def sample_event_b() -> dict:
    """Event dengan event_id berbeda dari sample_event."""
    return {
        "topic":     "logs/test",
        "event_id":  "test-event-002",
        "timestamp": "2024-01-15T10:31:00Z",
        "source":    "test-service",
        "payload":   {"message": "second event"},
    }
