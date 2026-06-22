"""
publisher.py
────────────
Event generator & simulator duplikasi.

Tujuan:
  Mensimulasikan publisher yang mengirim event ke sistem aggregator,
  termasuk duplikasi yang disengaja (default 35%) untuk menguji
  idempotency dan deduplication.

Fitur:
  - Kirim 25.000 event dalam batch 50 (configurable via env)
  - 35% event adalah duplikat dari event yang pernah dikirim sebelumnya
  - Retry dengan exponential backoff (0.5s → 1s → 2s → 4s → 8s, max 30s)
  - Tunggu aggregator healthy sebelum mulai kirim
  - Structured logging progress setiap 1000 event

Cara menjalankan:
  docker compose up    (otomatis)
  python publisher.py  (manual, set TARGET_URL dulu)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone

import httpx

# ── Konfigurasi via environment variable ─────────────────────────────────
TARGET_URL     = os.environ.get("TARGET_URL",     "http://aggregator:8080/publish")
EVENTS_TOTAL   = int(os.environ.get("EVENTS_TOTAL",   "25000"))
DUPLICATE_RATE = float(os.environ.get("DUPLICATE_RATE", "0.35"))
BATCH_SIZE     = int(os.environ.get("BATCH_SIZE",     "50"))
LOG_LEVEL      = os.environ.get("LOG_LEVEL",     "INFO")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format='{"ts":"%(asctime)s","level":"%(levelname)s","service":"publisher","msg":"%(message)s"}',
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger("publisher")

# Topik dan source yang disimulasikan
TOPICS  = ["logs/app", "logs/error", "logs/audit", "metrics/cpu", "metrics/mem", "events/order"]
SOURCES = ["service-a", "service-b", "service-c", "gateway", "worker-pool"]
LEVELS  = ["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"]


class EventGenerator:
    """Generate event acak dan duplikat sesuai DUPLICATE_RATE."""

    def __init__(self) -> None:
        self._recent: list[str] = []    # event_id yang pernah dikirim

    def _fresh(self) -> dict:
        return {
            "topic":     random.choice(TOPICS),
            "event_id":  str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source":    random.choice(SOURCES),
            "payload": {
                "message": f"event at t={time.monotonic():.4f}",
                "level":   random.choice(LEVELS),
                "value":   round(random.uniform(0.0, 100.0), 4),
                "seq":     len(self._recent),
            },
        }

    def next(self) -> tuple[dict, bool]:
        """
        Kembalikan (event, is_duplicate).
        Jika is_duplicate=True, event_id diambil dari event yang pernah dikirim.
        """
        event = self._fresh()
        is_dup = bool(self._recent) and random.random() < DUPLICATE_RATE

        if is_dup:
            # Timpa event_id dengan yang sudah pernah dikirim
            event["event_id"] = random.choice(self._recent[-2000:])
        else:
            self._recent.append(event["event_id"])
            if len(self._recent) > 5000:
                self._recent = self._recent[-5000:]

        return event, is_dup


class Publisher:
    """Kirim event dalam batch ke POST /publish dengan retry exponential backoff."""

    def __init__(self) -> None:
        self.gen          = EventGenerator()
        self.sent         = 0
        self.duplicates   = 0
        self.failed_batch = 0

    # ── Health check: tunggu aggregator siap ─────────────────────────────
    async def _wait_healthy(self, client: httpx.AsyncClient, max_sec: int = 90) -> None:
        health_url = TARGET_URL.rsplit("/publish", 1)[0] + "/health"
        logger.info(f"Waiting for aggregator at {health_url} ...")
        deadline = time.monotonic() + max_sec
        while time.monotonic() < deadline:
            try:
                r = await client.get(health_url, timeout=5.0)
                if r.status_code == 200:
                    logger.info("Aggregator ready — starting publish")
                    return
            except Exception:
                pass
            await asyncio.sleep(3)
        raise RuntimeError(f"Aggregator not healthy after {max_sec}s")

    # ── Kirim satu batch dengan exponential backoff ───────────────────────
    async def _send(self, client: httpx.AsyncClient, events: list[dict]) -> bool:
        delay = 0.5
        for attempt in range(1, 6):
            try:
                resp = await client.post(
                    TARGET_URL,
                    content=json.dumps({"events": events}),
                    headers={"Content-Type": "application/json"},
                    timeout=30.0,
                )
                if resp.status_code in (200, 201):
                    return True
                logger.warning(f"HTTP {resp.status_code} attempt {attempt}/5")
            except httpx.RequestError as exc:
                logger.warning(f"Request error attempt {attempt}/5: {exc}")
            if attempt < 5:
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30.0)
        self.failed_batch += 1
        return False

    # ── Loop utama ────────────────────────────────────────────────────────
    async def run(self) -> None:
        logger.info(
            f"Publisher start: total={EVENTS_TOTAL} "
            f"dup_rate={DUPLICATE_RATE:.0%} batch={BATCH_SIZE}"
        )
        async with httpx.AsyncClient() as client:
            await self._wait_healthy(client)

            batch: list[dict] = []
            t0 = time.monotonic()

            for _ in range(EVENTS_TOTAL):
                event, is_dup = self.gen.next()
                if is_dup:
                    self.duplicates += 1
                batch.append(event)

                if len(batch) >= BATCH_SIZE:
                    if await self._send(client, batch):
                        self.sent += len(batch)
                    if self.sent % 1000 < BATCH_SIZE:
                        elapsed = time.monotonic() - t0
                        rate    = self.sent / elapsed if elapsed else 0
                        logger.info(
                            f"Progress: {self.sent}/{EVENTS_TOTAL} "
                            f"({rate:.0f} ev/s) | dups={self.duplicates} "
                            f"| failed_batches={self.failed_batch}"
                        )
                    batch = []

            if batch:
                if await self._send(client, batch):
                    self.sent += len(batch)

        elapsed = time.monotonic() - t0
        logger.info(
            f"Done: sent={self.sent} | dups_sent={self.duplicates} "
            f"| elapsed={elapsed:.1f}s | rate={self.sent/elapsed:.0f} ev/s "
            f"| failed_batches={self.failed_batch}"
        )


if __name__ == "__main__":
    asyncio.run(Publisher().run())
