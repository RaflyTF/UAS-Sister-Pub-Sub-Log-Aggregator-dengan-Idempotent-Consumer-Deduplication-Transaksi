"""
broker.py
─────────
Redis Streams client untuk pub-sub.

Tujuan:
  Menyediakan interface bersih ke Redis Streams:
  - xadd()         → publisher dan FastAPI mempublikasikan event
  - xreadgroup()   → consumer workers membaca event
  - xack()         → acknowledge setelah berhasil diproses
  - create_consumer_group() → setup group saat startup

Kenapa Redis Streams (bukan Redis List):
  Redis Streams memberikan:
  1. Consumer groups: setiap message hanya di-deliver ke SATU worker
     → mencegah double-processing di level broker
  2. Message acknowledgment (XACK): at-least-once delivery
     → jika worker crash sebelum XACK, message di-deliver ulang
  3. Pending entries: bisa di-reclaim setelah worker crash
  4. Message history: bisa di-replay jika diperlukan

  Redis List (LPUSH/RPOP) tidak mendukung consumer groups,
  sehingga tidak cocok untuk multi-worker scenario ini.
"""

from __future__ import annotations

import json
import logging
import os

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)

STREAM_NAME    = "events_stream"
CONSUMER_GROUP = "aggregators"


class RedisBroker:
    """
    Singleton Redis Streams client.
    Instance dibuat di main.py dan di-inject ke consumer/outbox.
    """

    def __init__(self) -> None:
        self._client: aioredis.Redis | None = None

    async def connect(self) -> None:
        """Buka koneksi ke Redis. Dipanggil saat startup (lifespan)."""
        url = os.environ.get("BROKER_URL", "redis://localhost:6379")
        self._client = aioredis.from_url(
            url,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=10,
            retry_on_timeout=True,
        )
        await self._client.ping()
        logger.info("broker_connected", extra={"url": url})

    async def disconnect(self) -> None:
        """Tutup koneksi. Dipanggil saat shutdown (lifespan)."""
        if self._client:
            await self._client.aclose()
            logger.info("broker_disconnected")

    async def ping(self) -> bool:
        """Health check — digunakan oleh GET /health."""
        try:
            if self._client:
                await self._client.ping()
                return True
        except Exception:
            pass
        return False

    async def create_consumer_group(self) -> None:
        """
        Buat consumer group 'aggregators' jika belum ada.
        id='0'       → consumer akan membaca dari awal stream
        mkstream=True → buat stream jika belum ada
        """
        try:
            await self._client.xgroup_create(
                name=STREAM_NAME,
                groupname=CONSUMER_GROUP,
                id="0",
                mkstream=True,
            )
            logger.info("consumer_group_created", extra={"group": CONSUMER_GROUP})
        except Exception as exc:
            if "BUSYGROUP" in str(exc):
                logger.info("consumer_group_exists", extra={"group": CONSUMER_GROUP})
            else:
                raise

    async def xadd(self, event: dict) -> str:
        """
        Publish event ke Redis Stream.
        maxlen=100_000 dengan approximate=True mencegah stream tumbuh tak terbatas.
        """
        msg_id = await self._client.xadd(
            STREAM_NAME,
            {"data": json.dumps(event, default=str)},
            maxlen=100_000,
            approximate=True,
        )
        return msg_id

    async def xreadgroup(
        self,
        worker_id: str,
        count: int = 10,
        block: int = 1000,
    ) -> list[tuple[str, dict]]:
        """
        Baca message baru dari consumer group.

        '>'    → hanya message yang belum pernah di-deliver ke siapapun
        block  → tunggu N ms jika stream kosong (long-poll efisien)
        count  → maksimum message per panggilan
        """
        if not self._client:
            return []
        results = await self._client.xreadgroup(
            groupname=CONSUMER_GROUP,
            consumername=worker_id,
            streams={STREAM_NAME: ">"},
            count=count,
            block=block,
        )
        if not results:
            return []

        parsed: list[tuple[str, dict]] = []
        for _, messages in results:
            for msg_id, fields in messages:
                event = json.loads(fields["data"])
                parsed.append((msg_id, event))
        return parsed

    async def xack(self, msg_id: str) -> None:
        """
        Acknowledge message — hapus dari pending list Redis.
        Setelah XACK, message tidak akan di-deliver ulang.
        Dipanggil SETELAH event berhasil masuk ke SQLite.
        """
        await self._client.xack(STREAM_NAME, CONSUMER_GROUP, msg_id)

    async def xlen(self) -> int:
        """Panjang stream saat ini — untuk monitoring."""
        try:
            return await self._client.xlen(STREAM_NAME) if self._client else 0
        except Exception:
            return 0
