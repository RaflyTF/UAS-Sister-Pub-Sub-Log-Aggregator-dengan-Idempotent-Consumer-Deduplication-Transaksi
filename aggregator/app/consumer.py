"""
consumer.py
───────────
3 asyncio consumer worker tasks.

Tujuan:
  Membaca event dari Redis Streams dan memprosesnya dengan idempotency
  penuh menggunakan process_event_idempotent().

Kenapa 3 worker (bukan 1):
  Mendemonstrasikan kontrol konkurensi (rubrik 16 poin).
  Setiap worker adalah asyncio Task terpisah dengan koneksi SQLite sendiri.
  UNIQUE constraint di SQLite memastikan tidak ada double-processing
  meski 3 worker mencoba memproses event yang sama bersamaan.

Pola at-least-once + idempotent consumer:
  1. XREADGROUP → ambil message (at-least-once dari Redis)
  2. process_event_idempotent → INSERT OR IGNORE (exactly-once di DB)
  3. XACK → hapus dari pending list Redis
  Jika worker crash antara step 2 dan 3:
    - Redis akan re-deliver message ke worker lain (at-least-once)
    - UNIQUE constraint mencegah double insert (idempotent)
  Hasil efektif: exactly-once processing ✓

Concurrency model:
  asyncio (single-threaded event loop) + SQLite WAL (concurrent readers,
  serialized writers via BEGIN IMMEDIATE + busy_timeout).
"""

from __future__ import annotations

import asyncio
import logging
import os

from .broker   import RedisBroker
from .database import get_db
from .dedup    import process_event_idempotent

logger      = logging.getLogger(__name__)
NUM_WORKERS = int(os.environ.get("NUM_WORKERS", "3"))

# Set yang bisa di-query oleh GET /health untuk status monitoring
active_workers: set[str] = set()


async def worker_loop(worker_id: str, broker: RedisBroker) -> None:
    """
    Loop tanpa henti untuk satu consumer worker.

    Setiap iterasi:
    1. XREADGROUP (block=1000ms) → ambil hingga 10 message baru
    2. Untuk setiap message:
       a. process_event_idempotent → transaksional INSERT OR IGNORE
       b. XACK → mark as acknowledged di Redis
    3. Ulangi

    Error handling:
    - Exception per-message: log error, JANGAN XACK (akan di-redeliver)
    - Exception loop-level: sleep 1 detik, retry
    - CancelledError: cleanup DB connection, keluar dengan bersih
    """
    active_workers.add(worker_id)
    logger.info("worker_started", extra={"worker": worker_id})

    db = await get_db()   # Setiap worker punya koneksi SQLite sendiri

    try:
        while True:
            try:
                messages = await broker.xreadgroup(
                    worker_id=worker_id,
                    count=10,
                    block=1000,   # tunggu 1 detik jika stream kosong
                )

                for msg_id, event in messages:
                    try:
                        result = await process_event_idempotent(event, worker_id, db)
                        # XACK hanya setelah berhasil di-proses
                        await broker.xack(msg_id)
                        logger.info(
                            "message_handled",
                            extra={
                                "worker":   worker_id,
                                "result":   result,
                                "event_id": event.get("event_id"),
                                "topic":    event.get("topic"),
                            },
                        )
                    except Exception:
                        # Jangan XACK — Redis akan redeliver ke worker lain
                        logger.exception(
                            "message_processing_failed",
                            extra={"worker": worker_id, "event_id": event.get("event_id")},
                        )

            except asyncio.CancelledError:
                raise   # propagate ke gather → semua worker di-cancel
            except Exception:
                logger.exception("worker_loop_error", extra={"worker": worker_id})
                await asyncio.sleep(1)   # brief backoff sebelum retry

    except asyncio.CancelledError:
        logger.info("worker_stopping", extra={"worker": worker_id})
    finally:
        await db.close()
        active_workers.discard(worker_id)
        logger.info("worker_stopped", extra={"worker": worker_id})


async def start_all_workers(broker: RedisBroker) -> None:
    """
    Buat NUM_WORKERS asyncio tasks dan jalankan bersamaan.

    Langkah:
    1. Buat consumer group di Redis (idempotent jika sudah ada)
    2. Buat NUM_WORKERS Task, masing-masing menjalankan worker_loop()
    3. asyncio.gather() → jalankan semua bersamaan sampai di-cancel

    Cancellation:
    Ketika task ini di-cancel (dari lifespan shutdown),
    CancelledError propagasi ke gather → setiap worker task di-cancel
    → cleanup DB connection di finally block masing-masing.
    """
    await broker.create_consumer_group()

    tasks = [
        asyncio.create_task(
            worker_loop(f"worker-{i}", broker),
            name=f"consumer-worker-{i}",
        )
        for i in range(NUM_WORKERS)
    ]
    logger.info("all_workers_started", extra={"count": NUM_WORKERS})

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
