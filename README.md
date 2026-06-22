# UAS: Pub-Sub Log Aggregator Terdistribusi

> **Sistem Pub-Sub Log Aggregator** dengan Idempotent Consumer, Deduplication,
> dan Transaksi/Kontrol Konkurensi menggunakan Docker Compose.
>
> Mata Kuliah: Sistem Paralel dan Terdistribusi | Bahasa: Python 3.11

---

## Arsitektur Singkat

```
Publisher ──POST /publish──► Aggregator (FastAPI)
                                      │ XADD
                              Redis Streams (broker)
                                      │ XREAD
                          Consumer Workers × 3 (asyncio)
                                      │ BEGIN IMMEDIATE
                          SQLite WAL ── UNIQUE(topic, event_id)
                                      │
                          GET /events · GET /stats · GET /health
```

---

## ⚡ Cara Menjalankan (Step by Step)

### Prasyarat
- Docker Desktop / Docker Engine 24+
- Docker Compose v2 (`docker compose version`)

### Langkah 1 — Clone / extract project

```bash
cd uas-log-aggregator
```

### Langkah 2 — Build dan jalankan seluruh stack

```bash
docker compose up --build
```

Tunggu hingga muncul log: `aggregator_ready`
(biasanya 15–30 detik pertama kali karena download image)

### Langkah 3 — Cek health

```bash
curl http://localhost:8080/health
```

Response yang diharapkan:
```json
{"status":"healthy","broker":"up","workers_active":3,"uptime_seconds":12.4}
```

### Langkah 4 — Lihat statistik (saat publisher berjalan)

```bash
curl http://localhost:8080/stats
```

### Langkah 5 — Lihat event yang sudah diproses

```bash
# Semua event
curl "http://localhost:8080/events?limit=10"

# Filter per topik
curl "http://localhost:8080/events?topic=logs/app&limit=10"
```

### Langkah 6 — Publish event manual (opsional)

```bash
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{
    "events": [{
      "topic": "logs/manual",
      "event_id": "my-unique-id-001",
      "timestamp": "2024-01-15T10:00:00Z",
      "source": "manual-test",
      "payload": {"message": "hello world"}
    }]
  }'
```

### Langkah 7 — Kirim event yang sama lagi (test dedup)

```bash
# Kirim ulang event yang sama → duplicate_dropped bertambah
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{
    "events": [{
      "topic": "logs/manual",
      "event_id": "my-unique-id-001",
      "timestamp": "2024-01-15T10:00:00Z",
      "source": "manual-test",
      "payload": {"message": "hello world"}
    }]
  }'

curl http://localhost:8080/stats
# Perhatikan: unique_processed TIDAK bertambah, duplicate_dropped bertambah +1
```

### Langkah 8 — Demo persistensi (penting untuk video!)

```bash
# Cek stats sebelum
curl http://localhost:8080/stats

# Hapus container (BUKAN volume)
docker compose down

# Jalankan ulang
docker compose up --build

# Cek stats → data masih ada!
curl http://localhost:8080/stats

# Kirim event yang pernah ada → DUPLICATE_DROPPED (bukan processed lagi)
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{"events": [{"topic":"logs/manual","event_id":"my-unique-id-001","timestamp":"2024-01-15T10:00:00Z","source":"test","payload":{}}]}'

curl http://localhost:8080/stats
# duplicate_dropped bertambah → DATA PERSISTEN TERBUKTI ✓
```

### Langkah 9 — Stop semua

```bash
docker compose down          # hentikan container, volume TETAP ada
docker compose down -v       # hentikan + HAPUS volume (reset total)
```

---

## 🧪 Menjalankan Tests (18 Test Cases)

### Prasyarat Python

```bash
# Di root folder proyek
pip install -r requirements-test.txt
```

### Jalankan semua test

```bash
pytest tests/ -v
```

### Jalankan dengan output lengkap

```bash
pytest tests/ -v -s
```

### Jalankan per file

```bash
pytest tests/test_01_schema.py      -v   # T01-T03: Schema validation
pytest tests/test_02_dedup.py       -v   # T04-T07: Idempotency & dedup
pytest tests/test_03_api.py         -v   # T08-T10: API endpoints
pytest tests/test_04_concurrency.py -v   # T11-T13: Race condition
pytest tests/test_05_persistence.py -v   # T14-T15: Persistence
pytest tests/test_06_performance.py -v -s # T16-T18: Performance (print throughput)
```

### Jalankan test spesifik

```bash
pytest tests/test_04_concurrency.py::TestConcurrencyControl::test_T11_three_workers_same_event_no_double_process -v
```

---

## 📊 K6 Benchmark (Opsional)

```bash
# Install k6: https://k6.io/docs/get-started/installation/
# Pastikan docker compose sudah running

k6 run k6/load_test.js

# Custom parameter
k6 run --env BATCH_SIZE=100 --env DUP_RATE=0.40 k6/load_test.js
```

---

## 📁 Struktur Folder

```
uas-log-aggregator/
├── aggregator/
│   ├── app/
│   │   ├── main.py          ← FastAPI + endpoints + lifespan
│   │   ├── models.py        ← Pydantic schema (Event, BatchPublishRequest)
│   │   ├── database.py      ← SQLite WAL init + get_db()
│   │   ├── broker.py        ← Redis Streams client (XADD/XREAD/XACK)
│   │   ├── dedup.py         ← process_event_idempotent() — INTI SISTEM
│   │   ├── outbox.py        ← Outbox pattern processor
│   │   ├── consumer.py      ← 3 asyncio worker tasks
│   │   └── logging_config.py ← Structured JSON logging
│   ├── Dockerfile
│   └── requirements.txt
├── publisher/
│   ├── publisher.py         ← Event generator + 35% duplikasi + retry backoff
│   ├── Dockerfile
│   └── requirements.txt
├── tests/                   ← 18 test cases (unit, tanpa Docker)
│   ├── conftest.py
│   ├── test_01_schema.py    ← T01-T03
│   ├── test_02_dedup.py     ← T04-T07
│   ├── test_03_api.py       ← T08-T10
│   ├── test_04_concurrency.py ← T11-T13
│   ├── test_05_persistence.py ← T14-T15
│   └── test_06_performance.py ← T16-T18
├── k6/
│   └── load_test.js         ← K6 load test
├── docker-compose.yml
├── requirements-test.txt
├── pytest.ini
└── README.md
```

---

## 🔌 Endpoints

| Method | Path | Deskripsi |
|--------|------|-----------|
| GET | `/health` | Liveness + readiness check |
| POST | `/publish` | Publish event (single/batch) |
| GET | `/events` | Daftar event unik yang diproses |
| GET | `/events?topic=X` | Filter event per topik |
| GET | `/stats` | Statistik sistem |
| GET | `/docs` | Swagger UI (auto-generated FastAPI) |

---

## 🏗️ Keputusan Desain Utama

| Keputusan | Pilihan | Alasan |
|-----------|---------|--------|
| Language | Python 3.11 | Ekosistem async matang, lebih ringan dari Rust untuk pemula |
| Framework | FastAPI | Native async, auto Swagger, Pydantic validation bawaan |
| Database | SQLite WAL | Zero overhead RAM (vs Postgres ~150MB), ACID penuh, UNIQUE constraint |
| Broker | Redis 7 Streams | Consumer groups (1 message → 1 worker), XACK at-least-once |
| Isolation | BEGIN IMMEDIATE | Acquire write lock segera, cegah race condition antar worker |
| Dedup | INSERT OR IGNORE | Atomik: insert sukses = baru; ignored = duplikat |
| Outbox | SQLite outbox table | Event tidak hilang jika Redis sementara down |
| Workers | 3 asyncio tasks | Demonstrasi concurrency control multi-worker |

---

## 📹 Video Demo

[Link YouTube: *isi setelah upload*]

---

## 📚 Referensi

Coulouris, G., Dollimore, J., Kindberg, T., & Blair, G. (2012).
*Distributed Systems: Concepts and Design* (5th ed.). Addison-Wesley.
