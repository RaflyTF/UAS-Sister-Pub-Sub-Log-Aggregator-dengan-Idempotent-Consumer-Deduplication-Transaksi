/**
 * K6 Load Test — Pub-Sub Log Aggregator
 * ──────────────────────────────────────
 * Mengirim 20.000+ event dengan 35% duplikasi.
 * Mengukur throughput, latency p95, dan error rate.
 *
 * Cara menjalankan (setelah docker compose up):
 *   k6 run k6/load_test.js
 *   k6 run --vus 5 --duration 30s k6/load_test.js
 *   k6 run --env BASE_URL=http://localhost:8080 k6/load_test.js
 *
 * Install k6: https://k6.io/docs/get-started/installation/
 */

import http from "k6/http";
import { check, sleep } from "k6";
import { Counter, Rate, Trend } from "k6/metrics";
import { uuidv4 } from "https://jslib.k6.io/k6-utils/1.4.0/index.js";

// ── Konfigurasi ──────────────────────────────────────────────────────────
const BASE_URL   = __ENV.BASE_URL   || "http://localhost:8080";
const BATCH_SIZE = parseInt(__ENV.BATCH_SIZE || "50");
const DUP_RATE   = parseFloat(__ENV.DUP_RATE  || "0.35");

export const options = {
  scenarios: {
    bulk_publish: {
      executor:        "constant-arrival-rate",
      rate:            10,           // 10 batch/detik = 500 event/detik
      timeUnit:        "1s",
      duration:        "60s",
      preAllocatedVUs: 5,
      maxVUs:          20,
    },
  },
  thresholds: {
    http_req_duration: ["p(95)<3000"],   // 95% request < 3 detik
    http_req_failed:   ["rate<0.05"],    // Error rate < 5%
    checks:            ["rate>0.90"],    // Check success rate > 90%
  },
};

// ── Custom metrics ────────────────────────────────────────────────────────
const publishSuccess = new Rate("publish_success_rate");
const publishLatency = new Trend("publish_latency_ms", true);
const dupsSent       = new Counter("duplicates_sent_total");
const uniqueSent     = new Counter("unique_sent_total");

// ── State: ID yang sudah pernah dikirim (untuk generate duplikat) ─────────
const sentIds = [];
const TOPICS  = ["logs/app", "logs/error", "metrics/cpu", "metrics/mem", "events/order"];
const SOURCES = ["service-a", "service-b", "gateway"];
const LEVELS  = ["INFO", "WARN", "ERROR", "DEBUG"];

function generateEvent() {
  const isDup = sentIds.length > 0 && Math.random() < DUP_RATE;
  let eventId;

  if (isDup) {
    eventId = sentIds[Math.floor(Math.random() * Math.min(sentIds.length, 500))];
    dupsSent.add(1);
  } else {
    eventId = uuidv4();
    sentIds.push(eventId);
    if (sentIds.length > 5000) sentIds.shift();
    uniqueSent.add(1);
  }

  return {
    topic:     TOPICS[Math.floor(Math.random() * TOPICS.length)],
    event_id:  eventId,
    timestamp: new Date().toISOString(),
    source:    SOURCES[Math.floor(Math.random() * SOURCES.length)],
    payload: {
      message: `k6 event ${Date.now()}`,
      level:   LEVELS[Math.floor(Math.random() * LEVELS.length)],
      value:   Math.random() * 100,
    },
  };
}

// ── VU function (dijalankan tiap VU setiap siklus) ────────────────────────
export default function () {
  const events  = Array.from({ length: BATCH_SIZE }, generateEvent);
  const payload = JSON.stringify({ events });
  const params  = { headers: { "Content-Type": "application/json" }, timeout: "30s" };

  const t0  = Date.now();
  const res = http.post(`${BASE_URL}/publish`, payload, params);
  publishLatency.add(Date.now() - t0);

  const ok = check(res, {
    "status 201":          (r) => r.status === 201,
    "body has count":      (r) => { try { return JSON.parse(r.body).count > 0; } catch { return false; } },
    "latency < 3000ms":    ()  => (Date.now() - t0) < 3000,
  });
  publishSuccess.add(ok ? 1 : 0);

  sleep(0.05);
}

// ── Summary setelah test selesai ──────────────────────────────────────────
export function handleSummary(data) {
  const statsRes = http.get(`${BASE_URL}/stats`);
  let stats = {};
  try { stats = JSON.parse(statsRes.body); } catch (_) {}

  const lines = [
    "═══════════ HASIL K6 LOAD TEST ═══════════",
    `Requests:         ${data.metrics.http_reqs.values.count}`,
    `P95 Latency:      ${(data.metrics.http_req_duration?.values["p(95)"] || 0).toFixed(0)} ms`,
    `Error Rate:       ${((data.metrics.http_req_failed?.values.rate || 0) * 100).toFixed(2)}%`,
    "───────────────────────────────────────────",
    "AGGREGATOR STATS (via GET /stats):",
    `  received:         ${stats.received || "N/A"}`,
    `  unique_processed: ${stats.unique_processed || "N/A"}`,
    `  dup_dropped:      ${stats.duplicate_dropped || "N/A"}`,
    `  topics:           ${(stats.topics || []).join(", ")}`,
    `  uptime:           ${stats.uptime_human || "N/A"}`,
    "═══════════════════════════════════════════",
  ];
  console.log(lines.join("\n"));
  return { stdout: JSON.stringify(data, null, 2) };
}
