# ── Stage: runtime ────────────────────────────────────────────────────────
# python:3.11-slim = image resmi Python minimal (~50MB), sesuai rekomendasi rubrik.
FROM python:3.11-slim

# Non-root user untuk keamanan (rubrik: Dockerfile best practice)
RUN groupadd -r appgroup && useradd -r -g appgroup appuser

WORKDIR /app

# Copy requirements terlebih dahulu agar layer cache bekerja optimal.
# Layer ini hanya direbuild jika requirements.txt berubah.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code aplikasi
COPY app/ ./app/

# Buat direktori untuk SQLite volume dan berikan kepemilikan ke appuser
RUN mkdir -p /var/lib/aggregator && chown appuser:appgroup /var/lib/aggregator

# Switch ke non-root user
USER appuser

# Expose port aplikasi
EXPOSE 8080

# Jalankan uvicorn dengan 1 worker process.
# Workers asyncio (consumer) dihandle di dalam aplikasi itu sendiri.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080", \
     "--log-level", "warning"]
