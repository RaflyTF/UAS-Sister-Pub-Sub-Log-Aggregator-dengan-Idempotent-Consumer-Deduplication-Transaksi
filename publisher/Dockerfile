FROM python:3.11-slim

RUN groupadd -r appgroup && useradd -r -g appgroup appuser

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY publisher.py .

USER appuser

# Publisher berjalan sekali lalu selesai (bukan server yang terus berjalan)
CMD ["python", "publisher.py"]
