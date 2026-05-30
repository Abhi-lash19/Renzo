# Stage 1: builder — install dependencies
FROM python:3.11-slim AS builder

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Stage 2: runtime — lean image
FROM python:3.11-slim

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy application code
COPY app/ ./app/
COPY config/ ./config/
COPY fetchers/ ./fetchers/
COPY intelligence/ ./intelligence/
COPY pipeline/ ./pipeline/
COPY storage/ ./storage/
COPY utils/ ./utils/
COPY core/ ./core/
COPY main.py .
COPY requirements.txt .

# Create data directory for SQLite persistence
RUN mkdir -p /app/data /app/logs /app/output

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
