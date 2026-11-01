# syntax=docker/dockerfile:1
FROM python:3.12-slim

# Prevent Python from writing .pyc files and enable unbuffered logging
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

# Install curl for container health checks
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy application code, migrations, and config
COPY alembic.ini .
COPY alembic/ ./alembic/
COPY app/ ./app/

# Run as non-root user for security
RUN addgroup --system appgroup && adduser --system --group appuser \
    && chown -R appuser:appgroup /app

USER appuser

EXPOSE 8000

# Default command starts the FastAPI server; overridden in docker-compose.yml for the worker
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
