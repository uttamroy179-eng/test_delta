# Final stage
FROM python:3.11-slim

# Create non-root user and install curl
RUN useradd -m trader && apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements and install as trader (using --user)
COPY requirements.txt .
USER trader
RUN pip install --user --no-cache-dir -r requirements.txt

# Ensure trader's local bin is in PATH
ENV PATH="/home/trader/.local/bin:${PATH}"

# Copy application code
COPY --chown=trader:trader . .

# Health check (curl is available system-wide)
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1

# Run with gunicorn + uvicorn worker
CMD ["gunicorn", "main:app", "--worker-class", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:8000"]