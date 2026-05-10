FROM python:3.12-slim@sha256:46cb7cc2877e60fbd5e21a9ae6115c30ace7a077b9f8772da879e4590c18c2e3

WORKDIR /app

ARG DEPENDENCY_COOLDOWN_DAYS=7
ARG PIP_VERSION=26.0.1
ARG INSTALL_DISTILBERT=false

# Install system CA certificates
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    update-ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Install dependencies first (layer caching)
COPY requirements.lock .
COPY requirements-distilbert.lock .
RUN python -m pip install --no-cache-dir --upgrade "pip==${PIP_VERSION}" && \
    cutoff="$(DEPENDENCY_COOLDOWN_DAYS="${DEPENDENCY_COOLDOWN_DAYS}" python -c "from datetime import datetime, timezone, timedelta; import os; days = int(os.environ['DEPENDENCY_COOLDOWN_DAYS']); print((datetime.now(timezone.utc) - timedelta(days=days)).replace(microsecond=0).isoformat().replace('+00:00', 'Z'))")" && \
    python -m pip install --no-cache-dir --require-hashes --uploaded-prior-to "$cutoff" -r requirements.lock && \
    if [ "$INSTALL_DISTILBERT" = "true" ] || [ "$INSTALL_DISTILBERT" = "1" ] || [ "$INSTALL_DISTILBERT" = "yes" ]; then \
        python -m pip install --no-cache-dir --require-hashes -r requirements-distilbert.lock; \
    fi

# Copy application code
COPY . .

# Remove any baked-in .env to avoid conflicts with docker-compose env_file
RUN rm -f .env .env_bkp

# Create data directory (will be overridden by volume mount)
RUN mkdir -p /data

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/healthz/health')" || exit 1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
