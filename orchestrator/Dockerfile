FROM python:3.11-slim
LABEL service="orchestrator"
RUN apt-get update && apt-get install -y --no-install-recommends curl nginx && rm -rf /var/lib/apt/lists/*
WORKDIR /app
RUN mkdir -p /data /etc/nginx/conf.d
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN useradd -r -s /bin/false orchestrator && chown -R orchestrator:orchestrator /app /data
USER orchestrator
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:8000/healthz || exit 1
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "info"]
