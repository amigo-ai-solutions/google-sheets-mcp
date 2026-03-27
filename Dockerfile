# Stage 1: Builder
FROM python:3.12-slim AS builder
WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends gcc && rm -rf /var/lib/apt/lists/*
COPY pyproject.toml README.md ./
COPY google_sheets_mcp/ ./google_sheets_mcp/
RUN pip install --no-cache-dir --prefix=/install .

# Stage 2: Runtime
FROM python:3.12-slim
RUN groupadd -r mcp && useradd -r -g mcp -d /app -s /sbin/nologin mcp
WORKDIR /app
COPY --from=builder /install /usr/local
COPY --chown=mcp:mcp google_sheets_mcp/ ./google_sheets_mcp/

USER mcp

ENV PYTHONUNBUFFERED=1

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen(f'http://localhost:{__import__(\"os\").environ.get(\"PORT\",8080)}/health')" || exit 1

CMD ["python", "-m", "google_sheets_mcp.app"]
