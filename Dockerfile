# Stage 1: Builder
FROM python:3.12-slim AS builder
WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends gcc && rm -rf /var/lib/apt/lists/*
COPY pyproject.toml .
COPY google_sheets_mcp/ ./google_sheets_mcp/
RUN pip install --no-cache-dir --prefix=/install .

# Stage 2: Runtime
FROM python:3.12-slim
RUN groupadd -r mcp && useradd -r -g mcp -d /app -s /sbin/nologin mcp
WORKDIR /app
COPY --from=builder /install /usr/local
COPY google_sheets_mcp/ ./google_sheets_mcp/

USER mcp

ENV PORT=8000
ENV PYTHONUNBUFFERED=1

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" || exit 1

CMD ["python", "-m", "google_sheets_mcp.app"]
