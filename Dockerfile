FROM python:3.14-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy dependency files first for layer caching
COPY pyproject.toml .

# Install runtime dependencies only (no dev group)
RUN uv sync --no-dev

# Copy application source
COPY localgcp/ localgcp/

# Expose all service ports
EXPOSE 4443 8080 8085 8090 8123 8888

# Health check via admin UI
HEALTHCHECK --interval=10s --timeout=5s --start-period=5s \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8888/health')" || exit 1

CMD ["uv", "run", "python", "-m", "localgcp.main"]
