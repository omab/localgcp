FROM python:3.14-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy dependency manifests first for layer caching
COPY pyproject.toml uv.lock ./

# Install runtime dependencies without the project itself
RUN uv sync --no-dev --no-install-project --frozen

# Copy application source and install the project
COPY LICENSE README.md ./
COPY localgcp/ localgcp/
RUN uv sync --no-dev --frozen

# Expose all service ports
EXPOSE 4443 8080 8085 8086 8090 8123 9050 9010 9020 8091 8888

# Health check via admin UI
HEALTHCHECK --interval=10s --timeout=5s --start-period=5s \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8888/health')" || exit 1

CMD ["uv", "run", "localgcp"]
