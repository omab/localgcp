"""Request/response logging middleware for Cloudbox services."""

from __future__ import annotations

import logging
import time

from fastapi import FastAPI, Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response


def add_request_logging(app: FastAPI, service_name: str) -> None:
    """Attach request logging middleware and an unhandled-exception handler.

    Args:
        app (FastAPI): The FastAPI application to attach the middleware to.
        service_name (str): Short service identifier used as the logger name suffix
            (e.g. "gcs" produces logger "cloudbox.gcs").
    """
    logger = logging.getLogger(f"cloudbox.{service_name}")

    class _Middleware(BaseHTTPMiddleware):
        async def dispatch(self, request: Request, call_next) -> Response:
            start = time.monotonic()
            qs = f"?{request.url.query}" if request.url.query else ""
            logger.debug("→ %s %s%s", request.method, request.url.path, qs)

            try:
                response = await call_next(request)
            except Exception as exc:
                elapsed_ms = (time.monotonic() - start) * 1000
                logger.error(
                    "✗ %s %s%s — unhandled exception (%.0f ms): %s",
                    request.method,
                    request.url.path,
                    qs,
                    elapsed_ms,
                    exc,
                    exc_info=True,
                )
                raise

            elapsed_ms = (time.monotonic() - start) * 1000
            level = logging.WARNING if response.status_code >= 400 else logging.INFO
            logger.log(
                level,
                "← %s %s%s  %d  (%.0f ms)",
                request.method,
                request.url.path,
                qs,
                response.status_code,
                elapsed_ms,
            )
            return response

    app.add_middleware(_Middleware)
