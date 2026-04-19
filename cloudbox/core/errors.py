"""GCP-style error responses.

GCP APIs return errors in the format:
    {"error": {"code": <http_status>, "message": "<text>", "status": "<STATUS_CODE>"}}
"""

import logging

from fastapi import HTTPException
from fastapi.responses import JSONResponse

logger = logging.getLogger("cloudbox.errors")

# Map of HTTP status code → GCP status string
_STATUS_MAP = {
    400: "INVALID_ARGUMENT",
    401: "UNAUTHENTICATED",
    403: "PERMISSION_DENIED",
    404: "NOT_FOUND",
    409: "ALREADY_EXISTS",
    412: "FAILED_PRECONDITION",
    429: "RESOURCE_EXHAUSTED",
    500: "INTERNAL",
    501: "UNIMPLEMENTED",
    503: "UNAVAILABLE",
}


def gcp_error(http_status: int, message: str, status: str | None = None) -> JSONResponse:
    """Return a GCP-format JSON error response."""
    gcp_status = status or _STATUS_MAP.get(http_status, "UNKNOWN")
    return JSONResponse(
        status_code=http_status,
        content={"error": {"code": http_status, "message": message, "status": gcp_status}},
    )


class GCPError(HTTPException):
    """Raise this from route handlers to return a GCP-format error."""

    def __init__(self, http_status: int, message: str, status: str | None = None):
        super().__init__(status_code=http_status, detail=message)
        self.gcp_status = status or _STATUS_MAP.get(http_status, "UNKNOWN")
        self.message = message


def add_gcp_exception_handler(app) -> None:
    """Register the GCPError and generic exception handlers on a FastAPI app."""
    from fastapi.responses import JSONResponse

    @app.exception_handler(GCPError)
    async def _gcp_handler(request, exc: GCPError):
        level = logging.WARNING if exc.status_code < 500 else logging.ERROR
        logger.log(
            level,
            "%s %s → %d %s: %s",
            request.method,
            request.url.path,
            exc.status_code,
            exc.gcp_status,
            exc.message,
        )
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": {
                    "code": exc.status_code,
                    "message": exc.message,
                    "status": exc.gcp_status,
                }
            },
        )

    @app.exception_handler(Exception)
    async def _generic_handler(request, exc: Exception):
        logger.error(
            "%s %s → 500 unhandled %s: %s",
            request.method,
            request.url.path,
            type(exc).__name__,
            exc,
            exc_info=True,
        )
        return JSONResponse(
            status_code=500,
            content={"error": {"code": 500, "message": str(exc), "status": "INTERNAL"}},
        )
