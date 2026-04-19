"""Fake authentication middleware.

Accepts any Bearer token (or no token). Injects the configured project ID
so route handlers don't need to parse it from the Authorization header.
"""

from fastapi import Request

from cloudbox.config import settings


async def get_project(request: Request) -> str:
    """Return the project ID for this request.

    Priority:
    1. Path parameter ``project`` (set by individual routers)
    2. CLOUDBOX_PROJECT env var / settings default
    """
    return request.path_params.get("project", settings.default_project)
