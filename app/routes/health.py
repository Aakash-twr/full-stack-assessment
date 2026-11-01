"""
app/routes/health.py
─────────────────────
GET /  — application and database health check.

Returns HTTP 200 with status=HEALTHY when the database is reachable.
Returns HTTP 503 with status=UNHEALTHY when the database is down.
The current UTC time is always included so callers can detect clock skew.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from fastapi import APIRouter
from fastapi.responses import JSONResponse

import app.core.database as _db_module
from app.schemas.transaction import HealthResponse

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Health"])


def _utc_iso_now() -> str:
    """Return the current UTC time as an ISO 8601 string with a 'Z' suffix."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


@router.get(
    "/",
    response_model=HealthResponse,
    summary="Health check",
    description=(
        "Returns HEALTHY when the API and database are both reachable. "
        "Returns UNHEALTHY (HTTP 503) if the database cannot be reached."
    ),
)
def health_check() -> JSONResponse:
    """
    Lightweight liveness + readiness probe.

    Checks database connectivity via a single 'SELECT 1' query.
    Never raises an unhandled exception — always returns a structured response.
    """
    current_time = _utc_iso_now()
    db_ok = _db_module.check_database_connection()

    if db_ok:
        logger.debug("Health check: HEALTHY")
        return JSONResponse(
            status_code=200,
            content={"status": "HEALTHY", "current_time": current_time},
        )

    logger.warning("Health check: UNHEALTHY — database is not reachable")
    return JSONResponse(
        status_code=503,
        content={"status": "UNHEALTHY", "current_time": current_time},
    )
