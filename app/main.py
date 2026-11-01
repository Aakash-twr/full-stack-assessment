"""
app/main.py
───────────
FastAPI application factory.

Responsibilities
────────────────
• Configure structured logging once, at startup.
• Register all API routers.
• Expose the ASGI application object (`app`) consumed by uvicorn.

Startup/shutdown lifecycle
──────────────────────────
The @asynccontextmanager lifespan logs startup/shutdown events and can be
extended later (e.g. to warm the connection pool or register signal handlers).
No database schema creation happens here — Alembic migrations are run as a
separate step before the container starts (see docker-compose.yml).
"""

from __future__ import annotations

import logging
import logging.config
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import get_settings
from app.routes import health, transactions, webhooks

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def _configure_logging(level: str) -> None:
    """
    Configure the root logger with a structured format.

    Using dictConfig keeps logging configuration centralised and reproducible.
    The format includes timestamp, level, logger name and message — enough
    context to trace issues in both local and cloud environments.
    """
    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "standard": {
                    "format": (
                        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
                    ),
                    "datefmt": "%Y-%m-%dT%H:%M:%S",
                },
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "stream": "ext://sys.stdout",
                    "formatter": "standard",
                },
            },
            "root": {
                "level": level,
                "handlers": ["console"],
            },
            # Quieten noisy third-party loggers.
            "loggers": {
                "uvicorn": {"level": "INFO", "propagate": True},
                "uvicorn.access": {"level": "WARNING", "propagate": True},
                "sqlalchemy.engine": {
                    "level": "WARNING" if level != "DEBUG" else "DEBUG",
                    "propagate": True,
                },
            },
        }
    )


# ---------------------------------------------------------------------------
# Application lifespan
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Handle application startup and shutdown events."""
    settings = get_settings()
    _configure_logging(settings.log_level)

    logger.info(
        "Transaction Webhook Service starting up | "
        "log_level=%s processing_delay=%ds",
        settings.log_level,
        settings.processing_delay_seconds,
    )
    yield  # Application is running — handle requests.
    logger.info("Transaction Webhook Service shutting down.")


# ---------------------------------------------------------------------------
# Application factory
# ---------------------------------------------------------------------------

def create_app() -> FastAPI:
    """
    Build and return the FastAPI application.

    Separating construction from the module-level `app` object makes the
    factory easy to call from tests with different settings.
    """
    settings = get_settings()

    application = FastAPI(
        title="Transaction Webhook Processing Service",
        description=(
            "Receives payment-provider webhooks, persists transactions to MySQL, "
            "and exposes a status API. "
            "Background workers process transactions asynchronously."
        ),
        version="1.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan,
    )

    # ── CORS (permissive for assessment; tighten in production) ────────────────
    application.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ── Routers ────────────────────────────────────────────────────────────────
    application.include_router(health.router)          # GET /
    application.include_router(webhooks.router)        # POST /v1/webhooks/transactions
    application.include_router(transactions.router)    # GET /v1/transactions/{id}

    return application


# Module-level app consumed by uvicorn:
#   uvicorn app.main:app --host 0.0.0.0 --port 8000
app = create_app()
