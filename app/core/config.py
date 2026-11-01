"""
app/core/config.py
──────────────────
Centralised application settings loaded from environment variables.
All configuration lives here; nothing is hardcoded elsewhere.
"""

from functools import lru_cache
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings sourced from environment variables (or a .env file
    when running locally).  Pydantic-settings validates and coerces every value
    at startup, so configuration errors surface immediately rather than at
    runtime.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        # Extra fields in .env are silently ignored — forward-compatible.
        extra="ignore",
    )

    # ── Database ──────────────────────────────────────────────────────────────
    database_url: str = (
        "mysql+pymysql://txn_user:txn_password@localhost:3306/transactions_db"
    )

    # SQLAlchemy connection-pool sizing
    db_pool_size: int = 10
    db_max_overflow: int = 20
    # How long (seconds) to wait for a connection from the pool before raising
    db_pool_timeout: int = 30
    # Recycle connections older than this many seconds (avoids stale connections)
    db_pool_recycle: int = 1800

    # ── Worker tuning ─────────────────────────────────────────────────────────
    # Seconds to simulate the external payment-processor call (override to 2 in tests)
    processing_delay_seconds: int = 30

    # How often the worker polls for new pending jobs
    worker_poll_interval_seconds: float = 1.0

    # Seconds a worker may hold a lease before it is considered abandoned
    job_lease_seconds: int = 120

    # Maximum processing attempts before a transaction is permanently failed
    max_attempts: int = 5

    # Base delay (seconds) for exponential back-off: delay = base * 2^(attempt-1)
    retry_base_delay_seconds: int = 5

    # ── API server ────────────────────────────────────────────────────────────
    port: int = 8000

    # ── Logging ───────────────────────────────────────────────────────────────
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Return a cached Settings singleton.

    Using lru_cache ensures the .env file is read only once per process, which
    also makes unit tests easier: replace the cache with a fresh Settings
    instance that has test values.
    """
    return Settings()
