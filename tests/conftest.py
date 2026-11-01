"""
tests/conftest.py
──────────────────
Shared pytest fixtures for the Transaction Webhook Processing Service.

Database strategy
─────────────────
Integration tests run against a **real MySQL** instance so that:
• UNIQUE constraints are exercised (idempotency tests).
• SELECT … FOR UPDATE SKIP LOCKED is exercised (worker tests).
• Actual INT/DECIMAL/DATETIME storage behaviour is tested.

Set the TEST_DATABASE_URL environment variable to point at your test schema:
    export TEST_DATABASE_URL="mysql+pymysql://txn_user:txn_password@localhost:3306/transactions_test"

If not set, the default value below is used (matches Docker Compose test service).

Schema lifecycle
────────────────
• Session-scoped  `db_engine`  fixture  – creates the schema once via Alembic.
• Function-scoped `db_session` fixture  – truncates `transactions` before each test.
• Function-scoped `client`     fixture  – FastAPI TestClient with `get_db` and
  `get_settings` overrides injected.

Settings overrides for tests
────────────────────────────
• PROCESSING_DELAY_SECONDS = 2  (override from 30 → fast tests)
• WORKER_POLL_INTERVAL_SECONDS = 0.1
• JOB_LEASE_SECONDS = 10        (short leases so recovery tests run quickly)
• MAX_ATTEMPTS = 3
"""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

# ---------------------------------------------------------------------------
# Test database URL
# ---------------------------------------------------------------------------

TEST_DATABASE_URL: str = os.getenv(
    "TEST_DATABASE_URL",
    "mysql+pymysql://txn_user:txn_password@localhost:3306/transactions_test",
)

# ---------------------------------------------------------------------------
# Test settings (override production values)
# ---------------------------------------------------------------------------

TEST_SETTINGS_OVERRIDES: dict = {
    "database_url": TEST_DATABASE_URL,
    "processing_delay_seconds": 2,
    "worker_poll_interval_seconds": 0.1,
    "job_lease_seconds": 10,
    "max_attempts": 3,
    "retry_base_delay_seconds": 1,
    "log_level": "WARNING",  # suppress noise in test output
}


def make_test_settings():
    """Return a Settings instance configured for testing."""
    from app.core.config import Settings
    return Settings(**TEST_SETTINGS_OVERRIDES)


# ---------------------------------------------------------------------------
# Session-scoped engine + schema setup (runs once per pytest session)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def db_engine():
    """
    Create the test database schema using Alembic and return the engine.
    Drops and recreates every table so tests always start with a clean schema.
    """
    import alembic.config
    import alembic.command
    from app.core.database import Base

    engine = create_engine(
        TEST_DATABASE_URL,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
    )

    # Run migrations via Alembic (exercises the real migration path).
    alembic_cfg = alembic.config.Config("alembic.ini")
    alembic_cfg.set_main_option("sqlalchemy.url", TEST_DATABASE_URL)

    with engine.begin() as conn:
        # Drop all tables cleanly before recreating.
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
        conn.execute(text("DROP TABLE IF EXISTS `transactions`"))
        conn.execute(text("DROP TABLE IF EXISTS `alembic_version`"))
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 1"))

    alembic.command.upgrade(alembic_cfg, "head")

    yield engine

    engine.dispose()


# ---------------------------------------------------------------------------
# Function-scoped session (truncates between tests for isolation)
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def clean_db(db_engine):
    """Ensure every test starts and ends with an empty transactions table."""
    with db_engine.begin() as conn:
        conn.execute(text("DELETE FROM transactions"))
    yield
    with db_engine.begin() as conn:
        conn.execute(text("DELETE FROM transactions"))


@pytest.fixture()
def db_session(db_engine) -> Session:
    """
    Yield a SQLAlchemy Session connected to the test database.
    Truncates the `transactions` table after every test so tests are isolated.
    """
    TestSession = sessionmaker(
        bind=db_engine,
        autocommit=False,
        autoflush=False,
        expire_on_commit=False,
    )
    session = TestSession()
    yield session
    session.close()

    # Clean up after the test.
    with db_engine.begin() as conn:
        conn.execute(text("DELETE FROM transactions"))


# ---------------------------------------------------------------------------
# FastAPI TestClient with dependency overrides
# ---------------------------------------------------------------------------


@pytest.fixture()
def client(db_engine) -> TestClient:
    """
    Return a FastAPI TestClient with:
    • get_db       → session on the test database
    • get_settings → test settings (2-second delay, short leases, etc.)
    """
    from app.core.config import get_settings
    from app.core.database import get_db
    from app.main import create_app

    TestSession = sessionmaker(
        bind=db_engine,
        autocommit=False,
        autoflush=False,
        expire_on_commit=False,
    )

    def override_get_db():
        db = TestSession()
        try:
            yield db
        finally:
            db.close()

    test_settings = make_test_settings()

    application = create_app()
    application.dependency_overrides[get_db] = override_get_db

    # Patch get_settings globally so health check and routes pick up test values.
    with patch("app.core.config.get_settings", return_value=test_settings), \
         patch("app.routes.health._db_module.check_database_connection", return_value=True):
        yield TestClient(application, raise_server_exceptions=True)

    application.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Helper: insert a transaction directly (bypasses API)
# ---------------------------------------------------------------------------


@pytest.fixture()
def insert_transaction(db_session: Session):
    """
    Factory fixture — call it to insert a Transaction directly into the DB.
    Returns the saved Transaction ORM object.
    """
    from decimal import Decimal
    from datetime import datetime, timezone

    from app.models.transaction import Transaction, TransactionStatus

    def _insert(
        transaction_id: str = "txn_fixture_001",
        status: str = TransactionStatus.PENDING,
        attempts: int = 0,
        lease_token: str | None = None,
        lease_expires_at: datetime | None = None,
        next_attempt_at: datetime | None = None,
        processing_started_at: datetime | None = None,
        processed_at: datetime | None = None,
        last_error: str | None = None,
    ) -> Transaction:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        txn = Transaction(
            transaction_id=transaction_id,
            source_account="acc_src",
            destination_account="acc_dst",
            amount=Decimal("1500.00"),
            currency="INR",
            status=status,
            created_at=now,
            processing_started_at=processing_started_at,
            processed_at=processed_at,
            attempts=attempts,
            lease_token=lease_token,
            lease_expires_at=lease_expires_at,
            next_attempt_at=next_attempt_at,
            last_error=last_error,
        )
        db_session.add(txn)
        db_session.commit()
        db_session.refresh(txn)
        return txn

    return _insert
