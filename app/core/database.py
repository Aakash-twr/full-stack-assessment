"""
app/core/database.py
────────────────────
SQLAlchemy 2.x engine and session factory.

Design notes
────────────
• We use the *synchronous* PyMySQL driver as specified.  FastAPI's async routes
  work fine with synchronous DB calls for I/O that completes quickly (< 100 ms).
  The 30-second delay lives entirely inside the worker process, never in a route.

• pool_pre_ping=True asks SQLAlchemy to issue a lightweight "SELECT 1" before
  handing a connection to application code.  This transparently handles stale
  connections dropped by MySQL's wait_timeout without surfacing errors to callers.

• Session objects are *not* thread-safe, so each request / worker iteration
  gets its own session via get_db().

• The engine is a module-level singleton; it is created once and shared across
  the entire process lifetime.
"""

from collections.abc import Generator

from sqlalchemy import create_engine, text
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from app.core.config import get_settings


def _build_engine():
    """Build and return the SQLAlchemy engine from current settings."""
    settings = get_settings()
    return create_engine(
        settings.database_url,
        # ── Connection-pool settings ──────────────────────────────────────────
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_timeout=settings.db_pool_timeout,
        pool_recycle=settings.db_pool_recycle,
        # Ping the DB before handing out a connection; handles dropped sockets.
        pool_pre_ping=True,
        # Echo SQL statements in DEBUG mode for easier local troubleshooting.
        echo=(settings.log_level == "DEBUG"),
    )


# Module-level engine singleton — shared across the process.
engine = _build_engine()

# Session factory — call SessionLocal() to obtain a new session.
SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,  # keeps attribute access working after commit
)


class Base(DeclarativeBase):
    """
    Shared declarative base for all ORM models.
    Import this class in every model file and inherit from it.
    """


def get_db() -> Generator[Session, None, None]:
    """
    FastAPI dependency that yields a database session.

    Usage in a route:
        @router.get("/example")
        def example(db: Session = Depends(get_db)):
            ...

    The finally block guarantees the session is closed even if an exception is
    raised inside the route handler, returning the underlying connection to the
    pool promptly.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def check_database_connection() -> bool:
    """
    Perform a lightweight connectivity probe.

    Returns True if the database is reachable, False otherwise.
    Used by the health-check endpoint.
    """
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False
