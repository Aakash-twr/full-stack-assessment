"""
alembic/env.py
──────────────
Alembic migration environment.

Key behaviours
──────────────
1. DATABASE_URL is read from the application settings (which load from env /
   .env file) — never from alembic.ini, so credentials stay out of VCS.
2. target_metadata points at our ORM Base, enabling autogenerate support.
3. compare_type=True makes Alembic detect column type changes in autogenerate.
4. NullPool is used for the migration connection so the script exits cleanly.
"""

import os
import sys
from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool

from alembic import context

# ---------------------------------------------------------------------------
# Make sure the project root is on sys.path so that `app.*` imports work even
# when Alembic is invoked from within the alembic/ sub-directory.
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# ---------------------------------------------------------------------------
# Alembic config object — gives access to alembic.ini values.
# ---------------------------------------------------------------------------
config = context.config

# Set up loggers from alembic.ini (if a config file is present).
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# ---------------------------------------------------------------------------
# Inject DATABASE_URL from application settings.
# This overrides the (empty) sqlalchemy.url in alembic.ini so that the real
# connection string is read from the DATABASE_URL environment variable.
# ---------------------------------------------------------------------------
if not config.get_main_option("sqlalchemy.url"):
    from app.core.config import get_settings  # noqa: E402
    _settings = get_settings()
    config.set_main_option("sqlalchemy.url", _settings.database_url)

# ---------------------------------------------------------------------------
# Import all ORM models so that Base.metadata knows about every table.
# Alembic autogenerate compares Base.metadata against the live schema.
# ---------------------------------------------------------------------------
from app.core.database import Base  # noqa: E402
import app.models.transaction  # noqa: E402, F401  -- registers Transaction model

target_metadata = Base.metadata

# ---------------------------------------------------------------------------
# Migration runner — offline mode (generates SQL without a live DB connection)
# ---------------------------------------------------------------------------


def run_migrations_offline() -> None:
    """
    Emit migration SQL to stdout without connecting to the database.
    Useful for reviewing changes or applying them manually.
    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )

    with context.begin_transaction():
        context.run_migrations()


# ---------------------------------------------------------------------------
# Migration runner — online mode (connects to the live database)
# ---------------------------------------------------------------------------


def run_migrations_online() -> None:
    """
    Apply migrations against a live database connection.
    NullPool prevents connection-pool threads from keeping the process alive
    after the migration command exits.
    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
        )

        with context.begin_transaction():
            context.run_migrations()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
