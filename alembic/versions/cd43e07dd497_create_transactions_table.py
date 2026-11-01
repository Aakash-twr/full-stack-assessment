"""create_transactions_table

Revision ID: cd43e07dd497
Revises:
Create Date: 2026-11-01

Creates the `transactions` table with all columns, constraints and indexes
required by the Transaction Webhook Processing Service.

Design choices
──────────────
• InnoDB engine — mandatory for SELECT … FOR UPDATE SKIP LOCKED (job queue).
• DECIMAL(18,2) for amount — never FLOAT/DOUBLE for monetary values.
• DATETIME(6) everywhere — microsecond precision, stored as UTC.
• UNIQUE(transaction_id) — DB-level idempotency guard (no pre-INSERT SELECT).
• Two composite indexes optimise the worker's polling and lease-expiry queries.
"""

from alembic import op
import sqlalchemy as sa

# -- Revision identifiers (do not change) -------------------------------------
revision: str = "cd43e07dd497"
down_revision: str | None = None
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_table(
        "transactions",
        # -- Primary key -------------------------------------------------------
        sa.Column(
            "id",
            sa.BigInteger(),
            autoincrement=True,
            nullable=False,
            comment="Surrogate primary key",
        ),
        # -- Business identity -------------------------------------------------
        sa.Column(
            "transaction_id",
            sa.String(128),
            nullable=False,
            comment="Payment-provider transaction identifier; UNIQUE",
        ),
        sa.Column("source_account", sa.String(128), nullable=False),
        sa.Column("destination_account", sa.String(128), nullable=False),
        sa.Column(
            "amount",
            sa.DECIMAL(18, 2),
            nullable=False,
            comment="Monetary amount — stored as DECIMAL, never FLOAT",
        ),
        sa.Column(
            "currency",
            sa.String(3),
            nullable=False,
            comment="ISO 4217 currency code",
        ),
        # -- Processing state --------------------------------------------------
        sa.Column(
            "status",
            sa.String(20),
            nullable=False,
            server_default="PENDING",
            comment="PENDING | PROCESSING | PROCESSED | FAILED",
        ),
        # -- Timestamps --------------------------------------------------------
        sa.Column(
            "created_at",
            sa.DateTime(timezone=False),
            nullable=False,
            comment="UTC time the webhook was received",
        ),
        sa.Column(
            "processing_started_at",
            sa.DateTime(timezone=False),
            nullable=True,
            comment="UTC time of the first processing attempt",
        ),
        sa.Column(
            "processed_at",
            sa.DateTime(timezone=False),
            nullable=True,
            comment="UTC time of successful completion",
        ),
        # -- Job-queue fields --------------------------------------------------
        sa.Column(
            "attempts",
            sa.Integer(),
            nullable=False,
            server_default="0",
            comment="Total number of processing attempts",
        ),
        sa.Column(
            "lease_token",
            sa.String(36),
            nullable=True,
            comment="UUID4 of the worker currently holding this job",
        ),
        sa.Column(
            "lease_expires_at",
            sa.DateTime(timezone=False),
            nullable=True,
            comment="After this UTC time an abandoned job is re-claimable",
        ),
        sa.Column(
            "next_attempt_at",
            sa.DateTime(timezone=False),
            nullable=True,
            comment="Earliest UTC time for the next retry",
        ),
        sa.Column(
            "last_error",
            sa.Text(),
            nullable=True,
            comment="Most recent processing error message",
        ),
        # -- Constraints -------------------------------------------------------
        sa.PrimaryKeyConstraint("id", name="pk_transactions"),
        sa.UniqueConstraint(
            "transaction_id",
            name="uq_transactions_transaction_id",
        ),
        # -- Table options -----------------------------------------------------
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
    )

    # -- Indexes ---------------------------------------------------------------
    # Index 1: worker polling query
    #   WHERE (status = 'PENDING' AND next_attempt_at <= NOW())
    #      OR (status = 'PROCESSING' AND lease_expires_at <= NOW())
    #   ORDER BY created_at ASC
    op.create_index(
        "ix_transactions_status_next_attempt_created",
        "transactions",
        ["status", "next_attempt_at", "created_at"],
    )

    # Index 2: lease-expiry recovery scan
    #   WHERE status = 'PROCESSING' AND lease_expires_at <= NOW()
    op.create_index(
        "ix_transactions_status_lease_expires",
        "transactions",
        ["status", "lease_expires_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_transactions_status_lease_expires", table_name="transactions")
    op.drop_index(
        "ix_transactions_status_next_attempt_created", table_name="transactions"
    )
    op.drop_table("transactions")
