"""
app/models/transaction.py
─────────────────────────
SQLAlchemy ORM model for the `transactions` table.

Design notes
────────────
• InnoDB is enforced via mysql_engine="InnoDB" on the table args.
• DECIMAL(18,2) is used for monetary amounts — never FLOAT/DOUBLE.
• All DATETIME columns use precision 6 (microseconds) and are stored in UTC.
• status is VARCHAR(20) rather than a MySQL ENUM so that adding new states
  does not require a schema migration.
• A TransactionStatus helper class provides the canonical status strings used
  throughout the codebase.
"""

from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    DECIMAL,
    BigInteger,
    DateTime,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class TransactionStatus:
    """Canonical status values — single source of truth for the whole app."""

    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    PROCESSED = "PROCESSED"
    FAILED = "FAILED"


class Transaction(Base):
    """
    Persists every inbound webhook and doubles as the background job queue.

    Columns
    -------
    id                    - surrogate PK, auto-increment
    transaction_id        - payment-provider identifier; UNIQUE
    source_account        - sender account identifier
    destination_account   - receiver account identifier
    amount                - monetary value; DECIMAL(18,2)
    currency              - ISO 4217 code; CHAR(3)
    status                - one of PENDING / PROCESSING / PROCESSED / FAILED
    created_at            - UTC timestamp when the webhook was received
    processing_started_at - UTC timestamp of the first processing attempt
    processed_at          - UTC timestamp of successful completion (nullable)
    attempts              - total number of processing attempts so far
    lease_token           - UUID4 string identifying the current worker lease
    lease_expires_at      - after this UTC time an abandoned job is re-claimable
    next_attempt_at       - earliest UTC time for the next retry
    last_error            - most recent error message (nullable)
    """

    __tablename__ = "transactions"

    # -- Primary key -----------------------------------------------------------
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # -- Business identity -----------------------------------------------------
    transaction_id: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
        comment="Payment-provider transaction identifier; enforced UNIQUE at DB level",
    )
    source_account: Mapped[str] = mapped_column(String(128), nullable=False)
    destination_account: Mapped[str] = mapped_column(String(128), nullable=False)
    amount: Mapped[Decimal] = mapped_column(DECIMAL(18, 2), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)

    # -- Processing state ------------------------------------------------------
    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default=TransactionStatus.PENDING,
        index=False,  # covered by composite indexes below
        comment="PENDING | PROCESSING | PROCESSED | FAILED",
    )

    # -- Timestamps (all UTC, microsecond precision) ---------------------------
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False),  # stored as UTC; no TZ offset in MySQL DATETIME
        nullable=False,
        comment="UTC time when the webhook was received",
    )
    processing_started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=False),
        nullable=True,
        default=None,
        comment="UTC time of the first processing attempt",
    )
    processed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=False),
        nullable=True,
        default=None,
        comment="UTC time of successful completion",
    )

    # -- Job-queue fields ------------------------------------------------------
    attempts: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        comment="Total processing attempts so far",
    )
    lease_token: Mapped[str | None] = mapped_column(
        String(36),
        nullable=True,
        default=None,
        comment="UUID4 identifying the worker currently holding this job",
    )
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=False),
        nullable=True,
        default=None,
        comment="After this UTC time the job can be claimed by another worker",
    )
    next_attempt_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=False),
        nullable=True,
        default=None,
        comment="Earliest UTC time for the next retry",
    )
    last_error: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        default=None,
        comment="Most recent processing error message",
    )

    # -- Table-level constraints and indexes -----------------------------------
    __table_args__ = (
        # Enforce payment-provider uniqueness at the database level.
        # Catching IntegrityError(errno=1062) on INSERT is the correct
        # idempotency strategy - no SELECT needed before INSERT.
        UniqueConstraint("transaction_id", name="uq_transactions_transaction_id"),
        # Composite index for the worker's polling query:
        #   WHERE status IN ('PENDING','PROCESSING') AND next_attempt_at <= NOW()
        #   ORDER BY created_at ASC
        Index(
            "ix_transactions_status_next_attempt_created",
            "status",
            "next_attempt_at",
            "created_at",
        ),
        # Composite index for lease-expiry recovery:
        #   WHERE status = 'PROCESSING' AND lease_expires_at <= NOW()
        Index(
            "ix_transactions_status_lease_expires",
            "status",
            "lease_expires_at",
        ),
        # Use InnoDB to enable SELECT ... FOR UPDATE SKIP LOCKED.
        {"mysql_engine": "InnoDB", "mysql_charset": "utf8mb4"},
    )

    def __repr__(self) -> str:
        return (
            f"<Transaction id={self.id} "
            f"transaction_id={self.transaction_id!r} "
            f"status={self.status!r}>"
        )
