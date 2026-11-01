"""
app/services/transaction_service.py
────────────────────────────────────
Business-logic layer for transaction persistence.

Responsibilities
────────────────
• create_transaction()  – idempotent INSERT; returns (transaction, is_new)
• get_transaction()     – lookup by payment-provider transaction_id

Idempotency design
──────────────────
We attempt the INSERT directly without a prior SELECT.  If MySQL raises
IntegrityError with errno 1062 (Duplicate entry), we treat the request as an
idempotent duplicate and fetch the existing record.

Why this approach is correct
─────────────────────────────
A SELECT-then-INSERT pattern is racy: two concurrent requests can both pass
the SELECT check and then both attempt an INSERT, causing a silent data race
or an unhandled error.

By relying on the database's UNIQUE constraint we push the serialisation
point down to the storage engine (InnoDB row-level locking), which handles
concurrent requests safely by design.

Errno 1062 is specifically "Duplicate entry for key" — only that error is
swallowed.  Every other IntegrityError (e.g. NOT NULL violation, data
truncation) propagates to the caller so the API can return HTTP 503 rather
than silently swallowing a real persistence failure.

Thread-safety
─────────────
Each Session object is used by a single request/worker iteration (supplied
via FastAPI's Depends(get_db) or explicitly in the worker).  Services do not
cache sessions or share state between calls.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models.transaction import Transaction, TransactionStatus
from app.schemas.transaction import WebhookPayload

logger = logging.getLogger(__name__)

# MySQL error number for "Duplicate entry … for key …"
_MYSQL_DUPLICATE_ENTRY_ERRNO = 1062


def _utcnow() -> datetime:
    """
    Return the current time as a naive UTC datetime.

    All timestamps in the database are stored without timezone info but are
    always UTC.  Using a naive datetime avoids SQLAlchemy / PyMySQL timezone
    conversion surprises.
    """
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _is_duplicate_key_error(exc: IntegrityError) -> bool:
    """
    Detect MySQL duplicate-key errors from an SQLAlchemy IntegrityError.

    SQLAlchemy wraps the original DBAPI exception; we reach through to PyMySQL's
    args tuple where args[0] is the MySQL errno.
    """
    orig = getattr(exc, "orig", None)
    if orig is None:
        return False
    # pymysql.err.IntegrityError stores (errno, message) in args
    args = getattr(orig, "args", ())
    return bool(args and args[0] == _MYSQL_DUPLICATE_ENTRY_ERRNO)


# ---------------------------------------------------------------------------
# Public service functions
# ---------------------------------------------------------------------------


def create_transaction(
    db: Session,
    payload: WebhookPayload,
) -> tuple[Transaction, bool]:
    """
    Persist a new transaction from an inbound webhook payload.

    Returns
    ───────
    (transaction, is_new)
        transaction  – the persisted Transaction ORM object
        is_new       – True if just inserted, False if a duplicate

    Raises
    ──────
    IntegrityError
        Re-raised for any database integrity error that is NOT a duplicate-key
        error (errno 1062).  The caller (route handler) must catch this and
        return HTTP 503.
    Exception
        Any other unexpected database exception propagates unchanged.

    Algorithm
    ─────────
    1. Build a Transaction with status=PENDING and created_at=now(UTC).
    2. db.add() + db.flush() — sends INSERT to MySQL within the open transaction
       but does not commit yet; this is when the UNIQUE constraint is checked.
    3. If flush() raises IntegrityError with errno 1062:
           a. Roll back to a clean state.
           b. Fetch the existing record by transaction_id.
           c. Return (existing, False).
    4. If flush() raises any other IntegrityError → re-raise.
    5. db.commit() — make the INSERT durable.
    6. db.refresh() — reload auto-generated fields (id, server defaults).
    7. Return (new_transaction, True).
    """
    now = _utcnow()

    txn = Transaction(
        transaction_id=payload.transaction_id,
        source_account=payload.source_account,
        destination_account=payload.destination_account,
        amount=payload.amount,
        currency=payload.currency,
        status=TransactionStatus.PENDING,
        created_at=now,
        # Job-queue fields — all nullable, set later by the worker
        processing_started_at=None,
        processed_at=None,
        attempts=0,
        lease_token=None,
        lease_expires_at=None,
        next_attempt_at=None,
        last_error=None,
    )

    try:
        db.add(txn)
        db.flush()  # execute INSERT; raises IntegrityError on constraint violation
    except IntegrityError as exc:
        db.rollback()  # restore a clean session state before any further use

        if _is_duplicate_key_error(exc):
            # Idempotent duplicate — fetch and return the existing record.
            logger.info(
                "Duplicate webhook received for transaction_id=%r; "
                "returning existing record.",
                payload.transaction_id,
            )
            existing = get_transaction(db, payload.transaction_id)
            # existing should always be found at this point; guard defensively.
            if existing is None:
                raise RuntimeError(
                    f"Duplicate key detected but record not found for "
                    f"transaction_id={payload.transaction_id!r}"
                ) from exc
            return existing, False

        # Any other integrity violation (NOT NULL, data truncation, …) is a
        # real persistence failure — propagate so the caller can return 503.
        logger.error(
            "Unexpected IntegrityError while inserting transaction_id=%r: %s",
            payload.transaction_id,
            exc,
        )
        raise

    # INSERT succeeded — commit and reload the full row (picks up server defaults
    # like status='PENDING' and attempts=0 set by MySQL's DEFAULT clause).
    db.commit()
    db.refresh(txn)

    logger.info(
        "Transaction created: transaction_id=%r id=%s status=%s",
        txn.transaction_id,
        txn.id,
        txn.status,
    )
    return txn, True


def get_transaction(
    db: Session,
    transaction_id: str,
) -> Transaction | None:
    """
    Look up a transaction by its payment-provider transaction_id.

    Returns the Transaction ORM object, or None if not found.
    The UNIQUE index on transaction_id makes this a single-row index scan.
    """
    stmt = select(Transaction).where(Transaction.transaction_id == transaction_id)
    return db.execute(stmt).scalar_one_or_none()
