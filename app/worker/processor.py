"""
app/worker/processor.py
────────────────────────
Core job-claiming and processing logic for the background worker.

Architecture
────────────
This module implements the transaction table as a persistent job queue.
Each function corresponds to one step in the processing lifecycle:

  claim_next_job()      – atomically claim one eligible transaction
  process_job()         – simulate external API call (configurable sleep)
  complete_job()        – mark PROCESSED (verified by lease_token)
  fail_job()            – record error, schedule retry or mark FAILED
  release_stale_lease() – (internal) used inside claim for expired leases

Atomic claiming design
──────────────────────
  BEGIN
    SELECT ... FOR UPDATE SKIP LOCKED   ← holds row lock
    UPDATE status=PROCESSING, lease=..., expires=... ← takes ownership
  COMMIT                                ← releases row lock immediately

The row lock is released on COMMIT — before the 30-second sleep begins.
This means multiple worker processes can claim different transactions
simultaneously without blocking each other.

Crash recovery
──────────────
Eligible rows for claiming include:
  (a) PENDING   with next_attempt_at IS NULL or <= NOW()
  (b) PROCESSING with lease_expires_at <= NOW() and attempts < MAX_ATTEMPTS

When a worker crashes mid-job, condition (b) reclaims it automatically
once the lease expires (default: 120 s).

Lease-token guard on completion
───────────────────────────────
All UPDATE statements that mark a job PROCESSED or FAILED include:
    WHERE id = :id AND lease_token = :token

If another worker already reclaimed the job (e.g. after our lease expired
during an unexpectedly long sleep), our UPDATE matches 0 rows and we simply
discard the result without overwriting the new worker's state.
"""

from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import text, update
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.models.transaction import Transaction, TransactionStatus

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _utcnow() -> datetime:
    """Naive UTC datetime for consistent DB storage."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _new_lease_token() -> str:
    """Generate a fresh UUID4 string to identify this worker's lease."""
    return str(uuid.uuid4())


def _backoff_seconds(attempts: int) -> int:
    """
    Exponential back-off: base * 2^(attempt - 1), capped at 5 minutes.

    attempts=1 → base * 1
    attempts=2 → base * 2
    attempts=3 → base * 4
    ...
    """
    settings = get_settings()
    delay = settings.retry_base_delay_seconds * (2 ** (attempts - 1))
    return min(delay, 300)  # cap at 5 minutes


# ---------------------------------------------------------------------------
# Job claiming
# ---------------------------------------------------------------------------


def claim_next_job(db: Session) -> tuple[Transaction, str] | None:
    """
    Atomically find and claim the next eligible transaction.

    Returns (transaction, lease_token) if a job was claimed, or None if the
    queue is empty.

    Eligible rows
    ─────────────
    (a) status=PENDING  AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
    (b) status=PROCESSING AND lease_expires_at <= NOW() AND attempts < MAX_ATTEMPTS

    The SELECT … FOR UPDATE SKIP LOCKED ensures:
    • Only one worker claims a given row (database-level mutual exclusion).
    • Other workers skip locked rows rather than blocking, enabling parallelism.

    The UPDATE and COMMIT happen inside the same transaction, releasing the
    lock before any slow work begins.
    """
    settings = get_settings()
    now = _utcnow()

    try:
        # One transaction: select + update + commit.
        # We use a raw text query for the FOR UPDATE SKIP LOCKED clause,
        # which SQLAlchemy ORM does not natively support in all versions.
        row = db.execute(
            text(
                """
                SELECT id
                FROM   transactions
                WHERE  (
                           status = 'PENDING'
                           AND (next_attempt_at IS NULL OR next_attempt_at <= :now)
                       )
                OR     (
                           status = 'PROCESSING'
                           AND lease_expires_at <= :now
                           AND attempts < :max_attempts
                       )
                ORDER  BY id ASC
                LIMIT  1
                FOR    UPDATE SKIP LOCKED
                """
            ),
            {"now": now, "max_attempts": settings.max_attempts},
        ).fetchone()

        if row is None:
            return None  # Queue is empty.

        txn_id = row[0]

        # Fetch the full ORM object within the same transaction so we can
        # update it via the ORM and get back the refreshed state.
        txn = db.get(Transaction, txn_id)
        if txn is None:
            # Should never happen: we just locked it.
            logger.warning("Row id=%s disappeared after FOR UPDATE SKIP LOCKED", txn_id)
            db.rollback()
            return None

        lease_token = _new_lease_token()
        lease_expires_at = now + timedelta(seconds=settings.job_lease_seconds)
        is_recovery = txn.status == TransactionStatus.PROCESSING

        # Update job state — take ownership.
        txn.status = TransactionStatus.PROCESSING
        txn.lease_token = lease_token
        txn.lease_expires_at = lease_expires_at
        txn.attempts = (txn.attempts or 0) + 1

        # Record the very first processing start time.
        if txn.processing_started_at is None:
            txn.processing_started_at = now

        db.commit()  # Releases the FOR UPDATE lock immediately.

        if is_recovery:
            logger.warning(
                "Recovered stale job: transaction_id=%r id=%s attempts=%d",
                txn.transaction_id,
                txn.id,
                txn.attempts,
            )
        else:
            logger.info(
                "Job claimed: transaction_id=%r id=%s attempt=%d/%d lease=%s",
                txn.transaction_id,
                txn.id,
                txn.attempts,
                settings.max_attempts,
                lease_token,
            )

        return txn, lease_token

    except Exception as exc:
        db.rollback()
        # MySQL error 1213: Deadlock found when trying to get lock; try restarting transaction
        # Under high concurrency, another worker claimed or updated rows in the range.
        # Rolling back and returning None allows the worker to poll again on the next cycle safely.
        orig = getattr(exc, "orig", None)
        if orig and hasattr(orig, "args") and len(orig.args) > 0 and orig.args[0] == 1213:
            logger.debug("Deadlock (1213) encountered during SKIP LOCKED claim; safely skipping cycle.")
            return None
        raise


# ---------------------------------------------------------------------------
# Simulated processing
# ---------------------------------------------------------------------------


def process_job(txn: Transaction) -> None:
    """
    Simulate the external payment-processor API call.

    In production this would call an external HTTP API.  Here we sleep for
    PROCESSING_DELAY_SECONDS (default 30, override to 2 in tests).

    This function is called AFTER the database transaction that claimed the
    job has been committed, so no database locks are held during the sleep.
    """
    settings = get_settings()
    delay = settings.processing_delay_seconds

    logger.info(
        "Processing started: transaction_id=%r id=%s delay=%ds",
        txn.transaction_id,
        txn.id,
        delay,
    )
    time.sleep(delay)
    logger.info(
        "Processing finished: transaction_id=%r id=%s",
        txn.transaction_id,
        txn.id,
    )


# ---------------------------------------------------------------------------
# Job completion
# ---------------------------------------------------------------------------


def complete_job(db: Session, txn: Transaction, lease_token: str) -> bool:
    """
    Mark a successfully processed transaction as PROCESSED.

    The WHERE clause includes lease_token = :token to guard against the
    scenario where:
      1. Our lease expired.
      2. Another worker claimed and is now processing the same transaction.
      3. We try to complete — but we must not overwrite the newer worker's state.

    Returns True if the update was applied (we still owned the lease).
    Returns False if another worker already took over (lease_token mismatch).
    """
    now = _utcnow()

    try:
        result = db.execute(
            update(Transaction)
            .where(
                Transaction.id == txn.id,
                Transaction.lease_token == lease_token,
            )
            .values(
                status=TransactionStatus.PROCESSED,
                processed_at=now,
                lease_token=None,
                lease_expires_at=None,
                last_error=None,
            )
        )
        db.commit()

        rows_affected = result.rowcount
        if rows_affected == 0:
            logger.warning(
                "Completion ignored — lease no longer valid: "
                "transaction_id=%r id=%s lease=%s (another worker took over)",
                txn.transaction_id,
                txn.id,
                lease_token,
            )
            return False

        logger.info(
            "Transaction PROCESSED: transaction_id=%r id=%s processed_at=%s",
            txn.transaction_id,
            txn.id,
            now.isoformat() + "Z",
        )
        return True

    except Exception:
        db.rollback()
        raise


# ---------------------------------------------------------------------------
# Job failure / retry
# ---------------------------------------------------------------------------


def fail_job(
    db: Session,
    txn: Transaction,
    lease_token: str,
    error: Exception,
) -> None:
    """
    Handle a processing failure.

    If attempts < MAX_ATTEMPTS:
        • Set status = PENDING so the worker picks it up again.
        • Schedule next_attempt_at using exponential back-off.
        • Store the error message in last_error.
        • Clear the lease.

    If attempts >= MAX_ATTEMPTS:
        • Set status = FAILED (permanently; no more retries).
        • Store the final error message.
        • Clear the lease.

    The lease_token guard prevents a stale worker from incorrectly scheduling
    retries for a job already reclaimed by another worker.
    """
    settings = get_settings()
    now = _utcnow()
    error_message = f"{type(error).__name__}: {error}"

    try:
        current_attempts = txn.attempts or 1

        if current_attempts >= settings.max_attempts:
            # Permanently failed — no more retries.
            result = db.execute(
                update(Transaction)
                .where(
                    Transaction.id == txn.id,
                    Transaction.lease_token == lease_token,
                )
                .values(
                    status=TransactionStatus.FAILED,
                    lease_token=None,
                    lease_expires_at=None,
                    next_attempt_at=None,
                    last_error=error_message,
                )
            )
            db.commit()

            if result.rowcount > 0:
                logger.error(
                    "Transaction permanently FAILED after %d attempts: "
                    "transaction_id=%r id=%s error=%s",
                    current_attempts,
                    txn.transaction_id,
                    txn.id,
                    error_message,
                )
            else:
                logger.warning(
                    "Fail-job update ignored — lease no longer valid: "
                    "transaction_id=%r id=%s",
                    txn.transaction_id,
                    txn.id,
                )
        else:
            # Schedule a retry.
            delay = _backoff_seconds(current_attempts)
            next_attempt_at = now + timedelta(seconds=delay)

            result = db.execute(
                update(Transaction)
                .where(
                    Transaction.id == txn.id,
                    Transaction.lease_token == lease_token,
                )
                .values(
                    status=TransactionStatus.PENDING,
                    lease_token=None,
                    lease_expires_at=None,
                    next_attempt_at=next_attempt_at,
                    last_error=error_message,
                )
            )
            db.commit()

            if result.rowcount > 0:
                logger.warning(
                    "Processing failed — retry scheduled in %ds (attempt %d/%d): "
                    "transaction_id=%r id=%s error=%s",
                    delay,
                    current_attempts,
                    settings.max_attempts,
                    txn.transaction_id,
                    txn.id,
                    error_message,
                )
            else:
                logger.warning(
                    "Retry-schedule ignored — lease no longer valid: "
                    "transaction_id=%r id=%s",
                    txn.transaction_id,
                    txn.id,
                )

    except Exception:
        db.rollback()
        raise
