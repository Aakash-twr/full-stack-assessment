"""
app/worker/main.py
───────────────────
Background worker entry point.

Run with:
    python -m app.worker.main

The worker runs an infinite polling loop:
  1. Open a fresh DB session.
  2. Attempt to claim one eligible transaction.
  3. If claimed, process it (sleep 30 s), then mark it PROCESSED.
  4. If nothing to claim, sleep WORKER_POLL_INTERVAL_SECONDS and try again.
  5. On any unhandled exception, log it and continue after a short pause.

Graceful shutdown
─────────────────
SIGTERM and SIGINT (Ctrl+C) set a threading.Event that the main loop checks
between iterations.  The current iteration always completes (including the
30-second sleep and the final DB update) before the process exits.  This
prevents a transaction from being left in PROCESSING state due to a clean
shutdown.  Docker sends SIGTERM before SIGKILL, giving the worker 120 s
(configurable) to finish.

Separate process
─────────────────
The worker is started as a separate OS process by Docker Compose:
    command: python -m app.worker.main

It shares no memory with the FastAPI process — only the MySQL database.
"""

from __future__ import annotations

import logging
import logging.config
import signal
import threading
import time

from app.core.config import get_settings
from app.core.database import SessionLocal
from app.worker import processor


# ---------------------------------------------------------------------------
# Logging setup (mirrors app/main.py so log lines look consistent)
# ---------------------------------------------------------------------------


def _configure_logging(level: str) -> None:
    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "standard": {
                    "format": "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
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
            "root": {"level": level, "handlers": ["console"]},
            "loggers": {
                "sqlalchemy.engine": {
                    "level": "WARNING" if level != "DEBUG" else "DEBUG",
                    "propagate": True,
                },
            },
        }
    )


logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shutdown coordination
# ---------------------------------------------------------------------------

_shutdown_event = threading.Event()


def _handle_signal(signum: int, frame: object) -> None:  # noqa: ARG001
    sig_name = signal.Signals(signum).name
    logger.info("Received %s — shutting down after current job completes…", sig_name)
    _shutdown_event.set()


# ---------------------------------------------------------------------------
# One iteration of the poll loop
# ---------------------------------------------------------------------------


def _run_one_iteration() -> bool:
    """
    Claim and process one job.

    Returns True if a job was processed (busy iteration).
    Returns False if the queue was empty (idle iteration).
    """
    db = SessionLocal()
    try:
        result = processor.claim_next_job(db)
        if result is None:
            return False  # Nothing to do.

        txn, lease_token = result
    except Exception as exc:
        logger.error("Error while claiming job: %s", exc, exc_info=True)
        db.close()
        return False

    # The claim transaction is committed — lock is released.
    # Close the session used for claiming; open a fresh one for completion.
    # This prevents the completion update from running in a tainted session.
    db.close()

    # ── Simulate external payment-processor call ───────────────────────────
    processing_error: Exception | None = None
    try:
        processor.process_job(txn)
    except Exception as exc:
        logger.error(
            "Processing error for transaction_id=%r: %s",
            txn.transaction_id,
            exc,
            exc_info=True,
        )
        processing_error = exc

    # ── Write the outcome back to MySQL ────────────────────────────────────
    db2 = SessionLocal()
    try:
        if processing_error is None:
            processor.complete_job(db2, txn, lease_token)
        else:
            processor.fail_job(db2, txn, lease_token, processing_error)
    except Exception as exc:
        logger.error(
            "Failed to update job outcome for transaction_id=%r: %s",
            txn.transaction_id,
            exc,
            exc_info=True,
        )
    finally:
        db2.close()

    return True


# ---------------------------------------------------------------------------
# Main polling loop
# ---------------------------------------------------------------------------


def run_worker() -> None:
    """
    Blocking polling loop — runs until _shutdown_event is set.

    Idle behaviour: when the queue is empty, sleep WORKER_POLL_INTERVAL_SECONDS
    between checks.  We sleep in small increments so the shutdown signal is
    noticed promptly.
    """
    settings = get_settings()
    poll_interval = settings.worker_poll_interval_seconds

    logger.info(
        "Worker started | poll_interval=%.1fs processing_delay=%ds max_attempts=%d",
        poll_interval,
        settings.processing_delay_seconds,
        settings.max_attempts,
    )

    consecutive_errors = 0

    while not _shutdown_event.is_set():
        try:
            busy = _run_one_iteration()
            consecutive_errors = 0

            if not busy:
                # Queue is empty — sleep before polling again.
                # Check shutdown every 0.5 s instead of sleeping the full
                # interval in one shot, so SIGTERM is noticed quickly.
                elapsed = 0.0
                while elapsed < poll_interval and not _shutdown_event.is_set():
                    time.sleep(min(0.5, poll_interval - elapsed))
                    elapsed += 0.5

        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received — stopping worker.")
            break

        except Exception as exc:
            consecutive_errors += 1
            # Exponential back-off for repeated failures (max 60 s).
            backoff = min(2 ** consecutive_errors, 60)
            logger.error(
                "Unhandled error in worker loop (attempt %d), "
                "backing off %ds: %s",
                consecutive_errors,
                backoff,
                exc,
                exc_info=True,
            )
            time.sleep(backoff)

    logger.info("Worker stopped cleanly.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    settings = get_settings()
    _configure_logging(settings.log_level)

    # Register signal handlers for graceful shutdown.
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    run_worker()


if __name__ == "__main__":
    main()
