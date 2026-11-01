"""
tests/test_worker.py
─────────────────────
Test 3:  Background Processing (2-second delay in tests)
Test 7:  Worker Recovery (expired lease reclaimed)
Test 8:  Failure Handling (retries, FAILED state)
Test 9:  Database Unavailability (API returns 503 not 202)

Notes
─────
• PROCESSING_DELAY_SECONDS is overridden to 2 s in conftest.py.
• JOB_LEASE_SECONDS is overridden to 10 s so recovery tests run quickly.
• MAX_ATTEMPTS is overridden to 3 for failure tests.
• Worker functions are called directly (not via a subprocess) for speed.
"""

from __future__ import annotations

import time
import threading
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.database import SessionLocal
from app.models.transaction import Transaction, TransactionStatus
from app.worker import processor
from app.worker.main import _run_one_iteration
from tests.conftest import TEST_SETTINGS_OVERRIDES, make_test_settings


# ---------------------------------------------------------------------------
# Helper to get a fresh test session (uses TEST_DATABASE_URL engine)
# ---------------------------------------------------------------------------


def _test_session(db_engine) -> Session:
    from sqlalchemy.orm import sessionmaker
    S = sessionmaker(bind=db_engine, autocommit=False, autoflush=False,
                     expire_on_commit=False)
    return S()


# ---------------------------------------------------------------------------
# Test 3: Background Processing
# ---------------------------------------------------------------------------


class TestBackgroundProcessing:
    def test_transaction_processed_after_delay(
        self, client: TestClient, db_engine, insert_transaction
    ):
        """
        Insert a PENDING transaction, run one worker iteration (2-second delay),
        and verify status becomes PROCESSED with processed_at populated.
        """
        txn = insert_transaction(transaction_id="txn_bg_process_001")

        test_settings = make_test_settings()
        db = _test_session(db_engine)

        try:
            with patch("app.worker.processor.get_settings", return_value=test_settings), \
                 patch("app.worker.main.processor.get_settings", return_value=test_settings):

                result = processor.claim_next_job(db)
                assert result is not None, "Worker should have claimed the job"
                claimed_txn, lease_token = result
                db.close()

                # Simulate the 2-second processing delay
                db2 = _test_session(db_engine)
                with patch("app.worker.processor.get_settings", return_value=test_settings):
                    processor.process_job(claimed_txn)
                    ok = processor.complete_job(db2, claimed_txn, lease_token)
                db2.close()

        finally:
            pass

        assert ok is True, "complete_job should return True"

        # Verify in DB via a fresh session
        db3 = _test_session(db_engine)
        refreshed = db3.query(Transaction).filter_by(
            transaction_id="txn_bg_process_001"
        ).first()
        db3.close()

        assert refreshed.status == TransactionStatus.PROCESSED
        assert refreshed.processed_at is not None

    def test_processed_at_is_approximately_delay_after_start(
        self, db_engine, insert_transaction
    ):
        """processed_at should be set approximately 2 s after processing_started_at."""
        txn = insert_transaction(transaction_id="txn_bg_timing_001")
        test_settings = make_test_settings()

        db = _test_session(db_engine)
        with patch("app.worker.processor.get_settings", return_value=test_settings):
            result = processor.claim_next_job(db)
        assert result is not None
        claimed_txn, lease_token = result
        db.close()

        before_sleep = datetime.now(timezone.utc).replace(tzinfo=None)

        db2 = _test_session(db_engine)
        with patch("app.worker.processor.get_settings", return_value=test_settings):
            processor.process_job(claimed_txn)
            processor.complete_job(db2, claimed_txn, lease_token)
        db2.close()

        after_sleep = datetime.now(timezone.utc).replace(tzinfo=None)
        elapsed = (after_sleep - before_sleep).total_seconds()

        # Should be ~2 s ± 1 s tolerance
        assert 1.5 <= elapsed <= 5.0, (
            f"Processing took {elapsed:.2f}s — expected ~2s"
        )

    def test_status_queryable_via_api_after_processing(
        self, client: TestClient, db_engine, insert_transaction
    ):
        """After processing completes, the GET endpoint must show PROCESSED."""
        txn = insert_transaction(transaction_id="txn_bg_api_001")
        test_settings = make_test_settings()

        db = _test_session(db_engine)
        with patch("app.worker.processor.get_settings", return_value=test_settings):
            result = processor.claim_next_job(db)
        assert result is not None
        claimed_txn, lease_token = result
        db.close()

        db2 = _test_session(db_engine)
        with patch("app.worker.processor.get_settings", return_value=test_settings):
            processor.process_job(claimed_txn)
            processor.complete_job(db2, claimed_txn, lease_token)
        db2.close()

        response = client.get(f"/v1/transactions/{txn.transaction_id}")
        assert response.status_code == 200
        item = response.json()[0]
        assert item["status"] == "PROCESSED"
        assert item["processed_at"] is not None
        assert item["processed_at"].endswith("Z")


# ---------------------------------------------------------------------------
# Test 7: Worker Recovery (expired lease reclaimed)
# ---------------------------------------------------------------------------


class TestWorkerRecovery:
    def test_expired_lease_reclaimed_by_another_worker(
        self, db_engine, insert_transaction
    ):
        """
        A PROCESSING transaction with an expired lease must be re-claimable.
        """
        expired_at = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(seconds=30)
        txn = insert_transaction(
            transaction_id="txn_recovery_001",
            status=TransactionStatus.PROCESSING,
            attempts=1,
            lease_token="old-crashed-worker-token",
            lease_expires_at=expired_at,
            processing_started_at=datetime.now(timezone.utc).replace(tzinfo=None),
        )

        test_settings = make_test_settings()
        db = _test_session(db_engine)
        with patch("app.worker.processor.get_settings", return_value=test_settings):
            result = processor.claim_next_job(db)
        db.close()

        assert result is not None, "Expired-lease job must be claimable"
        recovered_txn, new_lease = result
        assert recovered_txn.transaction_id == txn.transaction_id
        assert recovered_txn.lease_token != "old-crashed-worker-token"
        assert recovered_txn.attempts == 2  # incremented

    def test_recovered_job_can_be_completed(
        self, db_engine, insert_transaction
    ):
        """After recovery, the job must process to PROCESSED state."""
        expired_at = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(seconds=30)
        insert_transaction(
            transaction_id="txn_recovery_002",
            status=TransactionStatus.PROCESSING,
            attempts=1,
            lease_token="stale-token",
            lease_expires_at=expired_at,
            processing_started_at=datetime.now(timezone.utc).replace(tzinfo=None),
        )

        test_settings = make_test_settings()

        db = _test_session(db_engine)
        with patch("app.worker.processor.get_settings", return_value=test_settings):
            result = processor.claim_next_job(db)
        assert result is not None
        claimed_txn, lease_token = result
        db.close()

        db2 = _test_session(db_engine)
        with patch("app.worker.processor.get_settings", return_value=test_settings):
            processor.process_job(claimed_txn)
            processor.complete_job(db2, claimed_txn, lease_token)
        db2.close()

        db3 = _test_session(db_engine)
        final = db3.query(Transaction).filter_by(
            transaction_id="txn_recovery_002"
        ).first()
        db3.close()

        assert final.status == TransactionStatus.PROCESSED
        assert final.processed_at is not None

    def test_stale_worker_cannot_overwrite_recovery(
        self, db_engine, insert_transaction
    ):
        """
        A crashed worker that wakes up late must NOT overwrite the new worker's result.
        Simulated by calling complete_job with the old (stale) lease token.
        """
        insert_transaction(transaction_id="txn_race_001")
        test_settings = make_test_settings()

        # Worker A claims the job
        db_a = _test_session(db_engine)
        with patch("app.worker.processor.get_settings", return_value=test_settings):
            result_a = processor.claim_next_job(db_a)
        assert result_a is not None
        txn_a, lease_a = result_a
        db_a.close()

        # Worker B also claims (simulating A's lease expiring)
        # To make B win, we manually expire A's lease in the DB
        db_expire = _test_session(db_engine)
        from sqlalchemy import update as sa_update
        db_expire.execute(
            sa_update(Transaction)
            .where(Transaction.transaction_id == "txn_race_001")
            .values(
                lease_expires_at=datetime.now(timezone.utc).replace(tzinfo=None)
                                  - timedelta(seconds=1)
            )
        )
        db_expire.commit()
        db_expire.close()

        db_b = _test_session(db_engine)
        with patch("app.worker.processor.get_settings", return_value=test_settings):
            result_b = processor.claim_next_job(db_b)
        assert result_b is not None
        txn_b, lease_b = result_b
        db_b.close()

        # B completes successfully
        db_complete_b = _test_session(db_engine)
        with patch("app.worker.processor.get_settings", return_value=test_settings):
            processor.process_job(txn_b)
            b_ok = processor.complete_job(db_complete_b, txn_b, lease_b)
        db_complete_b.close()

        assert b_ok is True

        # A wakes up late and tries to complete — must be rejected
        db_complete_a = _test_session(db_engine)
        a_ok = processor.complete_job(db_complete_a, txn_a, lease_a)
        db_complete_a.close()

        assert a_ok is False, "Stale worker A must not overwrite worker B's result"

        # Final state must reflect B's completion
        db_check = _test_session(db_engine)
        final = db_check.query(Transaction).filter_by(
            transaction_id="txn_race_001"
        ).first()
        db_check.close()

        assert final.status == TransactionStatus.PROCESSED


# ---------------------------------------------------------------------------
# Test 8: Failure Handling
# ---------------------------------------------------------------------------


class TestFailureHandling:
    def test_processing_error_schedules_retry(
        self, db_engine, insert_transaction
    ):
        """A processing error on attempt 1 of 3 must schedule a retry (PENDING)."""
        insert_transaction(transaction_id="txn_fail_retry_001")
        test_settings = make_test_settings()

        db = _test_session(db_engine)
        with patch("app.worker.processor.get_settings", return_value=test_settings):
            result = processor.claim_next_job(db)
        assert result is not None
        claimed_txn, lease_token = result
        db.close()

        db2 = _test_session(db_engine)
        with patch("app.worker.processor.get_settings", return_value=test_settings):
            processor.fail_job(db2, claimed_txn, lease_token, RuntimeError("network error"))
        db2.close()

        db3 = _test_session(db_engine)
        txn = db3.query(Transaction).filter_by(
            transaction_id="txn_fail_retry_001"
        ).first()
        db3.close()

        assert txn.status == TransactionStatus.PENDING, (
            "Failed job with attempts < max must go back to PENDING for retry"
        )
        assert txn.next_attempt_at is not None, "next_attempt_at must be scheduled"
        assert txn.last_error is not None
        assert "network error" in txn.last_error

    def test_max_attempts_marks_failed(
        self, db_engine, insert_transaction
    ):
        """After max_attempts (3) failures, status must become FAILED."""
        test_settings = make_test_settings()  # max_attempts=3

        insert_transaction(
            transaction_id="txn_fail_max_001",
            status=TransactionStatus.PENDING,
            attempts=0,
        )

        for attempt_num in range(1, 4):  # attempts 1, 2, 3
            # Force next_attempt_at to past so it is immediately eligible for claiming
            with db_engine.begin() as conn:
                conn.execute(
                    text(
                        "UPDATE transactions SET next_attempt_at = '2000-01-01 00:00:00' "
                        "WHERE transaction_id = 'txn_fail_max_001'"
                    )
                )

            with _test_session(db_engine) as db:
                with patch("app.worker.processor.get_settings", return_value=test_settings):
                    result = processor.claim_next_job(db)
                assert result is not None, f"Failed to claim on attempt {attempt_num}"
                claimed_txn, lease_token = result

            with _test_session(db_engine) as db_fail:
                with patch("app.worker.processor.get_settings", return_value=test_settings):
                    processor.fail_job(
                        db_fail, claimed_txn, lease_token,
                        RuntimeError(f"error attempt {attempt_num}")
                    )

        # Final check
        with _test_session(db_engine) as db_check:
            txn = db_check.query(Transaction).filter_by(
                transaction_id="txn_fail_max_001"
            ).first()
            assert txn.status == TransactionStatus.FAILED, (
                f"After max attempts, status must be FAILED, got {txn.status}"
            )
            assert txn.last_error is not None

    def test_error_stored_in_last_error(
        self, db_engine, insert_transaction
    ):
        """The last_error column must contain the most recent error message."""
        insert_transaction(transaction_id="txn_last_error_001")
        test_settings = make_test_settings()

        db = _test_session(db_engine)
        with patch("app.worker.processor.get_settings", return_value=test_settings):
            result = processor.claim_next_job(db)
        assert result is not None
        claimed_txn, lease_token = result
        db.close()

        db2 = _test_session(db_engine)
        with patch("app.worker.processor.get_settings", return_value=test_settings):
            processor.fail_job(
                db2, claimed_txn, lease_token,
                ValueError("payment gateway timeout")
            )
        db2.close()

        db3 = _test_session(db_engine)
        txn = db3.query(Transaction).filter_by(
            transaction_id="txn_last_error_001"
        ).first()
        db3.close()

        assert txn.last_error is not None
        assert "payment gateway timeout" in txn.last_error

    def test_failed_transaction_not_retried(
        self, db_engine, insert_transaction
    ):
        """A FAILED transaction (attempts >= max) must not be picked up by the worker."""
        test_settings = make_test_settings()  # max_attempts=3
        insert_transaction(
            transaction_id="txn_no_retry_001",
            status=TransactionStatus.FAILED,
            attempts=3,  # at max
        )

        db = _test_session(db_engine)
        with patch("app.worker.processor.get_settings", return_value=test_settings):
            result = processor.claim_next_job(db)
        db.close()

        assert result is None, "FAILED transaction must not be claimed by worker"


# ---------------------------------------------------------------------------
# Test 9: Database Unavailability
# ---------------------------------------------------------------------------


class TestDatabaseUnavailability:
    def test_api_returns_503_not_202_when_db_down(self, client: TestClient):
        """
        When MySQL is unavailable, POST /v1/webhooks/transactions must return
        HTTP 503 — never HTTP 202 — because the transaction was not persisted.
        """
        from sqlalchemy.exc import OperationalError

        with patch(
            "app.services.transaction_service.create_transaction",
            side_effect=OperationalError("stmt", {}, Exception("Connection refused")),
        ):
            response = client.post(
                "/v1/webhooks/transactions",
                json={
                    "transaction_id": "txn_db_down_001",
                    "source_account": "acc_src",
                    "destination_account": "acc_dst",
                    "amount": 100,
                    "currency": "INR",
                },
            )

        assert response.status_code == 503, (
            f"Expected HTTP 503 when DB is down, got {response.status_code}. "
            "Must never return 202 for an unpersisted transaction."
        )

    def test_health_check_returns_unhealthy_when_db_down(self, client: TestClient):
        """Health endpoint must report UNHEALTHY when DB is unavailable."""
        with patch(
            "app.core.database.check_database_connection", return_value=False
        ):
            response = client.get("/")

        assert response.status_code == 503
        assert response.json()["status"] == "UNHEALTHY"

    def test_worker_survives_db_connection_failure(self, db_engine):
        """
        Worker must not crash when it cannot reach the database.
        It should log the error and return False (idle iteration).
        """
        from sqlalchemy.exc import OperationalError

        with patch(
            "app.worker.main.processor.claim_next_job",
            side_effect=OperationalError("stmt", {}, Exception("DB down")),
        ), patch("app.worker.main.SessionLocal"):
            # _run_one_iteration should catch the error and return False
            result = _run_one_iteration()

        assert result is False, "Worker iteration must return False on DB error"
