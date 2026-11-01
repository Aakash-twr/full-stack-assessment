"""
tests/test_idempotency.py
──────────────────────────
Test 4: Duplicate Prevention

Verifies:
• Sending the same webhook 3× returns HTTP 202 each time.
• Only one record exists in MySQL.
• The transaction is processed only once.
• An already-PROCESSED transaction is NOT reset by a subsequent duplicate.
• Multiple simultaneous requests with the same transaction_id are safe
  (tests the database-level UNIQUE constraint, not the application layer).
"""

from __future__ import annotations

import concurrent.futures
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

DUPLICATE_PAYLOAD = {
    "transaction_id": "txn_dup_test_001",
    "source_account": "acc_user_789",
    "destination_account": "acc_merchant_456",
    "amount": 1500,
    "currency": "INR",
}


class TestIdempotency:
    # ── Sequential duplicates ──────────────────────────────────────────────────

    def test_first_request_returns_202(self, client: TestClient):
        r = client.post("/v1/webhooks/transactions", json=DUPLICATE_PAYLOAD)
        assert r.status_code == 202

    def test_second_duplicate_returns_202(self, client: TestClient):
        client.post("/v1/webhooks/transactions", json=DUPLICATE_PAYLOAD)
        r = client.post("/v1/webhooks/transactions", json=DUPLICATE_PAYLOAD)
        assert r.status_code == 202

    def test_third_duplicate_returns_202(self, client: TestClient):
        for _ in range(3):
            r = client.post("/v1/webhooks/transactions", json=DUPLICATE_PAYLOAD)
            assert r.status_code == 202

    def test_only_one_record_after_three_requests(
        self, client: TestClient, db_session: Session
    ):
        for _ in range(3):
            client.post("/v1/webhooks/transactions", json=DUPLICATE_PAYLOAD)

        from app.models.transaction import Transaction
        count = db_session.query(Transaction).filter_by(
            transaction_id=DUPLICATE_PAYLOAD["transaction_id"]
        ).count()
        assert count == 1, f"Expected 1 record, found {count}"

    def test_duplicate_does_not_reset_processed_transaction(
        self, client: TestClient, db_session: Session, insert_transaction
    ):
        """
        A PROCESSED transaction must not be reset to PENDING by a late duplicate.
        """
        from datetime import datetime, timezone
        from app.models.transaction import TransactionStatus

        processed_at = datetime(2026, 11, 1, 4, 10, 30)
        insert_transaction(
            transaction_id=DUPLICATE_PAYLOAD["transaction_id"],
            status=TransactionStatus.PROCESSED,
            attempts=1,
            processed_at=processed_at,
        )

        # Send duplicate after it's already PROCESSED
        r = client.post("/v1/webhooks/transactions", json=DUPLICATE_PAYLOAD)
        assert r.status_code == 202  # Still 202

        from app.models.transaction import Transaction
        txn = db_session.query(Transaction).filter_by(
            transaction_id=DUPLICATE_PAYLOAD["transaction_id"]
        ).first()

        db_session.refresh(txn)
        assert txn.status == TransactionStatus.PROCESSED, (
            "Duplicate webhook must not reset a PROCESSED transaction"
        )
        assert txn.processed_at is not None, "processed_at must not be cleared"

    def test_duplicate_does_not_overwrite_data(
        self, client: TestClient, db_session: Session
    ):
        """Original amount/currency must not be overwritten by a duplicate with different values."""
        # Insert original
        client.post("/v1/webhooks/transactions", json=DUPLICATE_PAYLOAD)

        # Send duplicate with different amount (should be ignored)
        different_payload = {**DUPLICATE_PAYLOAD, "amount": 9999}
        client.post("/v1/webhooks/transactions", json=different_payload)

        from app.models.transaction import Transaction
        txn = db_session.query(Transaction).filter_by(
            transaction_id=DUPLICATE_PAYLOAD["transaction_id"]
        ).first()
        db_session.refresh(txn)

        assert str(txn.amount) == "1500.00", (
            "Duplicate webhook must not overwrite original transaction data"
        )

    # ── Concurrent duplicates (DB-level constraint test) ──────────────────────

    def test_concurrent_duplicates_only_one_record(
        self, client: TestClient, db_session: Session
    ):
        """
        Fire 5 simultaneous requests with the same transaction_id.
        The MySQL UNIQUE constraint must ensure exactly one record is created.
        All requests must receive HTTP 202.
        """
        def send():
            return client.post("/v1/webhooks/transactions", json=DUPLICATE_PAYLOAD)

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(send) for _ in range(5)]
            responses = [f.result() for f in concurrent.futures.as_completed(futures)]

        statuses = [r.status_code for r in responses]
        assert all(s == 202 for s in statuses), (
            f"All 5 concurrent requests must return 202, got: {statuses}"
        )

        from app.models.transaction import Transaction
        count = db_session.query(Transaction).filter_by(
            transaction_id=DUPLICATE_PAYLOAD["transaction_id"]
        ).count()
        assert count == 1, (
            f"Concurrent duplicates must produce exactly 1 DB record, found {count}"
        )

    def test_concurrent_duplicates_status_is_processing(
        self, client: TestClient
    ):
        """After concurrent duplicates, status must still be PROCESSING (not corrupted)."""
        def send():
            return client.post("/v1/webhooks/transactions", json=DUPLICATE_PAYLOAD)

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(send) for _ in range(5)]
            [f.result() for f in concurrent.futures.as_completed(futures)]

        r = client.get(f"/v1/transactions/{DUPLICATE_PAYLOAD['transaction_id']}")
        assert r.status_code == 200
        assert r.json()[0]["status"] == "PROCESSING"

    # ── Worker idempotency ─────────────────────────────────────────────────────

    def test_only_one_worker_can_claim_the_job(
        self, db_session: Session, insert_transaction, db_engine
    ):
        """
        Two concurrent claim_next_job() calls on the same PENDING transaction
        must result in exactly one successful claim.
        """
        from app.models.transaction import Transaction, TransactionStatus
        from app.worker.processor import claim_next_job
        from sqlalchemy.orm import sessionmaker
        import threading

        # Insert one PENDING transaction directly
        insert_transaction(transaction_id="txn_claim_race_001")

        WorkerSession = sessionmaker(
            bind=db_engine, autocommit=False, autoflush=False, expire_on_commit=False
        )

        claims = []
        errors = []

        def try_claim():
            db = WorkerSession()
            try:
                result = claim_next_job(db)
                claims.append(result)
            except Exception as e:
                errors.append(e)
            finally:
                db.close()

        threads = [threading.Thread(target=try_claim) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Exactly one claim should succeed; the others get None (SKIP LOCKED)
        successful = [c for c in claims if c is not None]
        assert len(successful) == 1, (
            f"Expected exactly 1 successful claim, got {len(successful)}: {claims}"
        )
        assert not errors, f"Claim raised errors: {errors}"
