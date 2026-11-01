"""
tests/test_webhooks.py
───────────────────────
Test 2: Single Transaction Webhook

Verifies:
• Valid webhook returns HTTP 202 within 500 ms.
• Exactly one transaction is created in MySQL.
• Initial public status is PROCESSING (internal PENDING mapped).
• processed_at is null before worker runs.
• Validation errors return HTTP 422.
• DB failure returns HTTP 503 (not 202).
"""

from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

VALID_PAYLOAD = {
    "transaction_id": "txn_webhook_001",
    "source_account": "acc_user_789",
    "destination_account": "acc_merchant_456",
    "amount": 1500,
    "currency": "INR",
}


class TestWebhookReceive:
    # ── Happy path ─────────────────────────────────────────────────────────────

    def test_valid_webhook_returns_202(self, client: TestClient):
        response = client.post("/v1/webhooks/transactions", json=VALID_PAYLOAD)
        assert response.status_code == 202

    def test_valid_webhook_returns_within_500ms(self, client: TestClient):
        start = time.perf_counter()
        response = client.post("/v1/webhooks/transactions", json=VALID_PAYLOAD)
        elapsed_ms = (time.perf_counter() - start) * 1000
        assert response.status_code == 202
        assert elapsed_ms < 500, f"Response took {elapsed_ms:.1f} ms (limit: 500 ms)"

    def test_valid_webhook_response_message(self, client: TestClient):
        response = client.post("/v1/webhooks/transactions", json=VALID_PAYLOAD)
        body = response.json()
        assert body["message"] == "Transaction accepted successfully"
        assert body["transaction_id"] == VALID_PAYLOAD["transaction_id"]

    def test_transaction_stored_in_db(self, client: TestClient, db_session: Session):
        client.post("/v1/webhooks/transactions", json=VALID_PAYLOAD)

        from app.models.transaction import Transaction
        txn = db_session.query(Transaction).filter_by(
            transaction_id=VALID_PAYLOAD["transaction_id"]
        ).first()

        assert txn is not None
        assert str(txn.amount) == "1500.00"
        assert txn.currency == "INR"
        assert txn.source_account == VALID_PAYLOAD["source_account"]
        assert txn.destination_account == VALID_PAYLOAD["destination_account"]

    def test_initial_status_is_processing(self, client: TestClient):
        """Public API must show PROCESSING (internal PENDING → PROCESSING)."""
        client.post("/v1/webhooks/transactions", json=VALID_PAYLOAD)
        response = client.get(f"/v1/transactions/{VALID_PAYLOAD['transaction_id']}")
        assert response.status_code == 200
        item = response.json()[0]
        assert item["status"] == "PROCESSING"

    def test_processed_at_is_null_before_worker(self, client: TestClient):
        client.post("/v1/webhooks/transactions", json=VALID_PAYLOAD)
        response = client.get(f"/v1/transactions/{VALID_PAYLOAD['transaction_id']}")
        item = response.json()[0]
        assert item["processed_at"] is None

    def test_only_one_record_created(self, client: TestClient, db_session: Session):
        client.post("/v1/webhooks/transactions", json=VALID_PAYLOAD)

        from app.models.transaction import Transaction
        count = db_session.query(Transaction).filter_by(
            transaction_id=VALID_PAYLOAD["transaction_id"]
        ).count()
        assert count == 1

    # ── Validation errors ──────────────────────────────────────────────────────

    def test_missing_body_returns_422(self, client: TestClient):
        response = client.post("/v1/webhooks/transactions", json={})
        assert response.status_code == 422

    def test_missing_transaction_id_returns_422(self, client: TestClient):
        payload = {**VALID_PAYLOAD}
        del payload["transaction_id"]
        response = client.post("/v1/webhooks/transactions", json=payload)
        assert response.status_code == 422

    def test_empty_transaction_id_returns_422(self, client: TestClient):
        response = client.post(
            "/v1/webhooks/transactions",
            json={**VALID_PAYLOAD, "transaction_id": ""},
        )
        assert response.status_code == 422

    def test_whitespace_only_transaction_id_returns_422(self, client: TestClient):
        response = client.post(
            "/v1/webhooks/transactions",
            json={**VALID_PAYLOAD, "transaction_id": "   "},
        )
        assert response.status_code == 422

    def test_zero_amount_returns_422(self, client: TestClient):
        response = client.post(
            "/v1/webhooks/transactions",
            json={**VALID_PAYLOAD, "amount": 0},
        )
        assert response.status_code == 422

    def test_negative_amount_returns_422(self, client: TestClient):
        response = client.post(
            "/v1/webhooks/transactions",
            json={**VALID_PAYLOAD, "amount": -100},
        )
        assert response.status_code == 422

    def test_invalid_currency_too_short_returns_422(self, client: TestClient):
        response = client.post(
            "/v1/webhooks/transactions",
            json={**VALID_PAYLOAD, "currency": "IN"},
        )
        assert response.status_code == 422

    def test_invalid_currency_lowercase_returns_422(self, client: TestClient):
        response = client.post(
            "/v1/webhooks/transactions",
            json={**VALID_PAYLOAD, "currency": "inr"},
        )
        assert response.status_code == 422

    def test_invalid_currency_numeric_returns_422(self, client: TestClient):
        response = client.post(
            "/v1/webhooks/transactions",
            json={**VALID_PAYLOAD, "currency": "123"},
        )
        assert response.status_code == 422

    def test_empty_source_account_returns_422(self, client: TestClient):
        response = client.post(
            "/v1/webhooks/transactions",
            json={**VALID_PAYLOAD, "source_account": ""},
        )
        assert response.status_code == 422

    def test_empty_destination_account_returns_422(self, client: TestClient):
        response = client.post(
            "/v1/webhooks/transactions",
            json={**VALID_PAYLOAD, "destination_account": ""},
        )
        assert response.status_code == 422

    # ── DB failure → 503 (not 202) ─────────────────────────────────────────────

    def test_db_failure_returns_503_not_202(self, client: TestClient):
        from unittest.mock import patch
        from sqlalchemy.exc import OperationalError

        with patch(
            "app.services.transaction_service.create_transaction",
            side_effect=OperationalError("stmt", {}, Exception("DB down")),
        ):
            response = client.post("/v1/webhooks/transactions", json=VALID_PAYLOAD)

        assert response.status_code == 503, (
            "Must return 503 when DB fails — never return 202 for an unpersisted transaction"
        )
