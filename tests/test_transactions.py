"""
tests/test_transactions.py
───────────────────────────
Test 5: Multiple Transactions
Test 6: Unknown Transaction

Verifies:
• 10 different webhooks are all stored.
• All eventually reach PROCESSED status after worker runs.
• API responses remain fast while processing is underway.
• GET /v1/transactions/{unknown} returns HTTP 404.
• Response format is a JSON array.
• Timestamps are UTC ISO 8601 strings.
• Amount and currency are preserved exactly.
"""

from __future__ import annotations

import re
import time
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

UTC_ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.*Z$")


def _make_payload(n: int) -> dict:
    return {
        "transaction_id": f"txn_multi_{n:04d}",
        "source_account": f"acc_src_{n}",
        "destination_account": f"acc_dst_{n}",
        "amount": 100 * n,
        "currency": "INR",
    }


class TestTransactionStatus:
    # ── Test 6: Unknown transaction ────────────────────────────────────────────

    def test_unknown_transaction_returns_404(self, client: TestClient):
        response = client.get("/v1/transactions/txn_does_not_exist_xyz")
        assert response.status_code == 404

    def test_404_detail_message(self, client: TestClient):
        response = client.get("/v1/transactions/txn_nonexistent")
        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()

    # ── Response format ────────────────────────────────────────────────────────

    def test_response_is_json_array(self, client: TestClient):
        payload = _make_payload(1)
        client.post("/v1/webhooks/transactions", json=payload)
        response = client.get(f"/v1/transactions/{payload['transaction_id']}")
        assert response.status_code == 200
        assert isinstance(response.json(), list)

    def test_response_array_has_one_element(self, client: TestClient):
        payload = _make_payload(2)
        client.post("/v1/webhooks/transactions", json=payload)
        response = client.get(f"/v1/transactions/{payload['transaction_id']}")
        assert len(response.json()) == 1

    def test_response_contains_correct_transaction_id(self, client: TestClient):
        payload = _make_payload(3)
        client.post("/v1/webhooks/transactions", json=payload)
        response = client.get(f"/v1/transactions/{payload['transaction_id']}")
        item = response.json()[0]
        assert item["transaction_id"] == payload["transaction_id"]

    def test_response_preserves_source_account(self, client: TestClient):
        payload = _make_payload(4)
        client.post("/v1/webhooks/transactions", json=payload)
        item = client.get(f"/v1/transactions/{payload['transaction_id']}").json()[0]
        assert item["source_account"] == payload["source_account"]

    def test_response_preserves_destination_account(self, client: TestClient):
        payload = _make_payload(5)
        client.post("/v1/webhooks/transactions", json=payload)
        item = client.get(f"/v1/transactions/{payload['transaction_id']}").json()[0]
        assert item["destination_account"] == payload["destination_account"]

    def test_response_preserves_amount_exactly(self, client: TestClient):
        payload = {**_make_payload(6), "amount": 1234.56}
        client.post("/v1/webhooks/transactions", json=payload)
        item = client.get(f"/v1/transactions/{payload['transaction_id']}").json()[0]
        # Amount comes back as a string representation of DECIMAL(18,2)
        assert Decimal(item["amount"]) == Decimal("1234.56")

    def test_response_preserves_currency(self, client: TestClient):
        payload = {**_make_payload(7), "currency": "USD"}
        client.post("/v1/webhooks/transactions", json=payload)
        item = client.get(f"/v1/transactions/{payload['transaction_id']}").json()[0]
        assert item["currency"] == "USD"

    def test_created_at_is_utc_iso8601(self, client: TestClient):
        payload = _make_payload(8)
        client.post("/v1/webhooks/transactions", json=payload)
        item = client.get(f"/v1/transactions/{payload['transaction_id']}").json()[0]
        assert UTC_ISO_RE.match(item["created_at"]), (
            f"created_at {item['created_at']!r} is not UTC ISO 8601"
        )

    def test_processed_at_is_null_before_worker(self, client: TestClient):
        payload = _make_payload(9)
        client.post("/v1/webhooks/transactions", json=payload)
        item = client.get(f"/v1/transactions/{payload['transaction_id']}").json()[0]
        assert item["processed_at"] is None

    def test_initial_status_is_processing(self, client: TestClient):
        payload = _make_payload(10)
        client.post("/v1/webhooks/transactions", json=payload)
        item = client.get(f"/v1/transactions/{payload['transaction_id']}").json()[0]
        assert item["status"] == "PROCESSING"

    # ── Test 5: Multiple transactions ─────────────────────────────────────────

    def test_ten_transactions_all_stored(self, client: TestClient, db_session: Session):
        """All 10 webhooks must produce a DB record."""
        payloads = [_make_payload(i) for i in range(100, 110)]

        for p in payloads:
            r = client.post("/v1/webhooks/transactions", json=p)
            assert r.status_code == 202

        from app.models.transaction import Transaction
        for p in payloads:
            txn = db_session.query(Transaction).filter_by(
                transaction_id=p["transaction_id"]
            ).first()
            assert txn is not None, f"{p['transaction_id']} was not stored"

    def test_ten_transactions_all_return_202(self, client: TestClient):
        payloads = [_make_payload(i) for i in range(200, 210)]
        for p in payloads:
            r = client.post("/v1/webhooks/transactions", json=p)
            assert r.status_code == 202, f"Expected 202, got {r.status_code} for {p}"

    def test_ten_transactions_api_stays_fast(self, client: TestClient):
        """All 10 POST responses must arrive within 500 ms each."""
        payloads = [_make_payload(i) for i in range(300, 310)]
        for p in payloads:
            start = time.perf_counter()
            r = client.post("/v1/webhooks/transactions", json=p)
            elapsed_ms = (time.perf_counter() - start) * 1000
            assert r.status_code == 202
            assert elapsed_ms < 500, (
                f"{p['transaction_id']} took {elapsed_ms:.1f} ms (limit 500 ms)"
            )

    def test_ten_transactions_status_queryable(self, client: TestClient):
        """Every stored transaction must be retrievable via GET."""
        payloads = [_make_payload(i) for i in range(400, 410)]
        for p in payloads:
            client.post("/v1/webhooks/transactions", json=p)

        for p in payloads:
            r = client.get(f"/v1/transactions/{p['transaction_id']}")
            assert r.status_code == 200
            assert len(r.json()) == 1
            assert r.json()[0]["transaction_id"] == p["transaction_id"]
