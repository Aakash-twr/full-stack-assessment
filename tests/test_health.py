"""
tests/test_health.py
─────────────────────
Test 1: Health Check

Verifies:
• GET / returns HTTP 200 when healthy.
• status field is "HEALTHY".
• current_time is present and is a valid UTC ISO 8601 string.
• GET / returns HTTP 503 when DB is unavailable.
"""

from __future__ import annotations

import re
from datetime import datetime

import pytest
from fastapi.testclient import TestClient

UTC_ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.*Z$")


class TestHealthCheck:
    def test_health_returns_200(self, client: TestClient):
        response = client.get("/")
        assert response.status_code == 200

    def test_health_status_is_healthy(self, client: TestClient):
        response = client.get("/")
        assert response.json()["status"] == "HEALTHY"

    def test_health_current_time_present(self, client: TestClient):
        response = client.get("/")
        assert "current_time" in response.json()

    def test_health_current_time_is_utc_iso8601(self, client: TestClient):
        response = client.get("/")
        current_time = response.json()["current_time"]
        assert UTC_ISO_RE.match(current_time), (
            f"current_time {current_time!r} is not a valid UTC ISO 8601 string"
        )

    def test_health_current_time_is_recent(self, client: TestClient):
        """The returned time should be within a few seconds of now."""
        from datetime import timezone
        response = client.get("/")
        current_time_str = response.json()["current_time"].rstrip("Z")
        parsed = datetime.fromisoformat(current_time_str).replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        diff = abs((now - parsed).total_seconds())
        assert diff < 10, f"current_time is {diff:.1f}s away from now"

    def test_health_unhealthy_when_db_down(self, client: TestClient):
        """Simulate DB failure — API must return 503."""
        from unittest.mock import patch
        with patch("app.core.database.check_database_connection", return_value=False):
            response = client.get("/")
        assert response.status_code == 503
        assert response.json()["status"] == "UNHEALTHY"
