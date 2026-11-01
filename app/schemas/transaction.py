"""
app/schemas/transaction.py
──────────────────────────
Pydantic v2 schemas for the Transaction Webhook Processing Service.

Schemas defined here
─────────────────────
  WebhookPayload        – validates inbound POST /v1/webhooks/transactions body
  WebhookResponse       – HTTP 202 acknowledgement body
  TransactionResponse   – single transaction in GET /v1/transactions/{id} array
  HealthResponse        – GET / health-check body

Status mapping (public API vs internal DB)
──────────────────────────────────────────
  Internal  │  Public (returned by API)
  ──────────┼───────────────────────────
  PENDING   │  PROCESSING
  PROCESSING│  PROCESSING
  PROCESSED │  PROCESSED
  FAILED    │  FAILED

The public API hides the PENDING internal state so consumers see a clean
two-outcome model: a transaction is either still PROCESSING or PROCESSED.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")

# Internal → public status mapping
_STATUS_MAP: dict[str, str] = {
    "PENDING": "PROCESSING",
    "PROCESSING": "PROCESSING",
    "PROCESSED": "PROCESSED",
    "FAILED": "FAILED",
}


def _to_public_status(internal: str) -> str:
    """Map internal DB status to the public API status string."""
    return _STATUS_MAP.get(internal, internal)


def _utc_iso(dt: datetime | None) -> str | None:
    """
    Serialise a datetime to UTC ISO 8601 with a 'Z' suffix.

    The application stores all datetimes as naive UTC.  We attach the 'Z'
    suffix here so API consumers always receive an unambiguous UTC string,
    e.g. "2026-11-01T04:10:00.000000Z".
    """
    if dt is None:
        return None
    # If somehow a tz-aware datetime sneaks in, convert to UTC first.
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt.isoformat() + "Z"


# ---------------------------------------------------------------------------
# Inbound schema: POST /v1/webhooks/transactions
# ---------------------------------------------------------------------------


class WebhookPayload(BaseModel):
    """
    Validates the payment-provider webhook body.

    Validation rules (per spec)
    ───────────────────────────
    • transaction_id   – non-empty string (stripped)
    • source_account   – non-empty string (stripped)
    • destination_account – non-empty string (stripped)
    • amount           – Decimal, must be > 0
    • currency         – exactly 3 uppercase ASCII letters (ISO 4217 format)
    """

    model_config = ConfigDict(str_strip_whitespace=True)

    transaction_id: str = Field(
        ...,
        min_length=1,
        description="Unique payment-provider transaction identifier",
        examples=["txn_abc123def456"],
    )
    source_account: str = Field(
        ...,
        min_length=1,
        description="Sender account identifier",
        examples=["acc_user_789"],
    )
    destination_account: str = Field(
        ...,
        min_length=1,
        description="Receiver account identifier",
        examples=["acc_merchant_456"],
    )
    amount: Decimal = Field(
        ...,
        description="Transaction amount; must be greater than zero",
        examples=[1500],
    )
    currency: str = Field(
        ...,
        description="ISO 4217 currency code (3 uppercase letters)",
        examples=["INR"],
    )

    # -- Field-level validators ------------------------------------------------

    @field_validator("amount")
    @classmethod
    def amount_must_be_positive(cls, v: Decimal) -> Decimal:
        if v <= 0:
            raise ValueError("amount must be greater than zero")
        return v

    @field_validator("currency")
    @classmethod
    def currency_must_be_three_letters(cls, v: str) -> str:
        if not _CURRENCY_RE.match(v):
            raise ValueError(
                "currency must be exactly 3 uppercase ASCII letters (e.g. INR, USD)"
            )
        return v

    @field_validator("transaction_id", "source_account", "destination_account")
    @classmethod
    def must_not_be_blank(cls, v: str) -> str:
        # str_strip_whitespace already strips; this guard catches edge cases.
        if not v:
            raise ValueError("field must not be empty or whitespace-only")
        return v


# ---------------------------------------------------------------------------
# Outbound schema: HTTP 202 acknowledgement
# ---------------------------------------------------------------------------


class WebhookResponse(BaseModel):
    """Returned immediately after a webhook is accepted (HTTP 202)."""

    message: str = Field(
        default="Transaction accepted successfully",
        examples=["Transaction accepted successfully"],
    )
    transaction_id: str = Field(
        ...,
        examples=["txn_abc123def456"],
    )


# ---------------------------------------------------------------------------
# Outbound schema: GET /v1/transactions/{transaction_id}
# ---------------------------------------------------------------------------


class TransactionResponse(BaseModel):
    """
    Public representation of a single transaction.

    Serialisation notes
    ───────────────────
    • Internal PENDING status is mapped to public PROCESSING.
    • Timestamps are serialised as UTC ISO 8601 strings with 'Z' suffix.
    • amount is returned as a string to preserve the original decimal
      representation exactly (e.g. "1500.00"), avoiding float precision loss.
    • processed_at is null until the transaction is in PROCESSED status.
    """

    model_config = ConfigDict(from_attributes=True)

    transaction_id: str
    source_account: str
    destination_account: str
    amount: Decimal
    currency: str
    # 'status' on the model is the raw DB value; we expose the public value.
    status: str
    created_at: datetime | None
    processed_at: datetime | None

    # -- Custom serialiser -----------------------------------------------------

    def model_post_init(self, __context: object) -> None:
        """Nothing to do here; serialisation is handled by model_dump_public()."""

    def to_public_dict(self) -> dict:
        """
        Return a JSON-serialisable dict using the public status mapping and
        UTC ISO 8601 timestamp strings.

        Call this method inside the route handler instead of model.model_dump().
        """
        return {
            "transaction_id": self.transaction_id,
            "source_account": self.source_account,
            "destination_account": self.destination_account,
            "amount": str(self.amount),
            "currency": self.currency,
            "status": _to_public_status(self.status),
            "created_at": _utc_iso(self.created_at),
            "processed_at": _utc_iso(self.processed_at),
        }


# ---------------------------------------------------------------------------
# Outbound schema: GET /
# ---------------------------------------------------------------------------


class HealthResponse(BaseModel):
    """
    Health-check response body.

    current_time is always returned as a UTC ISO 8601 string so that clients
    can rely on a consistent format without needing to parse timezone offsets.
    """

    status: Literal["HEALTHY", "UNHEALTHY"]
    current_time: str = Field(
        ...,
        description="Current UTC time in ISO 8601 format",
        examples=["2026-11-01T04:10:00.000000Z"],
    )
