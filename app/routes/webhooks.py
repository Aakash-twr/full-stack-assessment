"""
app/routes/webhooks.py
───────────────────────
POST /v1/webhooks/transactions — receive and acknowledge payment webhooks.

Performance contract
─────────────────────
This handler MUST return within 500 ms under normal conditions.
It does NOT perform any slow operation (no time.sleep, no external HTTP call).
The 30-second processing delay lives entirely in the background worker process.

Flow
────
1. FastAPI validates the request body against WebhookPayload.
2. service.create_transaction() attempts an INSERT.
3. On success (new or idempotent duplicate) → HTTP 202 Accepted.
4. On unexpected persistence failure → HTTP 503 Service Unavailable.

Idempotency
───────────
Duplicate transaction_ids return HTTP 202 (not 409).  The payment provider
should be able to retry safely without special-casing the response code.
The database-level UNIQUE constraint guarantees exactly-once storage.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from sqlalchemy.exc import IntegrityError, OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.schemas.transaction import WebhookPayload, WebhookResponse
from app.services import transaction_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/webhooks", tags=["Webhooks"])


@router.post(
    "/transactions",
    status_code=202,
    response_model=WebhookResponse,
    summary="Receive transaction webhook",
    description=(
        "Accepts an inbound payment-provider webhook, persists the transaction "
        "with status PENDING, and returns HTTP 202 immediately. "
        "Duplicate transaction_ids are acknowledged without creating a second record."
    ),
)
def receive_webhook(
    payload: WebhookPayload,
    db: Session = Depends(get_db),
) -> JSONResponse:
    """
    Validate → persist → acknowledge.  Never blocks on processing.

    HTTP responses
    ──────────────
    202  New or duplicate webhook accepted successfully.
    422  Request body failed Pydantic validation (FastAPI default).
    503  Unexpected database error; transaction was NOT persisted.
    """
    try:
        txn, is_new = transaction_service.create_transaction(db, payload)
    except (IntegrityError, OperationalError, SQLAlchemyError) as exc:
        # Any persistence failure that is NOT a duplicate key error.
        # We must NOT return 202 here — the transaction was not saved.
        logger.error(
            "Failed to persist webhook for transaction_id=%r: %s",
            payload.transaction_id,
            exc,
            exc_info=True,
        )
        raise HTTPException(
            status_code=503,
            detail="Database error: transaction could not be persisted. Please retry.",
        ) from exc
    except Exception as exc:
        logger.error(
            "Unexpected error processing webhook for transaction_id=%r: %s",
            payload.transaction_id,
            exc,
            exc_info=True,
        )
        raise HTTPException(
            status_code=503,
            detail="Unexpected error; transaction could not be accepted.",
        ) from exc

    if is_new:
        logger.info(
            "Webhook accepted: transaction_id=%r id=%s",
            txn.transaction_id,
            txn.id,
        )
    else:
        logger.info(
            "Duplicate webhook received (idempotent): transaction_id=%r id=%s status=%s",
            txn.transaction_id,
            txn.id,
            txn.status,
        )

    return JSONResponse(
        status_code=202,
        content={
            "message": "Transaction accepted successfully",
            "transaction_id": txn.transaction_id,
        },
    )
