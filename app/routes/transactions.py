"""
app/routes/transactions.py
───────────────────────────
GET /v1/transactions/{transaction_id} — retrieve transaction status.

Response format
───────────────
The spec requires an array response, e.g.:
    [
        {
            "transaction_id": "txn_abc123",
            "status": "PROCESSING",
            ...
        }
    ]

Internal PENDING status is mapped to public PROCESSING before serialisation.
All timestamps are UTC ISO 8601 strings with a 'Z' suffix.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.schemas.transaction import TransactionResponse
from app.services import transaction_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/transactions", tags=["Transactions"])


@router.get(
    "/{transaction_id}",
    summary="Get transaction status",
    description=(
        "Returns the current status of a transaction identified by its "
        "payment-provider transaction_id. "
        "Returns a JSON array containing a single transaction object. "
        "Returns HTTP 404 if the transaction does not exist."
    ),
)
def get_transaction_status(
    transaction_id: str,
    db: Session = Depends(get_db),
) -> JSONResponse:
    """
    Look up a single transaction and return it inside a JSON array.

    HTTP responses
    ──────────────
    200  Transaction found; body is a one-element JSON array.
    404  No transaction with this transaction_id exists.
    """
    txn = transaction_service.get_transaction(db, transaction_id)

    if txn is None:
        logger.info("Transaction not found: transaction_id=%r", transaction_id)
        raise HTTPException(
            status_code=404,
            detail=f"Transaction '{transaction_id}' not found.",
        )

    logger.debug(
        "Transaction status retrieved: transaction_id=%r status=%s",
        txn.transaction_id,
        txn.status,
    )

    # Build the public response using the schema's serialiser (status mapping
    # + UTC ISO 8601 timestamps).
    response_schema = TransactionResponse.model_validate(txn)
    public_dict = response_schema.to_public_dict()

    # Spec requires an array-wrapped response.
    return JSONResponse(status_code=200, content=[public_dict])
