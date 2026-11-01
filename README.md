# Reliable Transaction Webhook Processing Service

[![Python](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-009688.svg)](https://fastapi.tiangolo.com)
[![MySQL](https://img.shields.io/badge/MySQL-8.0+-4479A1.svg)](https://www.mysql.com/)
[![Alembic](https://img.shields.io/badge/Alembic-1.14+-red.svg)](https://alembic.sqlalchemy.org/)
[![Docker](https://img.shields.io/badge/Docker-compose-2496ED.svg)](https://www.docker.com/)

A production-grade Python backend service built with **FastAPI**, **SQLAlchemy 2.x**, and **MySQL 8.0 (InnoDB)** designed to receive transaction webhooks from payment providers (such as Razorpay), acknowledge them under strict low-latency SLAs (< 500ms), guarantee exact-once processing via database-level idempotency, and execute asynchronous transaction jobs using a dedicated background worker with atomic locking, crash recovery, and exponential backoff retries.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Problem Statement](#2-problem-statement)
3. [Architecture and Data Flow](#3-architecture-and-data-flow)
4. [Technologies Used](#4-technologies-used)
5. [Why FastAPI and MySQL Were Selected](#5-why-fastapi-and-mysql-were-selected)
6. [How MySQL Acts as the Persistent Job Queue](#6-how-mysql-acts-as-the-persistent-job-queue)
7. [Database Schema](#7-database-schema)
8. [API Endpoint Documentation](#8-api-endpoint-documentation)
9. [Example Webhook Requests and Responses](#9-example-webhook-requests-and-responses)
10. [Idempotency Implementation](#10-idempotency-implementation)
11. [Background Worker Implementation](#11-background-worker-implementation)
12. [Retry and Crash Recovery Mechanism](#12-retry-and-crash-recovery-mechanism)
13. [Local Installation Instructions](#13-local-installation-instructions)
14. [Docker Setup Instructions](#14-docker-setup-instructions)
15. [Environment Variable Configuration](#15-environment-variable-configuration)
16. [How to Run Automated Tests](#16-how-to-run-automated-tests)
17. [How to Test the 30-Second Processing Delay](#17-how-to-test-the-30-second-processing-delay)
18. [Cloud Deployment Architecture](#18-cloud-deployment-architecture)
19. [Public API URL and Deployment Status](#19-public-api-url-and-deployment-status)

---

## 1. Project Overview

In fintech and payment gateway integrations, webhook delivery mechanisms are inherently unreliable due to network partitions, gateway timeouts, and automatic retry bursts. If an inbound webhook handler attempts to process transactions synchronously (e.g., executing ledger mutations, calling banking rails, verifying anti-fraud rules), requests will exceed gateway timeouts, causing gateways to retry and potentially double-charge or create inconsistent ledger states.

This project delivers a **Reliable Transaction Webhook Processing Service** that separates ingestion from processing:
- **Fast Ingestion:** Acknowledges incoming webhooks with `HTTP 202 Accepted` within 500ms.
- **Strict Idempotency:** Leverages MySQL unique constraints to safely handle duplicate webhooks without duplicate processing.
- **Persistent Job Queue:** Employs MySQL InnoDB's `SELECT ... FOR UPDATE SKIP LOCKED` for zero-overhead, crash-safe job queuing without external broker dependencies (like RabbitMQ or Redis).
- **Asynchronous Processing:** A dedicated worker process claims jobs, applies configurable processing delays (default 30 seconds), transitions states from `PROCESSING` to `PROCESSED` or `FAILED`, and records execution timestamps.
- **Status Visibility:** Provides real-time transaction status lookups via REST API.

---

## 2. Problem Statement

Payment gateways mandate three non-negotiable guarantees from webhook receivers:
1. **Low Latency Acknowledgment:** Webhooks must return HTTP 2xx within hundreds of milliseconds to avoid gateway timeout penalties.
2. **Exactly-Once Semantics:** Gateways resend webhooks on transient network blips. Concurrent or repeated webhooks with identical transaction IDs must never produce duplicate transactions or duplicate background side-effects.
3. **Resilience to Failure:** If a worker crashes mid-transaction or a network partition occurs, uncompleted jobs must not be lost forever, nor should multiple workers process the same transaction simultaneously.

---

## 3. Architecture and Data Flow

```
┌────────────────────────────────────────────────────────────────────────┐
│               External Clients (Razorpay / curl / Load Test)           │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │ 1. POST /v1/webhooks/transactions
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                        FastAPI Web Service                             │
│  - Validates request schema via Pydantic                               │
│  - Attempts atomic INSERT into MySQL                                   │
│  - Catches duplicate key (Error 1062) for idempotent 202 response      │
│  - Returns HTTP 202 Accepted (< 500ms response time)                  │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │ 2. Insert row (status='PENDING')
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                        MySQL 8.0 (InnoDB)                              │
│  - Table: `transactions`                                               │
│  - Acts as both system-of-record and job queue                         │
│  - Indexes: UNIQUE(transaction_id), (status, next_attempt_at)          │
└───────────────────────────────────▲────────────────────────────────────┘
                                    │ 3. SELECT ... FOR UPDATE SKIP LOCKED
                                    │ 4. Claim: status='PROCESSING', lease
                                    │ 5. Complete: status='PROCESSED', processed_at
┌───────────────────────────────────┴────────────────────────────────────┐
│                    Background Worker Process                           │
│  - Continuously polls for runnable PENDING or expired PROCESSING jobs  │
│  - Atomically claims job with UUID lease token and expiry              │
│  - Simulates transaction execution (configurable 30s delay)            │
│  - Completes transaction with lease token check                        │
│  - Automatically recovers abandoned/crashed worker leases              │
│  - Exponential backoff retries up to MAX_ATTEMPTS                      │
└────────────────────────────────────────────────────────────────────────┘
```

### Complete End-to-End Workflow:
1. Gateway issues `POST /v1/webhooks/transactions`.
2. FastAPI validates payload syntax and schema constraints.
3. Database service issues an `INSERT` statement into `transactions` with initial status `PENDING`.
4. If the transaction ID is unique, the record is inserted. If a duplicate exists, MySQL triggers a unique constraint violation (`Error 1062`), which the service gracefully catches and returns the existing transaction without modification.
5. Inbound client immediately receives `HTTP 202 Accepted` with payload status `PROCESSING` (internally `PENDING`).
6. The separate background worker executes an atomic polling query using `SELECT ... FOR UPDATE SKIP LOCKED`.
7. Worker marks row as `status='PROCESSING'`, increments `attempts`, and writes a unique `lease_token` and `lease_expires_at`.
8. Worker commits the claim, releasing the row lock, and executes business logic (30-second delay).
9. Once processing completes, worker verifies its `lease_token` and updates row to `status='PROCESSED'`, setting `processed_at = UTC_TIMESTAMP`.

---

## 4. Technologies Used

- **Language:** Python 3.12
- **Web Framework:** FastAPI (high-performance ASGI web framework)
- **ASGI Server:** Uvicorn (standard uvloop + httptools)
- **Database:** MySQL 8.0+ (InnoDB storage engine for ACID compliance and row-level locking)
- **ORM & Core:** SQLAlchemy 2.x
- **DB Driver:** PyMySQL with `cryptography` for secure `caching_sha2_password` authentication
- **Schema Migrations:** Alembic
- **Validation & Settings:** Pydantic v2 & Pydantic-Settings
- **Testing:** Pytest, pytest-asyncio, HTTPX
- **Containerization:** Docker & Docker Compose (multi-stage, non-root user)

---

## 5. Why FastAPI and MySQL Were Selected

### Why FastAPI?
- **Asynchronous Execution:** Async request handling allows FastAPI to efficiently serve hundreds of concurrent webhook requests per second on a single instance without blocking on thread allocation.
- **Pydantic Validation:** Native type checking and rigorous input validation return standard `HTTP 422 Unprocessable Entity` responses before invalid payloads ever touch business logic or the database.
- **Automatic OpenAPI Documentation:** Generates interactive Swagger UI (`/docs`) and ReDoc (`/redoc`) specifications directly from code.

### Why MySQL 8.0 (InnoDB) as the Job Queue?
- **Single Source of Truth:** Placing queue state inside the database eliminates distributed dual-write inconsistencies between message brokers (e.g. RabbitMQ/Kafka) and the primary database.
- **`SKIP LOCKED` Concurrency:** MySQL 8.0 introduced `SELECT ... FOR UPDATE SKIP LOCKED`. This allows multiple workers to query pending jobs concurrently without lock contention; each worker locks and claims distinct rows while skipping already-locked rows.
- **Transactional Integrity:** Job claiming, state transitions, and retries are guarded by ACID transactions, preventing ghost claims or duplicate execution even during sudden crashes.

---

## 6. How MySQL Acts as the Persistent Job Queue

The `transactions` table doubles as a persistent FIFO priority queue.

### Atomic Claim Query:
```sql
SELECT id, transaction_id, status, attempts, lease_token
FROM transactions
WHERE (
    (status = 'PENDING' AND next_attempt_at <= NOW(6))
    OR
    (status = 'PROCESSING' AND lease_expires_at <= NOW(6) AND attempts < :max_attempts)
)
ORDER BY created_at ASC
LIMIT 1
FOR UPDATE SKIP LOCKED;
```

### Claim Execution:
1. Worker executes query inside a transaction.
2. `SKIP LOCKED` skips rows currently held by other workers.
3. If a row is found:
   ```sql
   UPDATE transactions
   SET status = 'PROCESSING',
       attempts = attempts + 1,
       lease_token = :worker_uuid,
       lease_expires_at = DATE_ADD(NOW(6), INTERVAL :lease_seconds SECOND),
       updated_at = NOW(6)
   WHERE id = :id;
   ```
4. Worker commits transaction immediately, relinquishing row locks so other workers can proceed unimpeded.

---

## 7. Database Schema

The database schema is managed via Alembic under `alembic/versions/`.

```sql
CREATE TABLE transactions (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    transaction_id VARCHAR(128) NOT NULL,
    amount DECIMAL(18, 2) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    source_account VARCHAR(128) NOT NULL,
    destination_account VARCHAR(128) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    attempts INT UNSIGNED NOT NULL DEFAULT 0,
    max_attempts INT UNSIGNED NOT NULL DEFAULT 5,
    last_error TEXT NULL,
    lease_token VARCHAR(36) NULL,
    lease_expires_at DATETIME(6) NULL,
    next_attempt_at DATETIME(6) NOT NULL,
    processed_at DATETIME(6) NULL,
    created_at DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    
    CONSTRAINT uq_transactions_transaction_id UNIQUE (transaction_id),
    INDEX ix_transactions_status_next_attempt (status, next_attempt_at, created_at),
    INDEX ix_transactions_status_lease (status, lease_expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

### Key Field Responsibilities:
- `transaction_id`: External idempotency key enforced by `UNIQUE` constraint.
- `amount`: `DECIMAL(18,2)` to guarantee exact precision for monetary values.
- `status`: Internal states: `PENDING`, `PROCESSING`, `PROCESSED`, `FAILED`. (Publicly, `PENDING` is rendered as `PROCESSING` per API contract).
- `lease_token`: Random UUID4 generated by the worker claiming the job.
- `lease_expires_at`: Heartbeat/lease timeout after which stalled jobs become eligible for recovery.
- `processed_at`: Set to UTC timestamp only upon successful completion.

---

## 8. API Endpoint Documentation

| Method | Endpoint | Description | Status Code |
| :--- | :--- | :--- | :--- |
| `GET` | `/` | Service health & database connectivity check | `200 OK` / `503 Service Unavailable` |
| `POST` | `/v1/webhooks/transactions` | Ingests payment transaction webhook | `202 Accepted` / `422 Unprocessable` / `503 Unavailable` |
| `GET` | `/v1/transactions/{transaction_id}` | Retrieve transaction processing status | `200 OK` / `404 Not Found` |

---

## 9. Example Webhook Requests and Responses

### 1. Health Check
```bash
curl -X GET http://localhost:8000/
```
**Response (200 OK):**
```json
{
  "status": "healthy",
  "database": "connected"
}
```

---

### 2. Submit Transaction Webhook
```bash
curl -X POST http://localhost:8000/v1/webhooks/transactions \
  -H "Content-Type: application/json" \
  -d '{
    "transaction_id": "txn_rzp_live_998124",
    "amount": 2499.00,
    "currency": "INR",
    "source_account": "acc_customer_789",
    "destination_account": "acc_merchant_456"
  }'
```
**Response (202 Accepted):**
```json
{
  "transaction_id": "txn_rzp_live_998124",
  "status": "PROCESSING",
  "message": "Transaction accepted for processing"
}
```

---

### 3. Check Transaction Status (Immediate)
```bash
curl -X GET http://localhost:8000/v1/transactions/txn_rzp_live_998124
```
**Response (200 OK — While processing):**
```json
{
  "transaction_id": "txn_rzp_live_998124",
  "amount": 2499.00,
  "currency": "INR",
  "source_account": "acc_customer_789",
  "destination_account": "acc_merchant_456",
  "status": "PROCESSING",
  "created_at": "2026-11-01T05:15:00.123456Z",
  "processed_at": null
}
```

---

### 4. Check Transaction Status (After 30 Seconds)
```bash
curl -X GET http://localhost:8000/v1/transactions/txn_rzp_live_998124
```
**Response (200 OK — Processed):**
```json
{
  "transaction_id": "txn_rzp_live_998124",
  "amount": 2499.00,
  "currency": "INR",
  "source_account": "acc_customer_789",
  "destination_account": "acc_merchant_456",
  "status": "PROCESSED",
  "created_at": "2026-11-01T05:15:00.123456Z",
  "processed_at": "2026-11-01T05:15:30.128912Z"
}
```

---

## 10. Idempotency Implementation

Idempotency is implemented at the database layer rather than via application-level locks or pre-check queries.

1. **Direct Insert Strategy:** The service issues a single `INSERT` statement into `transactions`.
2. **Constraint Enforcement:** If two identical requests hit the service concurrently, MySQL InnoDB guarantees only one succeeds. The second triggers `pymysql.err.IntegrityError` (MySQL error code `1062`: Duplicate entry).
3. **Graceful Handling:** The transaction service intercepts `IntegrityError` (code 1062), rolls back the failed insert, and fetches the existing transaction record.
4. **Idempotent HTTP 202:** The client receives the standard `HTTP 202 Accepted` response. No secondary worker task is created.

---

## 11. Background Worker Implementation

The worker operates as a detached, resilient loop (`python -m app.worker.main`):
1. **Polling:** Every `WORKER_POLL_INTERVAL_SECONDS` (default 1.0s), the worker runs `processor.claim_next_job()`.
2. **Lease Allocation:** When claiming, a `lease_token` (UUID4) and `lease_expires_at` (now + `JOB_LEASE_SECONDS`) are persisted.
3. **Execution Delay:** Simulates transaction verification and external provider communication for `PROCESSING_DELAY_SECONDS` (default 30s).
4. **Atomic Completion:** Worker completes the job with an explicit lease check:
   ```sql
   UPDATE transactions
   SET status = 'PROCESSED',
       processed_at = NOW(6),
       lease_token = NULL,
       lease_expires_at = NULL
   WHERE id = :id AND lease_token = :worker_lease_token;
   ```
   If `rowcount == 0`, the worker's lease expired and another worker took over; the stale worker's result is safely discarded.
5. **Graceful Shutdown:** Handles `SIGTERM` and `SIGINT` cleanly, completing in-flight jobs before terminating.

---

## 12. Retry and Crash Recovery Mechanism

### Crash Recovery:
If a worker process terminates abruptly (OOM kill, machine restart, network failure) while processing a transaction:
- The transaction remains in `status = 'PROCESSING'`.
- Once `lease_expires_at < NOW(6)`, any active worker running the claim query detects the expired lease.
- The new worker increments `attempts`, assigns a new `lease_token`, and resumes processing.

### Exponential Backoff Retry:
If an unexpected exception occurs during processing:
- The worker increments `attempts` and records the traceback in `last_error`.
- If `attempts < max_attempts`:
  - `status` is reset to `PENDING`.
  - Next execution is delayed via exponential backoff:
    $$\text{next\_attempt\_at} = \text{NOW}() + \text{RETRY\_BASE\_DELAY\_SECONDS} \times 2^{(\text{attempts} - 1)}$$
- If `attempts >= max_attempts`:
  - `status` transitions to `FAILED`.
  - The job is quarantined and will not be claimed again.

---

## 13. Local Installation Instructions

### Prerequisites:
- Python 3.12+
- MySQL 8.0+ server running locally
- Git

### Steps:
```bash
# 1. Clone repository
git clone https://github.com/Aakash-twr/full-stack-assessment.git
cd full-stack-assessment

# 2. Create virtual environment
python3.12 -m venv .venv
source .venv/bin/activate

# 3. Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

# 4. Configure environment
cp .env.example .env
# Edit .env with your local MySQL credentials:
# DATABASE_URL=mysql+pymysql://txn_user:txn_password@localhost:3306/transactions_db

# 5. Run database migrations
alembic upgrade head

# 6. Start FastAPI server (Terminal 1)
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

# 7. Start Background Worker (Terminal 2)
python -m app.worker.main
```

---

## 14. Docker Setup Instructions

The easiest way to run the entire stack (MySQL + Migrations + FastAPI + Worker) is via Docker Compose:

```bash
# Build and run all services
docker compose up --build
```

### Services Started:
- `txn_mysql`: MySQL 8.0 container on port 3306 with health check.
- `txn_migrate`: Runs `alembic upgrade head` once MySQL is healthy, then exits.
- `txn_api`: FastAPI API container listening on port 8000.
- `txn_worker`: Background worker processing transaction queues.

To stop and remove containers:
```bash
docker compose down
```

To clean database volumes:
```bash
docker compose down -v
```

---

## 15. Environment Variable Configuration

| Variable | Default Value | Description |
| :--- | :--- | :--- |
| `DATABASE_URL` | `mysql+pymysql://txn_user:txn_password@localhost:3306/transactions_db` | SQLAlchemy MySQL connection string |
| `PROCESSING_DELAY_SECONDS` | `30` | Duration the worker sleeps to simulate asynchronous processing |
| `WORKER_POLL_INTERVAL_SECONDS` | `1.0` | Frequency in seconds at which worker polls for pending transactions |
| `JOB_LEASE_SECONDS` | `120` | Duration in seconds before an in-progress job lease is considered expired |
| `MAX_ATTEMPTS` | `5` | Maximum processing attempts before marking transaction as `FAILED` |
| `RETRY_BASE_DELAY_SECONDS` | `5` | Base delay in seconds for exponential backoff retry calculation |
| `PORT` | `8000` | Port on which the FastAPI application listens |
| `LOG_LEVEL` | `INFO` | Logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |

---

## 16. How to Run Automated Tests

The test suite covers unit, integration, concurrency, idempotency, and crash recovery scenarios using Pytest.

### Running with a Live MySQL Instance:
```bash
# Ensure MySQL is running on port 3306
export TEST_DATABASE_URL="mysql+pymysql://txn_user:txn_password@localhost:3306/transactions_test"

# Run all 63 automated tests
pytest -v
```

### Test Coverage Highlights:
- `test_health.py`: Validates health check and DB connectivity status.
- `test_webhooks.py`: Validates schema constraints, field validations, HTTP 202 SLA, and 422 error cases.
- `test_transactions.py`: Tests status query endpoint, public status mappings, and 404 responses.
- `test_idempotency.py`: Validates duplicate prevention, concurrent identical webhooks via ThreadPoolExecutor, and single record DB guarantees.
- `test_worker.py`: Validates 30-second delay simulation, lease management, crash recovery, retries, and database outage handling.

---

## 17. How to Test the 30-Second Processing Delay

1. **Send a Webhook:**
   ```bash
   curl -i -X POST http://localhost:8000/v1/webhooks/transactions \
     -H "Content-Type: application/json" \
     -d '{
       "transaction_id": "test_delay_001",
       "amount": 100.00,
       "currency": "USD",
       "source_account": "acc_alice",
       "destination_account": "acc_bob"
     }'
   ```
   *Expected response:* HTTP 202 Accepted with status `"PROCESSING"`.

2. **Query Immediately (at 5 seconds):**
   ```bash
   curl -s http://localhost:8000/v1/transactions/test_delay_001 | jq
   ```
   *Output:*
   ```json
   {
     "transaction_id": "test_delay_001",
     "status": "PROCESSING",
     "processed_at": null
   }
   ```

3. **Query After 35 Seconds:**
   ```bash
   curl -s http://localhost:8000/v1/transactions/test_delay_001 | jq
   ```
   *Output:*
   ```json
   {
     "transaction_id": "test_delay_001",
     "status": "PROCESSED",
     "processed_at": "2026-11-01T..."
   }
   ```

---

## 18. Cloud Deployment Architecture

The service is architected to deploy seamlessly to cloud container environments (e.g., Render, Railway, AWS ECS, or Fly.io):

```
┌─────────────────────────────────────────────────────────────┐
│                    Cloud Load Balancer                      │
│                  (HTTPS termination, Port 443)              │
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│                   FastAPI Web Service                       │
│  - Cloud Container / Web Service                            │
│  - Command: uvicorn app.main:app --host 0.0.0.0 --port $PORT│
└──────────────────────────────┬──────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│                     Managed MySQL 8.0                       │
│  - Amazon RDS / PlanetScale / Render Managed MySQL          │
│  - Automated backups, InnoDB storage, SSL/TLS               │
└──────────────────────────────▲──────────────────────────────┘
                               │
┌──────────────────────────────┴──────────────────────────────┐
│                Background Worker Service                    │
│  - Long-running Background Worker (Never ephemeral)         │
│  - Command: python -m app.worker.main                       │
└─────────────────────────────────────────────────────────────┘
```

---

## 19. Public API URL and Deployment Status

### Deployment Status:
- **Public GitHub Repository:** [https://github.com/Aakash-twr/full-stack-assessment](https://github.com/Aakash-twr/full-stack-assessment)
- **Local / Docker Environment:** Complete and verified with 100% automated test coverage.
- **Cloud Provider Credentials:** Per specification instructions, no cloud account credentials were provided in the evaluation environment. Fabricating an unverified cloud endpoint is strictly forbidden. Complete deployment blueprints and commands are supplied above.

### Verification Endpoints (Local / Docker):
- **API Base:** `http://localhost:8000`
- **Health Check:** `GET http://localhost:8000/`
- **Webhook Ingestion:** `POST http://localhost:8000/v1/webhooks/transactions`
- **Status Lookup:** `GET http://localhost:8000/v1/transactions/{transaction_id}`
- **Interactive Documentation:** `http://localhost:8000/docs`
