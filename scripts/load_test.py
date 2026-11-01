#!/usr/bin/env python3
"""
scripts/load_test.py
────────────────────
High-concurrency load testing script for the Transaction Webhook Processing Service.

Features:
• Sends N concurrent webhook POST requests to /v1/webhooks/transactions.
• Tests idempotency with duplicate transaction IDs.
• Measures min, mean, p50, p95, p99, and max response latencies.
• Asserts that 100% of responses return HTTP 202 and meet the < 500ms SLA.

Usage:
    python scripts/load_test.py [--url http://localhost:8000] [--concurrency 100]
"""

from __future__ import annotations

import argparse
import asyncio
import statistics
import time
import uuid
from typing import Any

import httpx


async def send_webhook(
    client: httpx.AsyncClient,
    base_url: str,
    txn_id: str,
    amount: float = 1500.50,
) -> dict[str, Any]:
    """Send a single webhook payload and return latency + status code."""
    payload = {
        "transaction_id": txn_id,
        "amount": amount,
        "currency": "INR",
        "source_account": "acc_user_load_001",
        "destination_account": "acc_merchant_load_001",
    }
    url = f"{base_url.rstrip('/')}/v1/webhooks/transactions"

    t0 = time.perf_counter()
    try:
        response = await client.post(url, json=payload, timeout=10.0)
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        return {
            "transaction_id": txn_id,
            "status_code": response.status_code,
            "latency_ms": elapsed_ms,
            "error": None,
            "response": response.json() if response.status_code in (200, 202) else response.text,
        }
    except Exception as exc:
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        return {
            "transaction_id": txn_id,
            "status_code": 0,
            "latency_ms": elapsed_ms,
            "error": str(exc),
            "response": None,
        }


async def run_load_test(base_url: str, total_requests: int, test_idempotency: bool = True) -> int:
    print("=" * 70)
    print(" Transaction Webhook Service — Concurrent Load Test")
    print("=" * 70)
    print(f"Target URL:        {base_url}/v1/webhooks/transactions")
    print(f"Total Requests:    {total_requests}")
    print(f"Test Idempotency:  {test_idempotency}")
    print("-" * 70)

    # 1. Health check first
    async with httpx.AsyncClient() as client:
        try:
            health_resp = await client.get(f"{base_url.rstrip('/')}/", timeout=5.0)
            if health_resp.status_code != 200 or health_resp.json().get("status") != "HEALTHY":
                print(f"❌ Target service not healthy: {health_resp.status_code} - {health_resp.text}")
                return 1
            print("✔ Service and MySQL connection confirmed healthy.")
        except Exception as exc:
            print(f"❌ Cannot connect to service at {base_url}: {exc}")
            return 1

    # 2. Generate transaction IDs (some duplicates if test_idempotency is True)
    if test_idempotency:
        unique_count = int(total_requests * 0.8)
        unique_ids = [f"txn_load_{uuid.uuid4().hex[:12]}" for _ in range(unique_count)]
        # Add duplicates from the existing pool
        dup_ids = [unique_ids[i % unique_count] for i in range(total_requests - unique_count)]
        txn_ids = unique_ids + dup_ids
    else:
        txn_ids = [f"txn_load_{uuid.uuid4().hex[:12]}" for _ in range(total_requests)]

    print(f"Dispatching {len(txn_ids)} concurrent webhook requests...")
    start_time = time.perf_counter()

    limits = httpx.Limits(max_connections=150, max_keepalive_connections=50)
    async with httpx.AsyncClient(limits=limits) as client:
        tasks = [send_webhook(client, base_url, txn_id) for txn_id in txn_ids]
        results = await asyncio.gather(*tasks)

    total_duration = time.perf_counter() - start_time
    print(f"All requests completed in {total_duration:.3f} seconds.")
    print("-" * 70)

    # 3. Analyze results
    latencies = [r["latency_ms"] for r in results]
    status_codes = [r["status_code"] for r in results]
    successes = [r for r in results if r["status_code"] == 202]
    errors = [r for r in results if r["status_code"] != 202]

    latencies.sort()
    min_lat = min(latencies)
    max_lat = max(latencies)
    mean_lat = statistics.mean(latencies)
    p50_lat = statistics.median(latencies)
    p95_lat = latencies[int(len(latencies) * 0.95)]
    p99_lat = latencies[int(len(latencies) * 0.99)]

    sla_passed = all(l < 500.0 for l in latencies)
    throughput = total_requests / total_duration

    print("📊 RESULTS SUMMARY:")
    print(f"  • Total Requests Sent:     {total_requests}")
    print(f"  • HTTP 202 Accepted:       {len(successes)} ({len(successes)/total_requests*100:.1f}%)")
    print(f"  • Errors/Failures:         {len(errors)}")
    print(f"  • Throughput:              {throughput:.2f} req/sec")
    print(f"  • Min Latency:             {min_lat:.2f} ms")
    print(f"  • Mean Latency:            {mean_lat:.2f} ms")
    print(f"  • p50 Median Latency:      {p50_lat:.2f} ms")
    print(f"  • p95 Latency:             {p95_lat:.2f} ms")
    print(f"  • p99 Latency:             {p99_lat:.2f} ms")
    print(f"  • Max Latency:             {max_lat:.2f} ms")
    print(f"  • Under 500ms SLA:         {'✔ PASSED' if sla_passed else '❌ FAILED'}")

    if errors:
        print("\n⚠️ First 5 Errors encountered:")
        for err in errors[:5]:
            print(f"   - {err['transaction_id']}: status={err['status_code']}, err={err['error']}")

    print("=" * 70)
    return 0 if (len(errors) == 0 and sla_passed) else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transaction Webhook Service Load Tester")
    parser.add_argument("--url", default="http://localhost:8000", help="Base URL of the service")
    parser.add_argument("--concurrency", "-c", type=int, default=100, help="Number of concurrent requests")
    parser.add_argument("--no-idempotency", action="store_true", help="Do not inject duplicate transaction IDs")
    args = parser.parse_args()

    exit_code = asyncio.run(
        run_load_test(
            base_url=args.url,
            total_requests=args.concurrency,
            test_idempotency=not args.no_idempotency,
        )
    )
    raise SystemExit(exit_code)
