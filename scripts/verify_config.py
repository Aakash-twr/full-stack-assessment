#!/usr/bin/env python3
"""
scripts/verify_config.py
─────────────────────────
Quick smoke-test: import Settings and database module, print resolved values.
Run with:  .venv/bin/python scripts/verify_config.py
"""
import sys, os

# Allow running from repo root without installing the package
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from app.core.config import get_settings

s = get_settings()
print("=== Settings ===")
print(f"  database_url              : {s.database_url}")
print(f"  processing_delay_seconds  : {s.processing_delay_seconds}")
print(f"  worker_poll_interval_secs : {s.worker_poll_interval_seconds}")
print(f"  job_lease_seconds         : {s.job_lease_seconds}")
print(f"  max_attempts              : {s.max_attempts}")
print(f"  retry_base_delay_seconds  : {s.retry_base_delay_seconds}")
print(f"  port                      : {s.port}")
print(f"  log_level                 : {s.log_level}")
print()

# Import database module (engine creation is deferred to first use with pool_pre_ping)
from app.core.database import Base, SessionLocal, engine, check_database_connection
print("=== Database module imported successfully ===")
print(f"  Engine dialect : {engine.dialect.name}")
print(f"  Engine driver  : {engine.dialect.driver}")
print()

# Connectivity check (will fail gracefully if MySQL isn't running locally)
print("=== Database connectivity ===")
reachable = check_database_connection()
if reachable:
    print("  ✅ Database is reachable")
else:
    print("  ⚠️  Database is NOT reachable (expected if MySQL isn't running locally)")

print()
print("Phase 1 smoke-test complete.")
