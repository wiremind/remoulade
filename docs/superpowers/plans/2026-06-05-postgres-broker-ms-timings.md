# Postgres Broker Millisecond Timings Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every Postgres broker timing input accept milliseconds, while keeping PGMQ and thread APIs working with their required units internally.

**Architecture:** Rename the broker-facing timeout parameters to `*_ms`, keep public consumer timing inputs in milliseconds, and convert to seconds only at the boundary where PGMQ or `threading.Event.wait()` require it. Update the Postgres tests to assert the new API and the conversion boundary explicitly.

**Tech Stack:** Python, pytest, psycopg, pgmq, SQLAlchemy

---

### Task 1: Update timing API names

**Files:**
- Modify: `remoulade/brokers/postgres.py`
- Modify: `tests/test_postgres.py`

- [ ] **Step 1: Write the failing test**

```python
broker = PostgresBroker(
    url=TEST_POSTGRES_URL,
    middleware=[],
    visibility_timeout_ms=17_000,
    heartbeat_interval_ms=500,
)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_postgres.py -k postgres_consumer_uses_broker_visibility_timeout_for_reads -v`
Expected: `TypeError` because the broker still expects `*_in_second` arguments.

- [ ] **Step 3: Write minimal implementation**

```python
def __init__(
    self,
    *,
    url: str,
    middleware: list["Middleware"] | None = None,
    group_transaction: bool = False,
    archive_partition_interval: str = "1 day",
    archive_retention_interval: str = "7 days",
    visibility_timeout_ms: int = 30_000,
    heartbeat_interval_ms: int = 10_000,
) -> None:
    if visibility_timeout_ms <= 0:
        raise ValueError("visibility_timeout_ms must be greater than 0")
    if heartbeat_interval_ms <= 0 or heartbeat_interval_ms >= visibility_timeout_ms:
        raise ValueError("heartbeat_interval_ms must be greater than 0 and lower than visibility_timeout_ms")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_postgres.py -k postgres_consumer_uses_broker_visibility_timeout_for_reads -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add remoulade/brokers/postgres.py tests/test_postgres.py docs/superpowers/plans/2026-06-05-postgres-broker-ms-timings.md
git commit -m "feat: use milliseconds for postgres broker timing inputs"
```

### Task 2: Convert broker internals to seconds

**Files:**
- Modify: `remoulade/brokers/postgres.py`
- Modify: `tests/test_postgres.py`

- [ ] **Step 1: Write the failing test**

```python
assert broker.client.read.call_args.kwargs["vt"] == 17
assert broker.client.set_vt.call_args.args[2] == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_postgres.py -k 'postgres_consumer_uses_broker_visibility_timeout_for_reads or postgres_consumer_heartbeat_extends_inflight_message_visibility' -v`
Expected: `vt` still reflects millisecond inputs instead of converted seconds.

- [ ] **Step 3: Write minimal implementation**

```python
from math import ceil

self.visibility_timeout = max(1, ceil(self.visibility_timeout_ms / 1000))
self.heartbeat_interval = self.heartbeat_interval_ms / 1000
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_postgres.py -k 'postgres_consumer_uses_broker_visibility_timeout_for_reads or postgres_consumer_heartbeat_extends_inflight_message_visibility' -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add remoulade/brokers/postgres.py tests/test_postgres.py
git commit -m "fix: convert postgres broker timing inputs"
```

### Task 3: Verify the full Postgres test file

**Files:**
- Test: `tests/test_postgres.py`

- [ ] **Step 1: Run the whole file**

Run: `pytest tests/test_postgres.py -v`
Expected: PASS

- [ ] **Step 2: Fix any remaining unit mismatches**

Adjust only the Postgres timing assertions or docstrings if a legacy `*_in_second` name remains.

- [ ] **Step 3: Commit**

```bash
git add tests/test_postgres.py
git commit -m "test: cover postgres broker millisecond timings"
```
