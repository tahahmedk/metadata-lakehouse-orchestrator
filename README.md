# Metadata Lakehouse Orchestrator

A local orchestration control plane that turns workload metadata into a validated,
restartable DAG execution. It focuses on **who may execute a pipeline, which upstream
results it depends on, and when an incremental checkpoint may advance**.

A loop over ETL jobs cannot resolve concurrent ownership, partial failure, replay and
incremental progress. Here those decisions are explicit state transitions backed by
SQLite. The data plane is an injected handler, so the demo needs no cloud account.

## Architecture

```mermaid
flowchart TD
    Metadata[Validated task metadata] --> DAG[Deterministic DAG planner]
    DAG --> Scheduler[Bounded dependency scheduler]
    Scheduler --> Claim[Atomic pipeline claim]
    State[(SQLite execution state)] --> Claim
    Claim --> Handler[Injected data-plane handler]
    Handler --> Success[Atomic success and watermark commit]
    Success --> State
    Handler --> Failure[Classified retry or terminal failure]
    Failure --> State
    Failure --> Block[Block dependent tasks]
```

- Graph validation rejects missing dependencies, cycles, duplicate names/dependencies
  and invalid duration estimates. Topological order is stable; critical path is a
  dependency lower bound, not a prediction under resource contention.
- Up to `max_workers` independent tasks run concurrently. Children start only after
  all parents succeed or are skipped as already successful. Independent branches continue.
- A transaction claims each pipeline across processes sharing one local database.
  Another run receives BUSY rather than executing the same pipeline concurrently.
- A logical identity is `(run_id, pipeline)`; a definition fingerprint prevents reuse
  with a different configuration or watermark window.
- Each handler receives a fixed integer range `(watermark_before, watermark_after]`.
  The cursor represents a synthetic monotonic source offset, not an event-time clock.
- Success and watermark advancement commit in the same SQLite transaction. Failed
  attempts preserve the old watermark. Durable audit events capture attempts and skips.
- Only `RetryableError` triggers bounded exponential retries. Permanent errors fail
  immediately. Metadata classifies workloads for a future backend; classification
  currently does not reserve CPU, prioritize queues or choose cloud compute.

## Run and test

Python 3.11–3.13:

```bash
python -m venv .venv
# Activate: Windows .venv\Scripts\Activate.ps1; POSIX source .venv/bin/activate
python -m pip install -r requirements-dev.lock.txt
python -m pip install -e . --no-deps
python -m pytest -q
python demo.py
python demo.py
```

The first demo creates ignored `out/control.db`, runs the four synthetic pipelines,
and persists watermark 100. The second skips all successful tasks. The graph's critical
path is 17 minutes of configured estimates; the handlers themselves perform no ETL.
Use a new run ID and greater upper watermark for a new incremental window.

Tests cover retries, restarts, durable watermarks, simultaneous claims, parallel branches,
blocked descendants, explicit recovery and incompatible replay. CI also checks lint,
formatting and the demo across three Python versions.

## Failure semantics

| Failure | Result |
| --- | --- |
| Invalid graph/configuration | No task starts |
| Transient handler error | Retry same window; bounded backoff |
| Permanent error or exhausted retries | FAILED, checkpoint unchanged, descendants BLOCKED |
| Concurrent pipeline owner | BUSY; descendants BLOCKED |
| Crash with active claim | Claim remains; operator recovery required |
| State commit fails after side effect | Claim retained; reconcile sink before recovery |
| Reuse run ID with changed metadata | Rejected |

**Exactly-once side effects are not guaranteed.** A worker can finish a sink write and
crash before recording success. Handlers must merge/upsert or deduplicate using a stable
business key or `(run_id, pipeline)`. Retrying alone does not make a sink idempotent.

A successful same-run replay is skipped. If a failed window has been superseded by
another run's watermark, it cannot be replayed under the old identity. Do not reuse a
run ID for a new graph or different handler implementation: the fingerprint covers
metadata, not Python handler code.

## Operations, trade-offs and evolution

Audit events expose pipeline, run ID, attempt and status without storing exception
messages or source records. JSON success events are emitted through Python logging.
See [runbook](docs/runbook.md), [state decision](docs/ADR-001-idempotency.md) and
[control schema](sql/control_tables.sql).

SQLite provides inspectable local transactions and serialized state writes; it is not
a distributed scheduler database and should not live on a network filesystem. The
scheduler is one process with threads; it does not interrupt hung handlers or provide
leases/heartbeats. Claims require explicit recovery after the old worker is stopped.
A production backend would add fenced leases, handler cancellation, durable dispatch,
shared transactional storage and backend-specific idempotent sinks.

All examples are independent clean-room implementations using synthetic offsets,
workloads and generic architecture patterns. No employer source or confidential data.
