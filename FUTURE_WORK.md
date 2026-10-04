# Future work

These are design and implementation candidates. The current executor remains local.

## 1. A Postgres state backend with the same invariants

Extract a small state-store interface and implement atomic claim and completion
transactions in Postgres. Keep the existing SQLite backend useful for local testing.

Done when the same claim/replay/rollback suite passes against both stores, including
simultaneous claims from separate processes. This alone does not provide distributed
execution or exactly-once sink effects.

## 2. Fenced leases and heartbeats

Design ownership expiry together with fencing tokens enforced by a test sink. Specify
what happens when a paused worker resumes after a replacement has acquired its lease.

Done when deterministic tests prove that the stale worker cannot apply effects or
commit progress after losing ownership. Do not add automatic claim expiry without that
protection.

## 3. Durable dispatch to workers

Separate planning from execution through a persisted dispatch/outcome protocol. Start
with a synthetic worker backend and stable execution identity rather than a cloud SDK.

Done when duplicate deliveries, worker crashes and late results preserve dependency
ordering and checkpoint invariants. Build on the fenced-ownership work before attempting
an Azure or Databricks adapter.
