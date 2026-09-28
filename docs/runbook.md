# Recovery and operation

1. Validate metadata and inspect the topological plan/critical path before execution.
2. Allocate a stable run ID for one logical graph/window; pass the same ID for recovery.
   Keep handler code/version fixed during that run.
3. Inspect AuditStore.history() and watermark(pipeline). FAILED preserves the previous
   checkpoint. BLOCKED means a parent did not complete; fix the parent before replay.
4. For BUSY, identify the active worker. Do not remove claims from a live worker.
5. After a crash, stop/fence the old worker and inspect the sink for partially applied
   changes. Reconcile or verify that replay is idempotent.
6. Only then call recover_abandoned(run_id, pipeline, worker_stopped=True).
   This records RECOVERED and releases ownership. Re-run the original graph/window.
7. Successful tasks skip; failed tasks retry. A changed definition or superseded watermark
   is rejected. Use a separately reviewed new window when the old identity cannot replay.

Back up the database with SQLite's backup API while running, or copy it only after all
connections/workers are stopped. Do not place it on network storage. An unavailable audit
store fails execution; it must never be bypassed to keep work flowing.

SQL examples mirror the runtime schema. No authentication, remote dispatch, worker
heartbeats, hard task deadlines or distributed resource scheduler is provided.
