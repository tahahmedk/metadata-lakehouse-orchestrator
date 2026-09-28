# ADR-001: Durable ownership and atomic checkpoints
Status: accepted for a local coordinator.

Separate the control-plane transaction from the sink side effect. Claim a pipeline under
BEGIN IMMEDIATE and persist the logical run and fixed offset range. Handler execution
occurs outside the state transaction. Commit SUCCEEDED and the new watermark together.
A unique pipeline claim prevents another process sharing this database from entering
the same pipeline. Metadata fingerprints detect incompatible run-ID reuse.

We deliberately do not expire claims based only on time. A paused worker could resume
after expiration and race a replacement. Recovery requires an operator to stop/fence the
old worker, reconcile its sink, and explicitly recover the claim. Production leases need
fencing tokens enforced at the sink.

This provides durable replay suppression for recorded successes, not exactly-once
external effects. Handlers must be idempotent across retries and the crash window between
sink completion and state commit. Non-transient errors are not automatically retried.
