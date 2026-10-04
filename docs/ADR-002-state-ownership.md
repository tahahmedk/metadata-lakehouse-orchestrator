# ADR-002: Why orchestration state lives outside the task handler

Status: accepted

Date: 2026-10-04

## The boundary I want

A handler should know how to process its assigned data window. It should not decide
whether another worker owns that window, whether its parents completed, or whether
a previous invocation already committed success.

Keep claims, retry attempts, dependency status, audit history and watermarks in the
control plane. Pass a fixed context into the handler and record the outcome centrally.

## Why not let each handler manage its own progress?

That is convenient for the first pipeline. It becomes difficult to reason about when
handlers disagree about retry counts, advance checkpoints at different points, or
interpret a previous failure differently. A scheduler cannot make a sound dependency
decision if each task defines "done" for itself.

The current store claims a pipeline before invoking the handler. On success, it commits
the execution result and watermark together. On failure, the old watermark remains.
The scheduler uses those outcomes to release children or mark them blocked. Recovery
can inspect one history instead of reconstructing decisions from task-specific logs.

## What remains the handler's responsibility

The handler owns data-plane correctness: reading the assigned offset range, validating
its transformation and making sink effects safe to replay. Central state does not make
external writes transactional with SQLite. A crash after a sink write but before the
success record can still cause the same handler to run again.

Exactly-once external effects are not promised. A merge/upsert, sink-side deduplication
or a backend-specific transaction is still required.

## Trade-offs and consequences

The state store becomes a dependency for all progress. Its schema, availability and
transaction boundaries deserve more care than a task-local status file. Handlers lose
some scheduling autonomy, and the current context only models integer source offsets.

In return, handlers remain small enough to test independently and recovery has one
ownership model. Moving to distributed workers would require durable dispatch and
fenced leases in addition to a shared store; merely replacing SQLite with Postgres
would not complete that design.

Use handler-local state for algorithmic details when needed. Do not let it become a
second authority for whether the orchestrator has completed a logical execution.
