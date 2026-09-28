import hashlib
import json
import logging
import math
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import asdict, dataclass
from typing import Callable

from .audit import AlreadyRunning, AuditStore
from .dag import Node, topological_sort

logger = logging.getLogger("lakehouse_orch")


@dataclass(frozen=True)
class TaskSpec:
    name: str
    depends_on: tuple[str, ...] = ()
    retries: int = 1
    retry_delay_seconds: float = 0.01
    estimated_minutes: float = 1.0
    kind: str = "incremental"
    upper_watermark: int = 100

    def __post_init__(self):
        if type(self.retries) is not int or not 0 <= self.retries <= 10:
            raise ValueError("retries must be an integer between zero and ten")
        if not math.isfinite(self.retry_delay_seconds) or not 0 <= self.retry_delay_seconds <= 60:
            raise ValueError("invalid retry delay")
        if type(self.upper_watermark) is not int or self.upper_watermark < 0:
            raise ValueError("watermark must be a nonnegative integer")
        if self.kind not in {"incremental", "full-load", "backfill", "ad-hoc", "exploration"}:
            raise ValueError("unknown workload kind")
        topological_sort([Node(self.name, (), self.estimated_minutes)])

    def fingerprint(self) -> str:
        return hashlib.sha256(json.dumps(asdict(self), sort_keys=True).encode()).hexdigest()


@dataclass(frozen=True)
class TaskContext:
    run_id: str
    pipeline: str
    watermark_before: int
    watermark_after: int


class RetryableError(RuntimeError):
    """Only explicitly transient data-plane failures are retried."""


class TaskFailed(RuntimeError):
    pass


class Executor:
    def __init__(self, audit: AuditStore):
        self.audit = audit

    def execute(self, spec: TaskSpec, run_id: str, fn: Callable[[TaskContext], None]) -> str:
        if not run_id.strip():
            raise ValueError("run_id is required")
        claim = self.audit.claim(run_id, spec.name, spec.fingerprint(), spec.upper_watermark)
        if claim is None:
            return "SKIPPED_ALREADY_SUCCEEDED"
        owner, before = claim
        context = TaskContext(run_id, spec.name, before, spec.upper_watermark)
        for attempt in range(1, spec.retries + 2):
            self.audit.record_attempt(run_id, spec.name, "RUNNING", attempt)
            try:
                fn(context)
            except Exception as exc:
                self.audit.record_attempt(run_id, spec.name, "ATTEMPT_FAILED", attempt)
                if isinstance(exc, RetryableError) and attempt <= spec.retries:
                    time.sleep(spec.retry_delay_seconds * 2 ** (attempt - 1))
                    continue
                self.audit.finish(run_id, spec.name, owner, False, attempt)
                raise TaskFailed(f"{spec.name} failed on attempt {attempt}") from exc
            # A state-store failure must not be treated as a reason to replay side effects.
            self.audit.finish(run_id, spec.name, owner, True, attempt)
            logger.info(
                json.dumps(
                    {
                        "event": "task_succeeded",
                        "run_id": run_id,
                        "pipeline": spec.name,
                        "attempt": attempt,
                        "watermark": spec.upper_watermark,
                    }
                )
            )
            return "SUCCEEDED"
        raise AssertionError("unreachable")

    def run(
        self,
        specs: list[TaskSpec],
        run_id: str,
        handlers: dict[str, Callable[[TaskContext], None]],
        max_workers: int = 2,
    ) -> dict[str, str]:
        if type(max_workers) is not int or not 1 <= max_workers <= 32:
            raise ValueError("max_workers must be between one and 32")
        order = topological_sort([Node(s.name, s.depends_on, s.estimated_minutes) for s in specs])
        if set(handlers) != set(order):
            raise ValueError("handlers must match configured pipelines")
        by_name = {s.name: s for s in specs}
        pending = set(order)
        results: dict[str, str] = {}
        good = {"SUCCEEDED", "SKIPPED_ALREADY_SUCCEEDED"}
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            running = {}
            while pending or running:
                for name in order:
                    if name not in pending:
                        continue
                    dependencies = by_name[name].depends_on
                    if any(d in results and results[d] not in good for d in dependencies):
                        results[name] = "BLOCKED"
                        self.audit.record_attempt(run_id, name, "BLOCKED", 0)
                        pending.remove(name)
                    elif all(d in results for d in dependencies) and len(running) < max_workers:
                        running[
                            pool.submit(self.execute, by_name[name], run_id, handlers[name])
                        ] = name
                        pending.remove(name)
                if running:
                    done, _ = wait(running, return_when=FIRST_COMPLETED)
                    for future in done:
                        name = running.pop(future)
                        try:
                            results[name] = future.result()
                        except TaskFailed:
                            results[name] = "FAILED"
                        except AlreadyRunning:
                            results[name] = "BUSY"
                elif pending:
                    raise RuntimeError("scheduler made no progress")
        return {name: results[name] for name in order}
