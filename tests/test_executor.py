from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pytest

from lakehouse_orch.audit import AlreadyRunning, AuditStore, DefinitionChanged
from lakehouse_orch.executor import Executor, RetryableError, TaskFailed, TaskSpec


def test_retry_restart_skip_and_durable_watermark(tmp_path):
    store = AuditStore(tmp_path / "state.db")
    executor = Executor(store)
    calls = []

    def flaky(ctx):
        calls.append((ctx.watermark_before, ctx.watermark_after))
        if len(calls) == 1:
            raise RetryableError("transient")

    spec = TaskSpec("a", upper_watermark=10)
    assert executor.execute(spec, "run1", flaky) == "SUCCEEDED"
    reopened = AuditStore(tmp_path / "state.db")
    assert reopened.watermark("a") == 10
    assert Executor(reopened).execute(spec, "run1", flaky) == "SKIPPED_ALREADY_SUCCEEDED"
    assert calls == [(0, 10), (0, 10)]
    Executor(reopened).execute(
        TaskSpec("a", upper_watermark=20),
        "run2",
        lambda ctx: calls.append((ctx.watermark_before, ctx.watermark_after)),
    )
    assert calls[-1] == (10, 20)
    assert reopened.watermark("a") == 20


def test_failure_does_not_advance_and_descendants_are_blocked(tmp_path):
    store = AuditStore(tmp_path / "state.db")
    specs = [TaskSpec("down", ("bad",)), TaskSpec("bad"), TaskSpec("independent")]

    def broken(ctx):
        raise ValueError("permanent")

    handlers = {
        "bad": broken,
        "down": lambda ctx: pytest.fail("blocked handler executed"),
        "independent": lambda ctx: None,
    }
    result = Executor(store).run(specs, "r", handlers)
    assert result == {"bad": "FAILED", "down": "BLOCKED", "independent": "SUCCEEDED"}
    assert store.watermark("bad") == 0
    assert store.watermark("independent") == 100


def test_concurrent_claims_exclude_same_pipeline(tmp_path):
    store = AuditStore(tmp_path / "state.db")
    start, release = Event(), Event()

    def blocking(ctx):
        start.set()
        assert release.wait(5)

    with ThreadPoolExecutor(max_workers=2) as pool:
        future = pool.submit(Executor(store).execute, TaskSpec("a"), "r1", blocking)
        assert start.wait(5)
        try:
            with pytest.raises(AlreadyRunning):
                Executor(AuditStore(tmp_path / "state.db")).execute(
                    TaskSpec("a"), "r2", lambda ctx: None
                )
        finally:
            release.set()
        assert future.result() == "SUCCEEDED"


def test_recovery_requires_fenced_worker_and_preserves_watermark(tmp_path):
    store = AuditStore(tmp_path / "state.db")
    spec = TaskSpec("a")
    store.claim("r", "a", spec.fingerprint(), 100)
    with pytest.raises(ValueError):
        store.recover_abandoned("r", "a")
    store.recover_abandoned("r", "a", worker_stopped=True)
    assert store.watermark("a") == 0
    assert Executor(store).execute(spec, "r", lambda ctx: None) == "SUCCEEDED"


def test_definition_change_and_watermark_regression_rejected(tmp_path):
    store = AuditStore(tmp_path / "state.db")
    Executor(store).execute(TaskSpec("a", upper_watermark=10), "r", lambda ctx: None)
    with pytest.raises(DefinitionChanged):
        Executor(store).execute(TaskSpec("a", upper_watermark=20), "r", lambda ctx: None)
    with pytest.raises(ValueError):
        Executor(store).execute(TaskSpec("a", upper_watermark=9), "new", lambda ctx: None)


def test_retry_exhaustion_and_failure_history(tmp_path):
    store = AuditStore(tmp_path / "state.db")

    def broken(ctx):
        raise RetryableError("transient")

    with pytest.raises(TaskFailed):
        Executor(store).execute(TaskSpec("a", retries=2, retry_delay_seconds=0), "r", broken)
    assert sum(row[2] == "ATTEMPT_FAILED" for row in store.history()) == 3
    assert store.watermark("a") == 0


def test_parallel_independent_tasks(tmp_path):
    from threading import Barrier

    barrier = Barrier(2)

    def handler(ctx):
        barrier.wait(timeout=5)

    result = Executor(AuditStore(tmp_path / "s.db")).run(
        [TaskSpec("a"), TaskSpec("b")], "r", {"a": handler, "b": handler}, max_workers=2
    )
    assert set(result.values()) == {"SUCCEEDED"}


@pytest.mark.parametrize(
    "kwargs", [{"retries": -1}, {"retries": 1.5}, {"upper_watermark": -1}, {"kind": "typo"}]
)
def test_invalid_task(kwargs):
    with pytest.raises(ValueError):
        TaskSpec("a", **kwargs)
