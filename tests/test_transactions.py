import sqlite3

import pytest

from lakehouse_orch.audit import AuditStore
from lakehouse_orch.executor import Executor, TaskSpec


def test_watermark_and_success_rollback_together(tmp_path):
    store = AuditStore(tmp_path / "s.db")
    with store.connection() as conn:
        conn.execute("""CREATE TRIGGER reject_success BEFORE INSERT ON events
                        WHEN NEW.status='SUCCEEDED'
                        BEGIN SELECT RAISE(ABORT, 'synthetic state failure'); END""")
    calls = []
    with pytest.raises(sqlite3.IntegrityError):
        Executor(store).execute(TaskSpec("a"), "r", lambda ctx: calls.append(ctx))
    assert len(calls) == 1
    assert store.watermark("a") == 0
    with store.connection() as conn:
        assert conn.execute("SELECT status FROM executions").fetchone()[0] == "RUNNING"
        assert conn.execute("SELECT count(*) FROM claims").fetchone()[0] == 1
    assert not any(event[2] == "SUCCEEDED" for event in store.history())
