"""SQLite state with exclusive pipeline claims and atomic success/watermarks."""

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

SCHEMA = """
CREATE TABLE IF NOT EXISTS executions (
    run_id TEXT NOT NULL, pipeline TEXT NOT NULL, definition TEXT NOT NULL,
    status TEXT NOT NULL, owner TEXT NOT NULL, watermark_before INTEGER NOT NULL,
    watermark_after INTEGER NOT NULL, PRIMARY KEY(run_id, pipeline)
);
CREATE TABLE IF NOT EXISTS claims (pipeline TEXT PRIMARY KEY, owner TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS watermarks (pipeline TEXT PRIMARY KEY, value INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY, run_id TEXT NOT NULL, pipeline TEXT NOT NULL,
    status TEXT NOT NULL, attempt INTEGER NOT NULL, ts_utc TEXT NOT NULL
);
"""


class AlreadyRunning(RuntimeError):
    pass


class DefinitionChanged(ValueError):
    pass


class AuditStore:
    def __init__(self, path: str | Path):
        self.path = str(path)
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with self.connection() as conn:
            conn.executescript(SCHEMA)

    @contextmanager
    def connection(self):
        conn = sqlite3.connect(self.path, timeout=10)
        try:
            with conn:
                yield conn
        finally:
            conn.close()

    @staticmethod
    def event(conn, run_id: str, pipeline: str, status: str, attempt: int = 0) -> None:
        conn.execute(
            "INSERT INTO events(run_id,pipeline,status,attempt,ts_utc) VALUES(?,?,?,?,?)",
            (run_id, pipeline, status, attempt, datetime.now(timezone.utc).isoformat()),
        )

    def watermark(self, pipeline: str) -> int:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT value FROM watermarks WHERE pipeline=?", (pipeline,)
            ).fetchone()
            return row[0] if row else 0

    def claim(
        self, run_id: str, pipeline: str, definition: str, upper: int
    ) -> tuple[str, int] | None:
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT definition,status,watermark_before,watermark_after FROM executions WHERE run_id=? AND pipeline=?",
                (run_id, pipeline),
            ).fetchone()
            if row and (row[0] != definition or row[3] != upper):
                raise DefinitionChanged("run identity reused with different definition or window")
            if row and row[1] == "SUCCEEDED":
                self.event(conn, run_id, pipeline, "SKIPPED_ALREADY_SUCCEEDED")
                return None
            if conn.execute("SELECT 1 FROM claims WHERE pipeline=?", (pipeline,)).fetchone():
                raise AlreadyRunning(
                    f"{pipeline} has an active claim; recovery requires operator fencing"
                )
            current = conn.execute(
                "SELECT value FROM watermarks WHERE pipeline=?", (pipeline,)
            ).fetchone()
            before = current[0] if current else 0
            if row and row[2] != before:
                raise DefinitionChanged("failed window superseded by another run")
            if upper < before:
                raise ValueError("watermark regression")
            owner = uuid4().hex
            conn.execute("INSERT INTO claims VALUES(?,?)", (pipeline, owner))
            conn.execute(
                "INSERT INTO executions VALUES(?,?,?,?,?,?,?) ON CONFLICT(run_id,pipeline) DO UPDATE SET status=excluded.status,owner=excluded.owner",
                (run_id, pipeline, definition, "RUNNING", owner, before, upper),
            )
            self.event(conn, run_id, pipeline, "CLAIMED")
            return owner, before

    def record_attempt(self, run_id: str, pipeline: str, status: str, attempt: int) -> None:
        with self.connection() as conn:
            self.event(conn, run_id, pipeline, status, attempt)

    def finish(self, run_id: str, pipeline: str, owner: str, success: bool, attempt: int) -> None:
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT watermark_after FROM executions WHERE run_id=? AND pipeline=? AND owner=? AND status='RUNNING'",
                (run_id, pipeline, owner),
            ).fetchone()
            claim = conn.execute(
                "SELECT owner FROM claims WHERE pipeline=?", (pipeline,)
            ).fetchone()
            if not row or not claim or claim[0] != owner:
                raise AlreadyRunning("ownership lost")
            status = "SUCCEEDED" if success else "FAILED"
            if success:
                conn.execute(
                    "INSERT INTO watermarks VALUES(?,?) ON CONFLICT(pipeline) DO UPDATE SET value=excluded.value",
                    (pipeline, row[0]),
                )
            conn.execute(
                "UPDATE executions SET status=? WHERE run_id=? AND pipeline=?",
                (status, run_id, pipeline),
            )
            conn.execute("DELETE FROM claims WHERE pipeline=? AND owner=?", (pipeline, owner))
            self.event(conn, run_id, pipeline, status, attempt)

    def recover_abandoned(
        self, run_id: str, pipeline: str, *, worker_stopped: bool = False
    ) -> None:
        """Explicit operator action after the previous worker has been fenced/stopped."""
        if not worker_stopped:
            raise ValueError("confirm old worker is stopped before recovery")
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT owner FROM executions WHERE run_id=? AND pipeline=? AND status='RUNNING'",
                (run_id, pipeline),
            ).fetchone()
            if not row:
                raise ValueError("no running execution to recover")
            conn.execute("DELETE FROM claims WHERE pipeline=? AND owner=?", (pipeline, row[0]))
            conn.execute(
                "UPDATE executions SET status='FAILED' WHERE run_id=? AND pipeline=?",
                (run_id, pipeline),
            )
            self.event(conn, run_id, pipeline, "RECOVERED")

    def history(self) -> list[tuple]:
        with self.connection() as conn:
            return conn.execute(
                "SELECT run_id,pipeline,status,attempt FROM events ORDER BY id"
            ).fetchall()
