-- SQLite runtime control tables. Integer source offsets; see the runbook for recovery.

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
