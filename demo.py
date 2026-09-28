import json
import logging
from pathlib import Path

from lakehouse_orch.audit import AuditStore
from lakehouse_orch.config import load_tasks
from lakehouse_orch.dag import Node, critical_path_minutes, topological_sort
from lakehouse_orch.executor import Executor, TaskContext
from lakehouse_orch.workload import classify


def synthetic_handler(ctx: TaskContext) -> None:
    # A real sink uses a merge/upsert keyed by business key or run_id/pipeline.
    # This mock intentionally has no external side effects.
    assert ctx.watermark_after >= ctx.watermark_before


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    base = Path(__file__).parent
    specs = load_tasks(base / "config/pipelines.json")
    nodes = [Node(s.name, s.depends_on, s.estimated_minutes) for s in specs]
    audit = AuditStore(base / "out/control.db")
    result = Executor(audit).run(
        specs, "synthetic-window-001", {s.name: synthetic_handler for s in specs}
    )
    print(
        json.dumps(
            {
                "plan": topological_sort(nodes),
                "critical_path_minutes": critical_path_minutes(nodes),
                "results": result,
                "watermarks": {s.name: audit.watermark(s.name) for s in specs},
                "workloads": {s.name: classify(s.kind, s.estimated_minutes) for s in specs},
            },
            indent=2,
        )
    )
