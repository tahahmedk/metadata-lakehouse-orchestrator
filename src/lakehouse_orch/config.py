import json
from pathlib import Path

from .dag import Node, topological_sort
from .executor import TaskSpec


def load_tasks(path: str | Path) -> list[TaskSpec]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, list) or not raw:
        raise ValueError("configuration must contain a nonempty list of tasks")
    specs = []
    for item in raw:
        item = dict(item)
        deps = item.pop("depends_on", [])
        if not isinstance(deps, list) or any(not isinstance(d, str) for d in deps):
            raise ValueError("depends_on must be a list of names")
        specs.append(TaskSpec(**item, depends_on=tuple(deps)))
    topological_sort([Node(s.name, s.depends_on, s.estimated_minutes) for s in specs])
    return specs
