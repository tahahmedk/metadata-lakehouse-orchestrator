import heapq
import math
from dataclasses import dataclass


class CycleError(ValueError):
    pass


class MissingDependency(ValueError):
    pass


@dataclass(frozen=True)
class Node:
    name: str
    depends_on: tuple[str, ...] = ()
    estimated_minutes: float = 1.0


def topological_sort(nodes: list[Node]) -> list[str]:
    by_name = {n.name: n for n in nodes}
    if len(by_name) != len(nodes):
        raise ValueError("duplicate node name")
    for node in nodes:
        if not node.name or not math.isfinite(node.estimated_minutes) or node.estimated_minutes < 0:
            raise ValueError("invalid node name or duration")
        if len(node.depends_on) != len(set(node.depends_on)):
            raise ValueError("duplicate dependency")
        missing = set(node.depends_on) - set(by_name)
        if missing:
            raise MissingDependency(f"{node.name} depends on missing nodes {sorted(missing)}")
    indegree = {n.name: len(n.depends_on) for n in nodes}
    children: dict[str, list[str]] = {n.name: [] for n in nodes}
    for node in nodes:
        for dep in node.depends_on:
            children[dep].append(node.name)
    ready = [name for name, count in indegree.items() if count == 0]
    heapq.heapify(ready)
    order = []
    while ready:
        cur = heapq.heappop(ready)
        order.append(cur)
        for child in sorted(children[cur]):
            indegree[child] -= 1
            if indegree[child] == 0:
                heapq.heappush(ready, child)
    if len(order) != len(nodes):
        raise CycleError(f"cycle or blocked descendants: {sorted(set(by_name) - set(order))}")
    return order


def critical_path_minutes(nodes: list[Node]) -> float:
    by_name = {n.name: n for n in nodes}
    longest: dict[str, float] = {}
    for name in topological_sort(nodes):
        node = by_name[name]
        longest[name] = (
            max((longest[d] for d in node.depends_on), default=0) + node.estimated_minutes
        )
    return round(max(longest.values(), default=0), 2)
