import pytest

from lakehouse_orch.config import load_tasks
from lakehouse_orch.dag import MissingDependency, Node, critical_path_minutes, topological_sort


@pytest.mark.parametrize(
    "nodes",
    [
        [Node("a"), Node("a")],
        [Node("b", ("a", "a")), Node("a")],
        [Node("a", estimated_minutes=float("nan"))],
        [Node("")],
    ],
)
def test_invalid_graph(nodes):
    with pytest.raises(ValueError):
        topological_sort(nodes)


def test_missing_dependency_and_empty_graph():
    with pytest.raises(MissingDependency):
        topological_sort([Node("a", ("missing",))])
    assert critical_path_minutes([]) == 0


def test_config_unknown_fields_rejected(tmp_path):
    path = tmp_path / "config.json"
    path.write_text('[{"name": "x", "typo": true}]')
    with pytest.raises(TypeError):
        load_tasks(path)
