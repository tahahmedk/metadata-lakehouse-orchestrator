import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))
from lakehouse_orch.dag import CycleError, Node, critical_path_minutes, topological_sort


class DagTests(unittest.TestCase):
    def test_order_and_critical_path(self):
        nodes = [
            Node("a", (), 2),
            Node("b", ("a",), 5),
            Node("c", ("a",), 1),
            Node("d", ("b", "c"), 3),
        ]
        self.assertEqual(topological_sort(nodes), ["a", "b", "c", "d"])
        self.assertEqual(critical_path_minutes(nodes), 10)

    def test_cycle(self):
        with self.assertRaises(CycleError):
            topological_sort([Node("a", ("b",)), Node("b", ("a",))])


if __name__ == "__main__":
    unittest.main()
