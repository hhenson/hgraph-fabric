import unittest

from hgraph_fabric.data_graph import DataEdgeGraph


class DataEdgeGraphTests(unittest.TestCase):
    def test_vertex_set_operations(self):
        graph = DataEdgeGraph([("a", 1), ("b", 2)])
        graph.add(("a", 1))
        self.assertEqual(len(graph), 2)
        self.assertEqual(set(graph), {("a", 1), ("b", 2)})
        self.assertNotIn(("a", 2), graph)
        self.assertNotIn("a", graph)
        self.assertNotIn([], graph)
        self.assertNotIn(([], 1), graph)
        graph.discard(("a", 2))
        with self.assertRaises(KeyError):
            graph.remove(("a", 2))
        graph.remove(("a", 1))
        self.assertEqual(set(graph), {("b", 2)})
        graph.discard(("a", 1))
        graph |= {("c", 3)}
        self.assertEqual(set(graph), {("b", 2), ("c", 3)})
        graph &= {("c", 3)}
        self.assertEqual(set(graph), {("c", 3)})
        self.assertEqual(set(graph | {("d", 4)}), {("c", 3), ("d", 4)})

    def test_conflicting_vertex_does_not_change_graph(self):
        graph = DataEdgeGraph([("a", 1)])
        with self.assertRaises(ValueError):
            graph.add(("a", 2))
        self.assertEqual(set(graph), {("a", 1)})

    def test_directed_edge_addition_and_removal(self):
        graph = DataEdgeGraph()
        edge = (("a", 1), ("b", 2))
        graph.add_edge(edge)
        graph.add_edge(edge)
        self.assertEqual(set(graph), {("a", 1), ("b", 2)})
        self.assertEqual(graph._from_index, {"a": {"b"}, "b": set()})
        self.assertEqual(graph._to_index, {"a": set(), "b": {"a"}})
        with self.assertRaises(KeyError):
            graph.remove_edge((edge[1], edge[0]))
        with self.assertRaises(KeyError):
            graph.remove_edge((("a", 9), ("b", 2)))
        self.assertEqual(graph._from_index["a"], {"b"})
        graph.remove_edge(edge)
        self.assertEqual(set(graph), {("a", 1), ("b", 2)})
        self.assertEqual(graph._from_index, {"a": set(), "b": set()})
        self.assertEqual(graph._to_index, {"a": set(), "b": set()})
        with self.assertRaises(KeyError):
            graph.remove_edge(edge)

    def test_edge_conflicts_do_not_partially_add_endpoints(self):
        graph = DataEdgeGraph([("b", 2)])
        for edge in (
            (("a", 1), ("b", 3)),
            (("b", 3), ("a", 1)),
            (("a", 1), ("a", 2)),
        ):
            with self.subTest(edge=edge):
                with self.assertRaises(ValueError):
                    graph.add_edge(edge)
                self.assertEqual(set(graph), {("b", 2)})
                self.assertEqual(graph._from_index, {"b": set()})
                self.assertEqual(graph._to_index, {"b": set()})

    def test_removing_vertex_cleans_incoming_outgoing_and_self_edges(self):
        graph = DataEdgeGraph()
        for source, target in (("a", "b"), ("b", "c"), ("a", "c"), ("b", "b")):
            graph.add_edge(((source, 1), (target, 1)))
        graph.remove(("b", 1))
        self.assertEqual(set(graph), {("a", 1), ("c", 1)})
        self.assertEqual(graph._from_index, {"a": {"c"}, "c": set()})
        self.assertEqual(graph._to_index, {"a": set(), "c": {"a"}})
        graph.add(("b", 2))
        self.assertEqual(graph._from_index["b"], set())
        self.assertEqual(graph._to_index["b"], set())
        graph.clear()
        self.assertEqual(len(graph), 0)
        self.assertEqual(graph._from_index, {})
        self.assertEqual(graph._to_index, {})

    def test_self_edge_can_be_removed_without_removing_vertex(self):
        graph = DataEdgeGraph()
        edge = (("a", 1), ("a", 1))
        graph.add_edge(edge)
        graph.remove_edge(edge)
        self.assertEqual(set(graph), {("a", 1)})
        self.assertEqual(graph._from_index, {"a": set()})
        self.assertEqual(graph._to_index, {"a": set()})

    def test_graph_instances_are_independent(self):
        graph = DataEdgeGraph()
        other = DataEdgeGraph()
        graph.add_edge((("a", 1), ("b", 2)))
        self.assertEqual(len(other), 0)
        self.assertEqual(other._from_index, {})
        self.assertEqual(other._to_index, {})
