import unittest

from hgraph_fabric.data_graph import DataEdgeGraph


AB = (("a", 1), ("b", 2))
BC = (("b", 2), ("c", 3))
AC = (("a", 1), ("c", 3))


class DataEdgeGraphTests(unittest.TestCase):
    def assert_indexes(self, graph, expected):
        versions = {data_id: version for edge in expected for data_id, version in edge}
        outgoing = {data_id: set() for data_id in versions}
        incoming = {data_id: set() for data_id in versions}
        for source, target in expected:
            outgoing[source[0]].add(target[0])
            incoming[target[0]].add(source[0])
        self.assertEqual(set(graph), expected)
        self.assertEqual(len(graph), len(expected))
        self.assertEqual(graph._edges, expected)
        self.assertEqual(graph._nodes, versions)
        self.assertEqual(graph._from_index, outgoing)
        self.assertEqual(graph._to_index, incoming)

    def test_node_and_data_id_queries_follow_edge_changes(self):
        graph = DataEdgeGraph()
        self.assertEqual(list(graph.nodes()), [])
        self.assertEqual(list(graph.data_ids()), [])
        self.assertFalse(graph.has_node(("a", 1)))
        self.assertFalse(graph.has_data_id("a"))
        graph.add(AB)
        graph.add(BC)
        self.assertEqual(set(graph.nodes()), {("a", 1), ("b", 2), ("c", 3)})
        self.assertEqual(set(graph.data_ids()), {"a", "b", "c"})
        self.assertTrue(graph.has_node(("a", 1)))
        self.assertFalse(graph.has_node(("a", 9)))
        self.assertTrue(graph.has_data_id("a"))
        self.assertFalse(graph.has_data_id("missing"))
        graph.remove(AB)
        self.assertFalse(graph.has_node(("a", 1)))
        self.assertFalse(graph.has_data_id("a"))
        self.assertTrue(graph.has_node(("b", 2)))
        self.assertTrue(graph.has_data_id("b"))
        self.assertEqual(set(graph.nodes()), {("b", 2), ("c", 3)})
        self.assertEqual(set(graph.data_ids()), {"b", "c"})
        graph.clear()
        self.assertEqual(list(graph.nodes()), [])
        self.assertEqual(list(graph.data_ids()), [])

    def test_get_data_version_id_tracks_current_nodes(self):
        graph = DataEdgeGraph([AB, BC])
        self.assertEqual(graph.get_data_version_id("a"), ("a", 1))
        self.assertEqual(graph.get_data_version_id("b"), ("b", 2))
        with self.assertRaises(KeyError):
            graph.get_data_version_id("missing")
        graph.remove(AB)
        with self.assertRaises(KeyError):
            graph.get_data_version_id("a")
        self.assertEqual(graph.get_data_version_id("b"), ("b", 2))
        graph.add((("a", 9), ("b", 2)))
        self.assertEqual(graph.get_data_version_id("a"), ("a", 9))

    def test_membership_and_iteration_are_over_directed_edges(self):
        graph = DataEdgeGraph([AB, BC])
        graph.add(AB)
        self.assert_indexes(graph, {AB, BC})
        self.assertIn(AB, graph)
        for absent in (AC, (AB[1], AB[0]), (("a", 9), ("b", 2)),
                       ("a", 1), "a", [], ([], AB[1])):
            with self.subTest(value=absent):
                self.assertNotIn(absent, graph)

    def test_predecessors_and_successors_follow_edge_changes(self):
        graph = DataEdgeGraph([AB, BC, AC])
        self.assertEqual(set(graph.successors("a")), {"b", "c"})
        self.assertEqual(set(graph.predecessors("c")), {"a", "b"})
        self.assertEqual(set(graph.successors("b")), {"c"})
        self.assertEqual(set(graph.predecessors("b")), {"a"})
        self.assertEqual(list(graph.predecessors("a")), [])
        self.assertEqual(list(graph.successors("c")), [])
        graph.remove(AC)
        self.assertEqual(set(graph.successors("a")), {"b"})
        self.assertEqual(set(graph.predecessors("c")), {"b"})
        self_edge = (("b", 2), ("b", 2))
        graph.add(self_edge)
        self.assertEqual(set(graph.successors("b")), {"b", "c"})
        self.assertEqual(set(graph.predecessors("b")), {"a", "b"})
        graph.remove(self_edge)
        graph.remove(AB)
        for query in (graph.successors, graph.predecessors):
            for missing in ("a", "unknown"):
                with self.subTest(query=query.__name__, data_id=missing):
                    with self.assertRaises(KeyError):
                        query(missing)

    def test_remove_and_discard_edges(self):
        graph = DataEdgeGraph([AB, BC])
        graph.discard(AC)
        with self.assertRaises(KeyError):
            graph.remove(AC)
        with self.assertRaises(KeyError):
            graph.remove((("a", 9), ("b", 2)))
        self.assert_indexes(graph, {AB, BC})
        graph.remove(AB)
        self.assert_indexes(graph, {BC})
        graph.discard(AB)
        graph.discard(BC)
        self.assert_indexes(graph, set())

    def test_conflicts_do_not_partially_mutate_graph(self):
        graph = DataEdgeGraph([AB])
        for edge in (
            (("x", 1), ("b", 3)),
            (("b", 3), ("x", 1)),
            (("x", 1), ("x", 2)),
        ):
            with self.subTest(edge=edge):
                with self.assertRaises(ValueError):
                    graph.add(edge)
                self.assert_indexes(graph, {AB})

    def test_versions_are_released_when_last_incident_edge_is_removed(self):
        graph = DataEdgeGraph([AB, BC])
        graph.remove(AB)
        with self.assertRaises(ValueError):
            graph.add((("b", 9), ("c", 3)))
        graph.remove(BC)
        replacement = (("a", 7), ("b", 9))
        graph.add(replacement)
        self.assert_indexes(graph, {replacement})

    def test_self_edges_and_reverse_edges_remain_independent(self):
        self_edge = (("a", 1), ("a", 1))
        reverse = (AB[1], AB[0])
        graph = DataEdgeGraph([AB, reverse, self_edge])
        graph.remove(AB)
        self.assert_indexes(graph, {reverse, self_edge})
        graph.remove(reverse)
        self.assert_indexes(graph, {self_edge})
        graph.remove(self_edge)
        self.assert_indexes(graph, set())

    def test_set_algebra_preserves_edges_and_indexes(self):
        graph = DataEdgeGraph([AB, BC])
        self.assert_indexes(graph | {AC}, {AB, BC, AC})
        self.assert_indexes(graph & {AB, AC}, {AB})
        self.assert_indexes(graph - {AB}, {BC})
        self.assert_indexes(graph ^ {AB, AC}, {BC, AC})
        graph |= {AC}
        self.assert_indexes(graph, {AB, BC, AC})
        graph &= {AB, AC}
        self.assert_indexes(graph, {AB, AC})
        graph -= {AB}
        self.assert_indexes(graph, {AC})
        graph ^= {AC, BC}
        self.assert_indexes(graph, {BC})
        self.assertEqual(graph.pop(), BC)
        self.assert_indexes(graph, set())
        graph.add(AB)
        graph.add(BC)
        graph.clear()
        self.assert_indexes(graph, set())

    def test_graph_instances_are_independent(self):
        graph = DataEdgeGraph([AB])
        other = DataEdgeGraph()
        other.add(BC)
        self.assert_indexes(graph, {AB})
        self.assert_indexes(other, {BC})
