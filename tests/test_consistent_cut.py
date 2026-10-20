import unittest
from unittest.mock import Mock, call

from hgraph_fabric.consistent_cut import find_consistent_cut
from hgraph_fabric.data_cache import DataCache
from hgraph_fabric.data_structures import DataRevisionDetails
from hgraph_fabric.data_subscription_graph import DataSubscriptionGraph


class ConsistentCutTests(unittest.TestCase):
    def setUp(self):
        self.revisions = {}
        self.cache = Mock(spec=DataCache)
        self.cache.get_revision.side_effect = self.revisions.__getitem__
        self.cache.get_latest_revision.side_effect = (
            lambda version: max(r for r in self.revisions if r[:2] == version)
        )

    def record(self, data_id, version, count=0, dependencies=None, previous=None):
        revision = (data_id, version, count)
        self.revisions[revision] = DataRevisionDetails(dependencies, None, previous)
        for dependency in dependencies or ():
            self.revisions.setdefault((*dependency, 0), DataRevisionDetails(None, None, None))
        return revision

    def subscription(self, revision):
        subscription = DataSubscriptionGraph(revision[0])
        subscription.latest_revision = revision
        return subscription

    def test_compatible_latest_trees_return_the_original_revision_pair(self):
        a = self.subscription(self.record("a", 10, dependencies=(("x", 7),)))
        b = self.subscription(self.record("b", 20, dependencies=(("x", 7),)))
        self.assertEqual(find_consistent_cut(a, b, self.cache), (("a", 10, 0), ("b", 20, 0)))
        self.assertIsNone(a.current_revision)
        self.assertIsNone(b.current_revision)

    def test_disjoint_leaf_roots_need_no_history_search(self):
        a = self.subscription(self.record("a", 10))
        b = self.subscription(self.record("b", 20))
        self.assertEqual(find_consistent_cut(a, b, self.cache), (("a", 10, 0), ("b", 20, 0)))
        self.assertEqual(self.cache.get_revision.call_args_list, [
            call(("a", 10, 0)), call(("b", 20, 0)),
        ])

    def test_bounds_use_zero_and_previous_maximum_across_sparse_versions(self):
        for count in range(16):
            self.record("a", 1000, count, (("x", count),))
        for count in range(9):
            self.record(
                "a", 9000, count, (("x", 100 + count),),
                previous=(1000, 15) if count == 0 else None,
            )
        a = self.subscription(("a", 9000, 8))
        b = self.subscription(self.record("b", 50, dependencies=(("x", 7),)))
        expected = (("a", 1000, 7), ("b", 50, 0))
        self.assertEqual(find_consistent_cut(a, b, self.cache), expected)
        a_loads = [args[0] for args, _ in self.cache.get_revision.call_args_list
                   if args[0][0] == "a"]
        self.assertIn(("a", 9000, 0), a_loads)
        self.assertIn(("a", 1000, 15), a_loads)
        self.assertFalse(any(0 < revision[2] < 8 for revision in a_loads
                             if revision[1] == 9000))
        self.assertTrue(all(revision[1] in (1000, 9000) for revision in a_loads))
        self.assertEqual(b.latest_revision, expected[1])
        self.assertIsNone(a.current_revision)
        self.assertIsNone(b.current_revision)

    def test_matching_first_node_can_require_moving_forward_for_another(self):
        # At count 1, x matches but y lags. Search forward inside the same
        # version to count 2; do not move the fixed subscription backwards.
        for count, (x, y) in enumerate(((0, 0), (1, 0), (1, 1), (2, 1))):
            self.record("a", 100, count, (("x", x), ("y", y)))
        a = self.subscription(("a", 100, 3))
        b = self.subscription(self.record("b", 200, dependencies=(("x", 1), ("y", 1))))
        self.assertEqual(find_consistent_cut(a, b, self.cache), (("a", 100, 2), ("b", 200, 0)))
        self.assertIn(call(("a", 100, 1)), self.cache.get_revision.call_args_list)

    def test_matching_first_node_can_require_moving_backward_for_another(self):
        values = ((0, 0), (1, 0), (1, 1), (1, 2), (2, 2), (2, 3), (2, 4))
        for count, (x, y) in enumerate(values):
            self.record("a", 100, count, (("x", x), ("y", y)))
        a = self.subscription(("a", 100, 6))
        b = self.subscription(self.record("b", 200, dependencies=(("x", 1), ("y", 1))))
        self.assertEqual(find_consistent_cut(a, b, self.cache), (("a", 100, 2), ("b", 200, 0)))
        self.assertIn(call(("a", 100, 3)), self.cache.get_revision.call_args_list)

    def test_returns_argument_order_when_right_subscription_leads(self):
        for count in range(4):
            self.record("a", 100, count, (("x", count),))
        a = self.subscription(("a", 100, 3))
        b = self.subscription(self.record("b", 200, dependencies=(("x", 1),)))
        self.assertEqual(find_consistent_cut(b, a, self.cache), (("b", 200, 0), ("a", 100, 1)))

    def test_exact_match_beats_newer_absence_after_disappearance(self):
        for count, x in enumerate((None, 1, 2, None, 4)):
            self.record("a", 100, count, (("x", x),) if x is not None else None)
        a = self.subscription(("a", 100, 4))
        b = self.subscription(self.record("b", 200, dependencies=(("x", 2),)))
        self.assertEqual(find_consistent_cut(a, b, self.cache), (("a", 100, 2), ("b", 200, 0)))

    def test_newest_absence_is_fallback_after_exact_match_is_ruled_out(self):
        for count, x in enumerate((None, None, None, 9, 10)):
            self.record("a", 100, count, (("x", x),) if x is not None else None)
        a = self.subscription(("a", 100, 4))
        b = self.subscription(self.record("b", 200, dependencies=(("x", 7),)))
        self.assertEqual(find_consistent_cut(a, b, self.cache), (("a", 100, 2), ("b", 200, 0)))

    def test_absent_candidate_must_still_match_other_common_nodes(self):
        for count, (x, y) in enumerate(((None, 1), (None, 2), (9, 2))):
            dependencies = (("y", y),) if x is None else (("x", x), ("y", y))
            self.record("a", 100, count, dependencies)
        a = self.subscription(("a", 100, 2))
        b = self.subscription(self.record("b", 200, dependencies=(("x", 7), ("y", 1))))
        self.assertEqual(find_consistent_cut(a, b, self.cache), (("a", 100, 0), ("b", 200, 0)))

    def test_leaf_root_is_checked_when_other_tree_consumes_it(self):
        self.record("a", 100, 0, (("x", 1),))
        a = self.subscription(self.record("a", 100, 1, (("x", 2),)))
        x = self.subscription(("x", 1, 0))
        self.assertEqual(find_consistent_cut(a, x, self.cache), (("a", 100, 0), ("x", 1, 0)))

    def test_newly_common_node_is_checked_and_may_disappear(self):
        self.record("a", 100, 0, (("x", 0), ("y", 2)))
        self.record("a", 100, 1, (("x", 1), ("y", 2)))
        self.record("a", 100, 2, (("x", 1),))
        a = self.subscription(self.record("a", 100, 3, (("x", 2),)))
        b = self.subscription(self.record("b", 200, dependencies=(("x", 1), ("y", 1))))
        self.assertEqual(find_consistent_cut(a, b, self.cache), (("a", 100, 2), ("b", 200, 0)))

    def test_missing_record_raises_without_changing_selections(self):
        a = self.subscription(self.record("a", 100, 4, (("x", 9),)))
        b = self.subscription(self.record("b", 200, dependencies=(("x", 7),)))
        a.current_revision = a.latest_revision
        b.current_revision = b.latest_revision
        before = (a.current_revision, b.current_revision)
        with self.assertRaises(KeyError):
            find_consistent_cut(a, b, self.cache)
        self.assertEqual((a.current_revision, b.current_revision), before)

    def test_monotonic_revision_range_uses_binary_probes(self):
        for count in range(128):
            self.record("a", 1000, count, (("x", count),))
        a = self.subscription(("a", 1000, 127))
        b = self.subscription(self.record("b", 2000, dependencies=(("x", 20),)))
        self.assertEqual(find_consistent_cut(a, b, self.cache), (("a", 1000, 20), ("b", 2000, 0)))
        loads = [args[0] for args, _ in self.cache.get_revision.call_args_list
                 if args[0][0] == "a"]
        self.assertLessEqual(len(loads), 12)

    def test_missing_selection_raises(self):
        with self.assertRaises(LookupError):
            find_consistent_cut(
                DataSubscriptionGraph("a"), DataSubscriptionGraph("b"), self.cache
            )
        self.assertEqual(self.cache.mock_calls, [])
