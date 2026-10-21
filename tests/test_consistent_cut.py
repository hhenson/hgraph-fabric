import unittest
from unittest.mock import Mock, call, patch

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
        subscription.proposed_revision = revision
        return subscription

    def test_compatible_proposed_trees_return_the_original_revision_pair(self):
        a = self.subscription(self.record("a", 10, dependencies=(("x", 7),)))
        b = self.subscription(self.record("b", 20, dependencies=(("x", 7),)))
        a.current_revision = ("a", 1, 0)
        b.current_revision = ("b", 2, 0)
        self.assertEqual(find_consistent_cut(a, b, self.cache), (("a", 10, 0), ("b", 20, 0)))
        self.assertEqual(a.proposed_revision, a.latest_revision)
        self.assertEqual(b.proposed_revision, b.latest_revision)
        self.assertEqual(a.current_revision, ("a", 1, 0))
        self.assertEqual(b.current_revision, ("b", 2, 0))

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
        self.assertEqual(a.proposed_revision, a.latest_revision)
        self.assertEqual(b.proposed_revision, b.latest_revision)

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
        self.assertEqual(find_consistent_cut(x, a, self.cache), (("x", 1, 0), ("a", 100, 0)))

    def test_leading_leaf_resolves_the_consumed_version_directly(self):
        historical = self.record("x", 100, 7)
        current = self.record("x", 9000, 3)
        x = self.subscription(current)
        a = self.subscription(self.record("a", 500, dependencies=(("x", 100),)))
        # No revision-zero record for x=9000 is available or needed here.
        self.assertEqual(find_consistent_cut(x, a, self.cache), (historical, a.proposed_revision))
        self.assertEqual(find_consistent_cut(a, x, self.cache), (a.proposed_revision, historical))
        self.assertNotIn(call(("x", 9000, 0)), self.cache.get_revision.call_args_list)
        self.assertEqual(x.proposed_revision, current)

    def test_unrelated_leaf_requires_no_adjustment(self):
        x = self.subscription(self.record("x", 100))
        a = self.subscription(self.record("a", 500, dependencies=(("y", 200),)))
        self.assertEqual(
            find_consistent_cut(x, a, self.cache), (x.proposed_revision, a.proposed_revision)
        )
        self.assertEqual(self.cache.get_latest_revision.call_args_list, [call(("y", 200))])

    def test_contained_root_match_ignores_other_common_node_versions(self):
        self.record("middle", 100, 0, (("a_source", 1),))
        self.record("middle", 100, 7, (("a_source", 2),))
        middle = self.subscription(("middle", 100, 0))
        parent = self.subscription(self.record("parent", 200, dependencies=(("middle", 100),)))
        # The parent expands middle's latest revision for version 100, while
        # the subscription has an earlier count. Root versions already agree.
        for left, right in ((middle, parent), (parent, middle)):
            with self.subTest(left=left.data_id):
                self.assertEqual(
                    find_consistent_cut(left, right, self.cache),
                    (left.proposed_revision, right.proposed_revision),
                )
        self.assertEqual(middle.proposed_revision, ("middle", 100, 0))

    def test_newer_contained_root_is_resolved_directly(self):
        historical = self.record("middle", 100, 7, (("a_source", 1),))
        current = self.record("middle", 9000, 3, (("a_source", 3),))
        middle = self.subscription(current)
        parent = self.subscription(self.record("parent", 200, dependencies=(("middle", 100),)))
        # a_source also differs (and sorts first), but only middle matters.
        # No revision-zero header for middle=9000 is required by this shortcut.
        self.assertEqual(
            find_consistent_cut(middle, parent, self.cache),
            (historical, parent.proposed_revision),
        )
        self.assertEqual(
            find_consistent_cut(parent, middle, self.cache),
            (parent.proposed_revision, historical),
        )
        self.assertNotIn(call(("middle", 9000, 0)), self.cache.get_revision.call_args_list)
        self.assertEqual(middle.proposed_revision, current)

    def test_downgraded_contained_root_only_constrains_its_own_version(self):
        self.record("middle", 100, 0, (("a_source", 1),))
        self.record("middle", 100, 7, (("a_source", 2),))
        self.record("middle", 9000, 3, (("a_source", 3),))
        self.record("parent", 200, 0, (("middle", 100),))
        self.record("parent", 200, 1, (("middle", 9000),))
        middle = self.subscription(("middle", 9000, 3))
        middle.proposed_revision = ("middle", 100, 0)
        parent = self.subscription(("parent", 200, 1))
        self.assertEqual(
            find_consistent_cut(middle, parent, self.cache),
            (("middle", 100, 0), ("parent", 200, 0)),
        )
        self.assertEqual(
            find_consistent_cut(parent, middle, self.cache),
            (("parent", 200, 0), ("middle", 100, 0)),
        )
        self.assertEqual(middle.proposed_revision, ("middle", 100, 0))
        self.assertEqual(parent.proposed_revision, ("parent", 200, 1))

    def test_two_leaves_of_same_id_only_adjust_the_leading_version(self):
        self.record("x", 100, 7)
        older = self.subscription(self.record("x", 100, 2))
        newer = self.subscription(self.record("x", 9000, 3))
        self.assertEqual(
            find_consistent_cut(older, newer, self.cache), (("x", 100, 2), ("x", 100, 7))
        )
        self.assertEqual(
            find_consistent_cut(newer, older, self.cache), (("x", 100, 7), ("x", 100, 2))
        )
        same_version = self.subscription(("x", 100, 7))
        self.assertEqual(
            find_consistent_cut(older, same_version, self.cache),
            (("x", 100, 2), ("x", 100, 7)),
        )

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
        a.proposed_revision = a.latest_revision
        b.proposed_revision = b.latest_revision
        before = (a.proposed_revision, b.proposed_revision)
        with self.assertRaises(KeyError):
            find_consistent_cut(a, b, self.cache)
        self.assertEqual((a.proposed_revision, b.proposed_revision), before)

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
        a = self.subscription(self.record("a", 100))
        b = self.subscription(self.record("b", 200))
        for missing in (a, b):
            with self.subTest(missing=missing.data_id):
                missing.proposed_revision = None
                with self.assertRaises(LookupError):
                    find_consistent_cut(a, b, self.cache)
                missing.proposed_revision = missing.latest_revision
        self.assertEqual(self.cache.mock_calls, [])

    def test_proposed_selection_is_used_even_when_latest_is_a_newer_version(self):
        a = self.subscription(self.record("a", 100, dependencies=(("x", 1),)))
        b = self.subscription(self.record("b", 200, dependencies=(("x", 1),)))
        a.latest_revision = ("a", 9000, 5)
        b.latest_revision = ("b", 8000, 7)
        expected = (a.proposed_revision, b.proposed_revision)
        # The newer records are deliberately unavailable. They are not inputs
        # to this comparison and must not be loaded or selected.
        self.assertEqual(find_consistent_cut(a, b, self.cache), expected)
        self.assertEqual((a.proposed_revision, b.proposed_revision), expected)
        self.assertEqual(a.latest_revision, ("a", 9000, 5))
        self.assertEqual(b.latest_revision, ("b", 8000, 7))

    def test_next_comparison_keeps_the_previous_rounds_revision_count_bound(self):
        for count in range(6):
            self.record("a", 100, count, (("x", count),))
        a = self.subscription(("a", 100, 5))
        b = self.subscription(self.record("b", 200, dependencies=(("x", 3),)))
        first = find_consistent_cut(a, b, self.cache)
        self.assertEqual(first, (("a", 100, 3), ("b", 200, 0)))
        a.proposed_revision, b.proposed_revision = first

        c = self.subscription(self.record("c", 300, dependencies=(("x", 1),)))
        c.latest_revision = ("c", 9000, 0)
        self.cache.reset_mock()
        with patch.object(a, "revision", wraps=a.revision) as revisions:
            second = find_consistent_cut(a, c, self.cache)
        self.assertEqual(second, (("a", 100, 1), ("c", 300, 0)))
        self.assertEqual(a.proposed_revision, first[0])
        self.assertEqual(a.latest_revision, ("a", 100, 5))
        self.assertEqual(c.proposed_revision, ("c", 300, 0))
        # Even already cached newer graphs must not be considered again.
        self.assertTrue(all(
            args[0][2] <= 3 for args, _ in revisions.call_args_list
        ))
