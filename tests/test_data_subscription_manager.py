import unittest
from unittest.mock import Mock, call, patch

from hgraph_fabric.consistent_cut import find_consistent_cut
from hgraph_fabric.data_cache import DataCache
from hgraph_fabric.data_structures import DataRevisionDetails
from hgraph_fabric.data_subscription_manager import DataSubscriptionManager


class DataSubscriptionManagerTests(unittest.TestCase):
    def setUp(self):
        self.latest = {}
        self.revisions = {}
        self.cache = Mock(spec=DataCache)
        self.cache.latest.side_effect = self.latest.__getitem__
        self.cache.get_revision.side_effect = self.revisions.__getitem__
        self.cache.get_latest_revision.side_effect = (
            lambda version: max(r for r in self.revisions if r[:2] == version)
        )
        self.manager = DataSubscriptionManager(self.cache)

    def add_revision(self, data_id, version, count=0, dependencies=None, previous=None):
        revision_id = (data_id, version, count)
        self.revisions[revision_id] = DataRevisionDetails(dependencies, None, previous)
        self.latest[data_id] = max(self.latest.get(data_id, revision_id), revision_id)
        return revision_id

    def test_add_loads_latest_tree_and_reuses_existing_subscription(self):
        revision_id = self.add_revision("prices", 2, dependencies=(("source", 1),))
        self.add_revision("source", 1)
        subscription = self.manager.add_subscription("prices")
        self.assertEqual(subscription.latest_revision, revision_id)
        self.assertEqual(
            set(subscription.latest(self.cache)),
            {(("prices", 2), ("source", 1))},
        )
        self.assertIsNone(subscription.current_revision)
        self.cache.reset_mock()
        self.assertIs(self.manager.add_subscription("prices"), subscription)
        self.assertEqual(self.cache.mock_calls, [])

    def test_add_bulk_accepts_iterable_and_deduplicates(self):
        self.add_revision("a", 1)
        self.add_revision("b", 1)
        self.manager.add_subscriptions(iter(["a", "b", "a"]))
        self.assertEqual(set(self.manager._subscriptions), {"a", "b"})
        self.assertEqual(self.cache.latest.call_args_list, [call("a"), call("b")])

    def test_failed_initial_load_can_be_retried(self):
        self.latest["prices"] = ("prices", 1, 0)
        with self.assertRaises(KeyError):
            self.manager.add_subscription("prices")
        self.assertEqual(self.manager._subscriptions, {})
        self.add_revision("prices", 1)
        subscription = self.manager.add_subscription("prices")
        self.assertEqual(subscription.latest_revision, ("prices", 1, 0))

    def test_empty_manager_has_empty_cut(self):
        self.assertEqual(self.manager.get_consistent_cut(), set())
        self.assertEqual(self.cache.mock_calls, [])

    def test_matching_and_disjoint_trees_keep_latest_versions(self):
        self.add_revision("source", 1)
        self.add_revision("a", 2, dependencies=(("source", 1),))
        self.add_revision("b", 3, dependencies=(("source", 1),))
        self.add_revision("leaf", 4)
        self.manager.add_subscriptions(["a", "b", "leaf"])
        self.assertEqual(
            self.manager.get_consistent_cut(), {("a", 2), ("b", 3), ("leaf", 4)}
        )
        for subscription in self.manager._subscriptions.values():
            self.assertEqual(subscription.current_revision, subscription.latest_revision)

    def test_tournament_aligns_complete_histories_in_either_registration_order(self):
        for version in (1, 2, 3):
            self.add_revision("source", version)
        for data_id, latest_version in (("a", 3), ("b", 2), ("c", 2)):
            for version in range(1, latest_version + 1):
                self.add_revision(
                    data_id, version, dependencies=(("source", version),),
                    previous=(version - 1, 0) if version > 1 else None,
                )
        for order in (["a", "b", "c"], ["c", "b", "a"]):
            with self.subTest(order=order):
                manager = DataSubscriptionManager(self.cache)
                manager.add_subscriptions(order)
                self.assertEqual(
                    manager.get_consistent_cut(), {("a", 2), ("b", 2), ("c", 2)}
                )
                for subscription in manager._subscriptions.values():
                    self.assertEqual(subscription.current_revision[1:], (2, 0))
                self.assertEqual(manager._subscriptions["a"].latest_revision, ("a", 3, 0))

    def test_walks_revision_counts_within_same_version(self):
        self.add_revision("source", 1)
        self.add_revision("source", 2)
        self.add_revision("a", 5, 0, dependencies=(("source", 1),))
        self.add_revision("a", 5, 1, dependencies=(("source", 2),))
        self.add_revision("b", 4, dependencies=(("source", 1),))
        self.manager.add_subscriptions(["a", "b"])
        self.assertEqual(self.manager.get_consistent_cut(), {("a", 5), ("b", 4)})
        self.assertEqual(
            self.manager._subscriptions["a"].current_revision, ("a", 5, 0)
        )

    def test_revision_zero_links_to_previous_versions_maximum_revision(self):
        self.add_revision("source", 1)
        self.add_revision("source", 2)
        for count in range(8):
            self.add_revision("a", 1000, count, dependencies=(("source", 1),))
        self.add_revision("a", 9000, dependencies=(("source", 2),), previous=(1000, 7))
        self.add_revision("b", 1, dependencies=(("source", 1),))
        self.manager.add_subscriptions(["a", "b"])
        self.assertEqual(self.manager.get_consistent_cut(), {("a", 1000), ("b", 1)})
        self.assertEqual(
            self.manager._subscriptions["a"].current_revision, ("a", 1000, 7)
        )
        self.cache.get_revision.assert_any_call(("a", 9000, 0))
        self.assertNotIn(call(("a", 8999, 0)), self.cache.get_revision.call_args_list)

    def test_leaf_subscription_root_participates_in_overlap(self):
        self.add_revision("source", 1)
        self.add_revision("source", 2, previous=(1, 0))
        self.add_revision("a", 1, dependencies=(("source", 1),))
        self.manager.add_subscriptions(["source", "a"])
        self.assertEqual(self.manager.get_consistent_cut(), {("source", 1), ("a", 1)})

    def test_compares_transitive_dependencies(self):
        self.add_revision("source", 1)
        self.add_revision("source", 2)
        self.add_revision("middle", 1, dependencies=(("source", 1),))
        self.add_revision("middle", 2, dependencies=(("source", 2),))
        self.add_revision("a", 1, dependencies=(("middle", 1),))
        self.add_revision("a", 2, dependencies=(("middle", 2),), previous=(1, 0))
        self.add_revision("b", 1, dependencies=(("source", 1),))
        self.manager.add_subscriptions(["a", "b"])
        self.assertEqual(self.manager.get_consistent_cut(), {("a", 1), ("b", 1)})

    def test_failed_search_preserves_current_selections(self):
        self.add_revision("source", 1)
        self.add_revision("source", 2)
        self.add_revision("a", 2, dependencies=(("source", 2),))
        a = self.manager.add_subscription("a")
        self.assertEqual(self.manager.get_consistent_cut(), {("a", 2)})
        self.add_revision("b", 1, dependencies=(("source", 1),))
        b = self.manager.add_subscription("b")
        with self.assertRaises(LookupError):
            self.manager.get_consistent_cut()
        self.assertEqual(a.current_revision, ("a", 2, 0))
        self.assertIsNone(b.current_revision)

    def test_existing_item_rollback_stabilizes_prefix_before_next_insertion(self):
        for data_id in ("x", "y"):
            for version in (1, 2):
                self.add_revision(data_id, version)
        for count, (x, y) in enumerate(((1, 1), (1, 2), (2, 2))):
            self.add_revision("a", 100, count, (("x", x), ("y", y)))
        self.add_revision("b", 100, 0, (("x", 1),))
        self.add_revision("b", 100, 1, (("x", 2),))
        self.add_revision("c", 100, 0, (("y", 1),))
        self.add_revision("d", 100)
        self.manager.add_subscriptions(["a", "b", "c", "d"])
        # a/b initially agree on x=2. Matching a/c rolls a back to x=1,y=1,
        # invalidating a/b; b/c have no overlap and cannot repair that pair.
        def compare(left, right, cache):
            if right.data_id == "d":
                for data_id in ("a", "b", "c"):
                    self.assertEqual(
                        self.manager._subscriptions[data_id].proposed_revision, (data_id, 100, 0)
                    )
            return find_consistent_cut(left, right, cache)

        with patch("hgraph_fabric.data_subscription_manager.find_consistent_cut", side_effect=compare):
            self.assertEqual(
                self.manager.get_consistent_cut(), {(data_id, 100) for data_id in ("a", "b", "c", "d")}
            )
        for data_id, subscription in self.manager._subscriptions.items():
            self.assertEqual(subscription.current_revision, (data_id, 100, 0))

    def test_incoming_item_rollback_rechecks_previously_matching_item(self):
        for data_id in ("x", "y"):
            for version in (1, 2):
                self.add_revision(data_id, version)
        self.add_revision("a", 100, 0, (("x", 1),))
        self.add_revision("a", 100, 1, (("x", 2),))
        self.add_revision("b", 100, 0, (("y", 1),))
        for count, (x, y) in enumerate(((1, 1), (1, 2), (2, 2))):
            self.add_revision("c", 100, count, (("x", x), ("y", y)))
        self.manager.add_subscriptions(["a", "b", "c"])
        # a/b form a consistent prefix. Incoming c first matches a on x=2,
        # then b lowers c to x=1,y=1. Rechecking a/c must now lower a too.
        self.assertEqual(self.manager.get_consistent_cut(), {("a", 100), ("b", 100), ("c", 100)})
        for data_id, subscription in self.manager._subscriptions.items():
            self.assertEqual(subscription.current_revision, (data_id, 100, 0))

    def test_matching_prefix_checks_each_pair_once(self):
        self.add_revision("source", 1)
        for data_id in ("a", "b", "c", "d"):
            self.add_revision(data_id, 100, dependencies=(("source", 1),))
        self.manager.add_subscriptions(["a", "b", "c", "d"])
        with patch(
            "hgraph_fabric.data_subscription_manager.find_consistent_cut", wraps=find_consistent_cut
        ) as compare:
            self.manager.get_consistent_cut()
        self.assertEqual(
            [(args[0].data_id, args[1].data_id) for args, _ in compare.call_args_list],
            [("a", "b"), ("a", "c"), ("b", "c"), ("a", "d"), ("b", "d"), ("c", "d")],
        )

    def test_initialization_uses_proposals_and_keeps_current_until_commit(self):
        self.add_revision("source", 1)
        for data_id in ("a", "b", "c"):
            self.add_revision(data_id, 100, dependencies=(("source", 1),))
        self.manager.add_subscriptions(["a", "b", "c"])
        for subscription in self.manager._subscriptions.values():
            subscription.current_revision = (subscription.data_id, 1, 0)

        def compare(left, right, cache):
            for subscription in self.manager._subscriptions.values():
                self.assertEqual(subscription.proposed_revision, subscription.latest_revision)
                self.assertEqual(subscription.current_revision, (subscription.data_id, 1, 0))
            return find_consistent_cut(left, right, cache)

        with patch("hgraph_fabric.data_subscription_manager.find_consistent_cut", side_effect=compare):
            self.assertEqual(self.manager.get_consistent_cut(), {("a", 100), ("b", 100), ("c", 100)})

    def test_single_subscription_is_reset_to_latest(self):
        self.add_revision("a", 100)
        subscription = self.manager.add_subscription("a")
        subscription.current_revision = ("a", 1, 0)
        self.assertEqual(self.manager.get_consistent_cut(), {("a", 100)})
        self.assertEqual(subscription.current_revision, ("a", 100, 0))

    def test_failed_later_comparison_restores_pre_tournament_selections(self):
        for version in (1, 2):
            self.add_revision("x", version)
        self.add_revision("a", 100, 0, (("x", 1),))
        self.add_revision("a", 100, 1, (("x", 2),))
        self.add_revision("b", 100, dependencies=(("x", 1),))
        self.add_revision("c", 100)
        self.manager.add_subscriptions(["a", "b", "c"])
        a = self.manager._subscriptions["a"]
        a.current_revision = ("a", 100, 1)
        previous = {key: sub.current_revision for key, sub in self.manager._subscriptions.items()}

        def compare(left, right, cache):
            if right.data_id == "c":
                self.assertEqual(a.proposed_revision, ("a", 100, 0))
                self.assertEqual(a.current_revision, ("a", 100, 1))
                raise OSError("Unavailable history")
            return find_consistent_cut(left, right, cache)

        with patch("hgraph_fabric.data_subscription_manager.find_consistent_cut", side_effect=compare):
            with self.assertRaises(OSError):
                self.manager.get_consistent_cut()
        self.assertEqual(
            {key: sub.current_revision for key, sub in self.manager._subscriptions.items()}, previous
        )

    def test_rejects_history_that_does_not_move_to_an_older_version(self):
        self.add_revision("source", 1)
        self.add_revision("source", 2)
        self.add_revision("a", 2, dependencies=(("source", 2),), previous=(2, 0))
        self.add_revision("b", 1, dependencies=(("source", 1),))
        self.manager.add_subscriptions(["a", "b"])
        with self.assertRaises(ValueError):
            self.manager.get_consistent_cut()
        for subscription in self.manager._subscriptions.values():
            self.assertIsNone(subscription.current_revision)

    def test_exact_match_beats_newer_absent_dependency_across_version_gaps(self):
        for version in (1, 2, 4):
            self.add_revision("source", version)
        self.add_revision("a", 100, dependencies=(("source", 1),))
        self.add_revision("a", 500, dependencies=(("source", 2),), previous=(100, 0))
        self.add_revision("a", 900, previous=(500, 0))
        self.add_revision("a", 1200, dependencies=(("source", 4),), previous=(900, 0))
        self.add_revision("b", 1, dependencies=(("source", 2),))
        self.manager.add_subscriptions(["a", "b"])
        self.assertEqual(self.manager.get_consistent_cut(), {("a", 500), ("b", 1)})

    def test_newest_absence_wins_when_dependency_version_has_no_match(self):
        for version in (1, 2, 4):
            self.add_revision("source", version)
        self.add_revision("a", 100, dependencies=(("source", 1),))
        self.add_revision("a", 500, previous=(100, 0))
        self.add_revision("a", 900, previous=(500, 0))
        self.add_revision("a", 1200, dependencies=(("source", 4),), previous=(900, 0))
        self.add_revision("b", 1, dependencies=(("source", 2),))
        self.manager.add_subscriptions(["a", "b"])
        self.assertEqual(self.manager.get_consistent_cut(), {("a", 900), ("b", 1)})

    def test_absent_midpoint_does_not_hide_matches_on_either_side(self):
        for dependencies, expected in (
            ([None, 2, None, 4, None, 5], 200),
            ([None, 1, None, 2, None, 5], 400),
            ([None, 2, None, 2, None, 5], 400),
        ):
            with self.subTest(dependencies=dependencies):
                self.setUp()
                for version in (1, 2, 4, 5):
                    self.add_revision("source", version)
                previous = None
                for i, dependency in enumerate(dependencies, 1):
                    self.add_revision(
                        "a", i * 100,
                        dependencies=((("source", dependency),) if dependency else None),
                        previous=previous,
                    )
                    previous = (i * 100, 0)
                self.add_revision("b", 1, dependencies=(("source", 2),))
                self.manager.add_subscriptions(["a", "b"])
                self.assertEqual(
                    self.manager.get_consistent_cut(), {("a", expected), ("b", 1)}
                )

    def test_history_search_stops_at_newest_exact_match(self):
        for count in range(128):
            self.add_revision("source", count + 1)
            self.add_revision("a", 9000, count, dependencies=(("source", count + 1),))
        self.add_revision("b", 1, dependencies=(("source", 20),))
        self.manager.add_subscriptions(["a", "b"])
        self.cache.reset_mock()
        self.assertEqual(self.manager.get_consistent_cut(), {("a", 9000), ("b", 1)})
        self.assertEqual(
            self.manager._subscriptions["a"].current_revision, ("a", 9000, 19)
        )
        root_loads = [
            args[0] for args, _ in self.cache.get_revision.call_args_list
            if args[0][0] == "a"
        ]
        self.assertEqual(min(revision[2] for revision in root_loads), 19)

    def test_absence_is_fallback_when_exact_match_cannot_form_a_cut(self):
        for data_id, versions in (("x", (2, 3)), ("y", (1, 2))):
            for version in versions:
                self.add_revision(data_id, version)
        self.add_revision("a", 10, dependencies=(("x", 2), ("y", 2)))
        self.add_revision("a", 20, previous=(10, 0))
        self.add_revision("a", 30, dependencies=(("x", 3),), previous=(20, 0))
        self.add_revision("b", 1, dependencies=(("x", 2), ("y", 1)))
        self.manager.add_subscriptions(["a", "b"])
        self.assertEqual(self.manager.get_consistent_cut(), {("a", 20), ("b", 1)})
