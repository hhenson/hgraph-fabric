import unittest
from unittest.mock import Mock, call

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

    def test_merge_walk_alternates_histories_until_all_three_agree(self):
        for version in (1, 2, 3):
            self.add_revision("source", version)
        for data_id, source_version in (("a", 3), ("b", 2), ("c", 2)):
            self.add_revision(data_id, 1, dependencies=(("source", 1),))
            self.add_revision(
                data_id, 2, dependencies=(("source", source_version),), previous=(1, 0)
            )
        for order in (["a", "b", "c"], ["c", "b", "a"]):
            with self.subTest(order=order):
                manager = DataSubscriptionManager(self.cache)
                manager.add_subscriptions(order)
                self.assertEqual(
                    manager.get_consistent_cut(), {("a", 1), ("b", 1), ("c", 1)}
                )
                for subscription in manager._subscriptions.values():
                    self.assertEqual(subscription.current_revision[1:], (1, 0))
                    self.assertEqual(subscription.latest_revision[1:], (2, 0))

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
        self.add_revision("a", 1000, 7, dependencies=(("source", 1),))
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

    def test_steps_other_tree_when_dependency_was_absent_in_older_revision(self):
        self.add_revision("source", 1)
        self.add_revision("source", 2)
        self.add_revision("a", 10, dependencies=(("source", 2),))
        self.add_revision("b", 10)
        self.add_revision("b", 30, dependencies=(("source", 1),), previous=(10, 0))
        self.manager.add_subscriptions(["a", "b"])
        self.assertEqual(self.manager.get_consistent_cut(), {("a", 10), ("b", 10)})

    def test_backtracks_when_changing_dependencies_make_merge_path_a_dead_end(self):
        self.add_revision("x", 1)
        self.add_revision("x", 2)
        self.add_revision("z", 1)
        self.add_revision("z", 2)
        self.add_revision("a", 10, dependencies=(("x", 2), ("z", 1)))
        self.add_revision(
            "a", 30, dependencies=(("x", 2), ("z", 2)), previous=(10, 0)
        )
        self.add_revision("b", 10, dependencies=(("z", 2),))
        self.add_revision("b", 30, dependencies=(("x", 1),), previous=(10, 0))
        self.manager.add_subscriptions(["a", "b"])
        self.assertEqual(self.manager.get_consistent_cut(), {("a", 30), ("b", 10)})
        self.assertEqual(
            self.manager._subscriptions["a"].current_revision, ("a", 30, 0)
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
