import unittest
from unittest.mock import patch

from hgraph_fabric.consistent_cut import find_consistent_cut
from hgraph_fabric.data_cache import DataCache
from hgraph_fabric.data_structures import DataUpdateDetails
from hgraph_fabric.data_subscription_manager import DataSubscriptionManager


class UpdateOnlyCache(DataCache):
    """All available records arrive through the real DataCache.apply_update."""

    def load_latest(self, data_id):
        raise KeyError(data_id)

    def load_revision(self, revision_id):
        raise KeyError(revision_id)

    def load_data(self, version_id):
        raise KeyError(version_id)


class SubscriptionUpdateTests(unittest.TestCase):
    def setUp(self):
        self.cache = UpdateOnlyCache()
        self.manager = DataSubscriptionManager(self.cache)

    def update(self, data_id, version, count=0, dependencies=None, previous=None):
        return DataUpdateDetails(
            id=data_id, revision=(version, count),
            dependencies=dependencies, weak_dependencies=None, previous_revision=previous,
        )

    def seed_pair(self):
        self.cache.apply_update(self.update("x", 1))
        for data_id in ("a", "b"):
            self.cache.apply_update(self.update(data_id, 100, dependencies=(("x", 1),)))
        self.manager.add_subscriptions(["a", "b"])
        self.manager.get_consistent_cut()
        return self.manager._subscriptions["a"], self.manager._subscriptions["b"]

    def test_unsubscribed_dependency_update_changes_only_cache(self):
        a, b = self.seed_pair()
        graph = a.current(self.cache)
        with patch("hgraph_fabric.data_subscription_manager.find_consistent_cut") as compare:
            self.assertEqual(self.manager.apply_update(self.update("x", 2, previous=(1, 0))), set())
        compare.assert_not_called()
        self.assertEqual(self.cache.latest("x"), ("x", 2, 0))
        self.assertEqual(self.cache.get_revision(("x", 2, 0)).previous_revision, (1, 0))
        self.assertEqual(a.latest_revision, ("a", 100, 0))
        self.assertEqual(b.latest_revision, ("b", 100, 0))
        self.assertIs(a.current(self.cache), graph)
        self.assertEqual(a.current_revision, ("a", 100, 0))

    def test_update_before_first_cut_initializes_subscriptions(self):
        self.cache.apply_update(self.update("a", 100))
        self.cache.apply_update(self.update("b", 100))
        self.manager.add_subscriptions(["a", "b"])
        self.assertEqual(
            self.manager.apply_update(self.update("a", 200, previous=(100, 0))),
            {("a", 200), ("b", 100)},
        )
        self.assertEqual(self.cache.latest("a"), ("a", 200, 0))
        self.assertEqual(self.manager._subscriptions["a"].latest_revision, ("a", 200, 0))
        self.assertEqual(self.manager._subscriptions["a"].current_revision, ("a", 200, 0))
        self.assertEqual(self.manager._subscriptions["b"].current_revision, ("b", 100, 0))

    def test_blocked_update_waits_then_peer_update_unlocks_both(self):
        a, b = self.seed_pair()
        self.assertEqual(self.manager.apply_update(self.update("x", 2, previous=(1, 0))), set())
        self.assertEqual(
            self.manager.apply_update(self.update("a", 200, dependencies=(("x", 2),), previous=(100, 0))),
            set(),
        )
        self.assertEqual(a.latest_revision, ("a", 200, 0))
        self.assertEqual(a.current_revision, ("a", 100, 0))
        self.assertEqual(b.current_revision, ("b", 100, 0))

        self.assertEqual(
            self.manager.apply_update(self.update("b", 200, dependencies=(("x", 2),), previous=(100, 0))),
            {("a", 200), ("b", 200)},
        )
        self.assertEqual(a.current_revision, ("a", 200, 0))
        self.assertEqual(b.current_revision, ("b", 200, 0))
        self.assertEqual(self.cache.latest("a"), a.latest_revision)
        self.assertEqual(self.cache.latest("b"), b.latest_revision)

    def test_unchanged_pairs_are_not_recompared(self):
        a, b = self.seed_pair()
        for data_id in ("c", "d"):
            self.cache.apply_update(self.update(data_id, 100))
            self.manager.add_subscription(data_id)
        self.manager.get_consistent_cut()
        with patch(
            "hgraph_fabric.data_subscription_manager.find_consistent_cut",
            wraps=find_consistent_cut,
        ) as compare:
            self.assertEqual(
                self.manager.apply_update(self.update("a", 100, 1, dependencies=(("x", 1),))),
                set(),
            )
        self.assertEqual(
            {frozenset((args[0].data_id, args[1].data_id)) for args, _ in compare.call_args_list},
            {frozenset(("a", other)) for other in ("b", "c", "d")},
        )
        self.assertEqual(compare.call_count, 3)
        self.assertEqual(a.current_revision, ("a", 100, 1))
        self.assertEqual(b.current_revision, ("b", 100, 0))

    def test_single_subscription_and_duplicate_update(self):
        self.cache.apply_update(self.update("a", 100))
        a = self.manager.add_subscription("a")
        self.manager.get_consistent_cut()
        update = self.update("a", 200, previous=(100, 0))
        with patch("hgraph_fabric.data_subscription_manager.find_consistent_cut") as compare:
            self.assertEqual(self.manager.apply_update(update), {("a", 200)})
            self.assertEqual(self.manager.apply_update(update), set())
        compare.assert_not_called()
        self.assertEqual(a.current_revision, ("a", 200, 0))
        self.assertEqual(a.latest_revision, ("a", 200, 0))
        self.assertEqual(self.cache.latest("a"), ("a", 200, 0))

    def test_failed_comparison_retains_update_and_allows_retry(self):
        a, b = self.seed_pair()
        update = self.update("a", 200, dependencies=(("missing", 1),), previous=(100, 0))
        with self.assertRaises(KeyError):
            self.manager.apply_update(update)
        self.assertEqual(a.latest_revision, ("a", 200, 0))
        self.assertEqual(self.cache.latest("a"), ("a", 200, 0))
        self.assertEqual(a.current_revision, ("a", 100, 0))
        self.assertEqual(b.current_revision, ("b", 100, 0))
        self.assertEqual(self.manager.apply_update(self.update("missing", 1)), set())
        self.assertEqual(self.manager.apply_update(update), {("a", 200)})
        self.assertEqual(a.current_revision, ("a", 200, 0))
        self.assertEqual(b.current_revision, ("b", 100, 0))

    def test_proposal_that_regresses_a_selection_keeps_the_previous_cut(self):
        self.cache.apply_update(self.update("a", 100, 0))
        self.cache.apply_update(self.update("a", 100, 1))
        self.cache.apply_update(self.update("b", 100, 0))
        self.manager.add_subscriptions(["a", "b"])
        self.manager.get_consistent_cut()
        a, b = self.manager._subscriptions.values()
        # The pair search can search below its starting revision. The update
        # layer must not publish a proposal below the already established cut.
        with patch(
            "hgraph_fabric.data_subscription_manager.find_consistent_cut",
            return_value=(("a", 100, 0), ("b", 100, 0)),
        ):
            self.assertEqual(
                self.manager.apply_update(self.update("a", 200, previous=(100, 1))),
                set(),
            )
        self.assertEqual(a.latest_revision, ("a", 200, 0))
        self.assertEqual(a.current_revision, ("a", 100, 1))
        self.assertEqual(b.current_revision, ("b", 100, 0))
