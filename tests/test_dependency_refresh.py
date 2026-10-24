"""Soft conflict subscriptions refresh exact versions and wake blocked roots."""
from adversarial_cases import current_choices

from itertools import permutations
import unittest
from unittest.mock import patch

from hgraph_fabric.consistent_cut import find_consistent_cut
from hgraph_fabric.data_subscription_graph import DataSubscriptionGraph
from hgraph_fabric.data_subscription_manager import DataSubscriptionManager
from adversarial_cases import History, HistoryCache, current_cut


class DependencyRefreshTests(unittest.TestCase):
    def waiting(self, roots=("a", "b"), order=None, nested=False):
        history = History()
        history.add("x", 1)
        history.add("x", 2)
        history.add("unrelated", 100)
        history.add("middle", 100, dependencies=(("x", 1),))
        if nested:
            history.add("outer", 100, dependencies=(("middle", 100),))
        for root in roots:
            history.add(root, 100)
            history.add(root, 9000, dependencies=(("outer" if nested else "middle", 100), ("unrelated", 100)))
        history.add("peer", 100, dependencies=(("x", 2),))
        cache = HistoryCache(history)
        manager = DataSubscriptionManager(cache)
        manager.add_subscriptions(order or (*roots, "peer"))
        manager.get_consistent_cut()
        self.assertEqual(current_cut(manager), {**{root: (root, 100, 0) for root in roots}, "peer": ("peer", 100, 0)})
        return history, cache, manager

    def test_conflict_path_update_advances_waiting_latest_trees_in_every_order(self):
        for nested in (False, True):
            for order in permutations(("a", "b", "peer")):
                with self.subTest(nested=nested, order=order):
                    history, cache, manager = self.waiting(order=order, nested=nested)
                    watched = {("middle", 100), ("outer", 100)} if nested else {("middle", 100)}
                    self.assertEqual(set(manager._conflicts), watched)
                    self.assertEqual(manager._dependents[("middle", 100)], {"a", "b"})
                    update = history.add("middle", 100, 1, dependencies=(("x", 2),))
                    self.assertEqual(manager.apply_update(update), {("a", 9000), ("b", 9000)})
                    self.assertTrue(history.consistent(current_cut(manager), current_choices(manager)))
                    self.assertEqual(manager._conflicts, {})
                    self.assertNotIn(("x", 1), manager._dependents)
                    self.assertEqual(manager._dependents[("x", 2)], {"a", "b", "peer"})
                    with patch("hgraph_fabric.data_subscription_manager.find_consistent_cut") as compare:
                        self.assertEqual(manager.apply_update(update), set())
                    compare.assert_not_called()

    def test_one_dependent_refreshes_without_attempting_resolution(self):
        history, cache, manager = self.waiting(roots=("a",))
        with patch("hgraph_fabric.data_subscription_manager.find_consistent_cut") as compare:
            self.assertEqual(manager.apply_update(history.add("middle", 100, 1, dependencies=(("x", 2),))), set())
        compare.assert_not_called()
        a = manager._subscriptions["a"]
        self.assertEqual(a.current_revision, ("a", 100, 0))
        self.assertTrue(a.latest(cache).has_node(("x", 2)))
        self.assertEqual(manager.get_consistent_cut(), {("a", 9000), ("peer", 100)})

    def test_subscribed_intermediate_needs_no_soft_watch(self):
        history = History()
        history.add("x", 1)
        history.add("x", 2)
        history.add("middle", 100, dependencies=(("x", 1),))
        history.add("middle", 200, dependencies=(("x", 2),))
        history.add("outer", 100, dependencies=(("middle", 100),))
        for root in ("a", "b"):
            history.add(root, 100)
            history.add(root, 200, dependencies=(("outer", 100),))
        history.add("peer", 100, dependencies=(("x", 2),))
        manager = DataSubscriptionManager(HistoryCache(history))
        manager.add_subscriptions(("a", "b", "peer", "middle"))
        manager.get_consistent_cut()
        # Continue walking past a subscribed intermediate to watch outer,
        # whose revisions can change the consumed middle version.
        self.assertEqual(set(manager._conflicts), {("outer", 100)})
        self.assertEqual(manager.apply_update(history.add(
            "outer", 100, 1, dependencies=(("middle", 200),)
        )), {("a", 200), ("b", 200)})
        self.assertTrue(history.consistent(current_cut(manager), current_choices(manager)))
        self.assertEqual(manager._conflicts, {})

    def test_all_dependents_at_latest_retain_committed_choices_without_search(self):
        history, cache, manager = self.waiting()
        manager.apply_update(history.add("middle", 100, 1, dependencies=(("x", 2),)))
        before = current_cut(manager)
        graphs = {key: sub.current(cache) for key, sub in manager._subscriptions.items()}
        with patch("hgraph_fabric.data_subscription_manager.find_consistent_cut") as compare:
            self.assertEqual(manager.apply_update(history.add("middle", 100, 2, dependencies=(("x", 2),))), set())
        compare.assert_not_called()
        self.assertEqual(current_cut(manager), before)
        self.assertIs(manager._subscriptions["a"].current(cache), graphs["a"])
        self.assertIs(manager._subscriptions["peer"].current(cache), graphs["peer"])

    def test_other_version_and_unrelated_branch_do_not_wake_conflict(self):
        history, cache, manager = self.waiting()
        with patch("hgraph_fabric.data_subscription_manager.find_consistent_cut") as compare:
            manager.apply_update(history.add("middle", 200, dependencies=(("x", 2),)))
            manager.apply_update(history.add("unrelated", 100, 1))
        compare.assert_not_called()
        self.assertEqual(manager._subscriptions["a"].current_revision, ("a", 100, 0))
        self.assertTrue(manager._subscriptions["a"].latest(cache).has_node(("x", 1)))

    def test_new_and_removed_transitive_dependencies_update_the_index(self):
        history, cache, manager = self.waiting()
        history.add("extra", 100)
        manager.apply_update(history.add("unrelated", 100, 1, dependencies=(("extra", 100),)))
        self.assertEqual(manager._dependents[("extra", 100)], {"a", "b"})
        manager.apply_update(history.add("unrelated", 100, 2))
        self.assertNotIn(("extra", 100), manager._dependents)
        with patch("hgraph_fabric.data_subscription_manager.find_consistent_cut") as compare:
            manager.apply_update(history.add("extra", 100, 1))
        compare.assert_not_called()

    def test_new_proposal_resolves_refreshed_dependencies_without_blocking_independent(self):
        history = History()
        history.add("x", 1)
        history.add("x", 2)
        history.add("middle", 100, dependencies=(("x", 1),))
        for root in ("a", "b"):
            history.add(root, 100, dependencies=(("middle", 100),))
        history.add("peer", 100, dependencies=(("x", 1),))
        history.add("independent", 100)
        cache = HistoryCache(history)
        manager = DataSubscriptionManager(cache)
        manager.add_subscriptions(("a", "b", "peer", "independent"))
        manager.get_consistent_cut()
        before = current_cut(manager)
        # Keep the committed expansion. The next proposal starts from latest
        # but can choose the older middle revision to retain X@1 agreement.
        manager.apply_update(history.add("middle", 100, 1, dependencies=(("x", 2),)))
        with patch("hgraph_fabric.data_subscription_manager.find_consistent_cut", wraps=find_consistent_cut) as compare:
            self.assertEqual(manager.apply_update(history.add("independent", 9000)), {("independent", 9000)})
        self.assertEqual(current_cut(manager), before | {"independent": ("independent", 9000, 0)})
        self.assertEqual(manager._subscriptions["a"].current_dependency_revisions[("middle", 100)], ("middle", 100, 0))
        self.assertTrue(any({args[0].data_id, args[1].data_id} == {"a", "peer"}
                            for args, _ in compare.call_args_list))
        self.assertEqual(manager.apply_update(history.add("peer", 9000, dependencies=(("x", 2),))),
                         {("peer", 9000)})
        self.assertTrue(history.consistent(current_cut(manager), current_choices(manager)))

    def test_failed_refresh_propagates_without_changing_selections(self):
        history, cache, manager = self.waiting()
        before = current_cut(manager)
        update = history.add("middle", 100, 1, dependencies=(("missing", 1), ("x", 2)))
        with self.assertRaises(KeyError):
            manager.apply_update(update)
        self.assertEqual(current_cut(manager), before)

    def test_retained_historical_tree_refreshes_without_a_selected_subscription(self):
        history = History()
        history.add("x", 1)
        history.add("x", 2)
        history.add("middle", 100, dependencies=(("x", 1),))
        history.add("a", 100, dependencies=(("middle", 100),))
        cache = HistoryCache(history)
        sub = DataSubscriptionGraph("a")
        revision = ("a", 100, 0)
        before = sub.revision(revision, cache)
        cache.apply_update(history.add("middle", 100, 1, dependencies=(("x", 2),)))
        refreshed = sub.revision(revision, cache)
        self.assertIsNot(refreshed, before)
        self.assertTrue(refreshed.has_node(("x", 2)))
        self.assertIs(sub.revision(revision, cache), refreshed)

    def test_a_still_blocked_root_does_not_prevent_another_affected_root_advancing(self):
        history = History()
        for source in ("x", "y"):
            history.add(source, 1)
            history.add(source, 2)
        history.add("middle", 100, dependencies=(("x", 1),))
        for root in ("a", "b"):
            history.add(root, 100)
            dependencies = (("middle", 100), ("y", 2)) if root == "a" else (("middle", 100),)
            history.add(root, 9000, dependencies=dependencies)
        history.add("peer", 100, dependencies=(("x", 2), ("y", 1)))
        manager = DataSubscriptionManager(HistoryCache(history))
        manager.add_subscriptions(("a", "b", "peer"))
        manager.get_consistent_cut()
        self.assertEqual(manager.apply_update(history.add("middle", 100, 1, dependencies=(("x", 2),))),
                         {("b", 9000)})
        self.assertEqual(manager._subscriptions["a"].current_revision, ("a", 100, 0))
        self.assertTrue(history.consistent(current_cut(manager), current_choices(manager)))

    def test_dependency_refresh_can_advance_counts_without_reporting_new_data(self):
        history = History()
        history.add("x", 1)
        history.add("x", 2)
        history.add("middle", 100, dependencies=(("x", 1),))
        for root in ("a", "b"):
            history.add(root, 100)
            history.add(root, 100, 1, dependencies=(("middle", 100),))
        history.add("peer", 100, dependencies=(("x", 2),))
        manager = DataSubscriptionManager(HistoryCache(history))
        manager.add_subscriptions(("a", "b", "peer"))
        manager.get_consistent_cut()
        self.assertEqual(manager.apply_update(history.add("middle", 100, 1, dependencies=(("x", 2),))), set())
        for root in ("a", "b"):
            self.assertEqual(manager._subscriptions[root].current_revision, (root, 100, 1))
        self.assertEqual(manager._conflicts, {})
