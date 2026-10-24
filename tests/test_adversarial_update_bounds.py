"""Committed currents bound update searches, including omission fallbacks."""
from adversarial_cases import current_choices

from itertools import permutations, product
import unittest

from hgraph_fabric.consistent_cut import NoConsistentCutError, find_consistent_cut
from hgraph_fabric.data_subscription_manager import DataSubscriptionManager
from adversarial_cases import History, HistoryCache, current_cut


class UpdateBoundTests(unittest.TestCase):
    def test_independent_progress_with_sparse_versions_or_count_floor_in_every_order(self):
        for order, counts in product(permutations(("a", "b", "c")), (False, True)):
            with self.subTest(order=order, counts=counts):
                history = History()
                history.add("x", 1)
                history.add("x", 2)
                history.add("a", 100)
                history.add("c", 100, dependencies=(("x", 1),))
                old, current, latest = ((9000, 0), (9000, 1), (9000, 2)) if counts else (
                    (100, 0), (9000, 0), (50000, 0)
                )
                history.add("b", *old, dependencies=(("x", 1),))
                history.add("b", *current)
                manager = DataSubscriptionManager(HistoryCache(history))
                manager.add_subscriptions(order)
                manager.get_consistent_cut()
                self.assertEqual(manager.apply_update(history.add("b", *latest, dependencies=(("x", 2),))), set())
                self.assertEqual(manager.apply_update(history.add("a", 200)), {("a", 200)})
                self.assertEqual(current_cut(manager), {
                    "a": ("a", 200, 0), "b": ("b", *current), "c": ("c", 100, 0),
                })
                self.assertTrue(history.consistent(current_cut(manager), current_choices(manager)))

    def test_floor_count_excludes_older_exact_match_without_loading_it(self):
        history = History()
        history.add("x", 1)
        history.add("x", 2)
        history.add("a", 9000, 0, dependencies=(("x", 1),))
        history.add("a", 9000, 1)  # Committed absence wins over excluded count 0.
        history.add("a", 9000, 2, dependencies=(("x", 2),))
        history.add("b", 100, dependencies=(("x", 1),))
        cache = HistoryCache(history)
        manager = DataSubscriptionManager(cache)
        manager.add_subscriptions(("a", "b"))
        a, b = manager._subscriptions.values()
        for sub in (a, b):
            sub.proposed_revision = sub.latest_revision
        bounds = (("a", 9000, 1), ("b", 100, 0))
        self.assertEqual(find_consistent_cut(a, b, cache, lower_bounds=bounds), bounds)
        self.assertNotIn(("revision", ("a", 9000, 0)), cache.loads)
        # Initialization is deliberately unbounded and still prefers the exact match.
        self.assertEqual(find_consistent_cut(a, b, cache), (("a", 9000, 0), ("b", 100, 0)))

    def test_containment_fallback_keeps_root_at_current_and_drops_consumers_dependency(self):
        history = History()
        history.add("a", 100)
        history.add("a", 9000)
        history.add("b", 100)
        history.add("b", 9000, dependencies=(("a", 100),))
        manager = DataSubscriptionManager(HistoryCache(history))
        manager.add_subscriptions(("a", "b"))
        a, b = manager._subscriptions.values()
        for sub in (a, b):
            sub.proposed_revision = sub.latest_revision
        bounds = (("a", 9000, 0), ("b", 100, 0))
        for left, right, floors in ((a, b, bounds), (b, a, bounds[::-1])):
            self.assertEqual(find_consistent_cut(left, right, manager._cache, lower_bounds=floors), floors)
        with self.assertRaises(NoConsistentCutError):
            find_consistent_cut(a, b, manager._cache, lower_bounds=(bounds[0], ("b", 9000, 0)))

    def test_joint_fallback_cannot_cross_either_floor(self):
        history = History()
        for source in ("x", "y"):
            history.add(source, 1)
            history.add(source, 2)
        for root in ("a", "b"):
            history.add(root, 100, dependencies=(("x", 1), ("y", 1)))
        history.add("a", 9000, dependencies=(("x", 2), ("y", 1)))
        history.add("b", 9000, dependencies=(("x", 1), ("y", 2)))
        manager = DataSubscriptionManager(HistoryCache(history))
        manager.add_subscriptions(("a", "b"))
        a, b = manager._subscriptions.values()
        for sub in (a, b):
            sub.proposed_revision = sub.latest_revision
        for a_floor, b_floor in product((100, 9000), repeat=2):
            bounds = (("a", a_floor, 0), ("b", b_floor, 0))
            for left, right, floors in ((a, b, bounds), (b, a, bounds[::-1])):
                with self.subTest(floors=floors):
                    if a_floor == b_floor == 100:
                        self.assertEqual(find_consistent_cut(left, right, manager._cache, lower_bounds=floors), floors)
                    else:
                        with self.assertRaises(NoConsistentCutError):
                            find_consistent_cut(left, right, manager._cache, lower_bounds=floors)

    def test_single_subscription_builds_full_tree_before_committing(self):
        history = History()
        history.add("x", 1)
        history.add("middle", 100, dependencies=(("x", 1),))
        history.add("a", 100)
        cache = HistoryCache(history)
        manager = DataSubscriptionManager(cache)
        a = manager.add_subscription("a")
        manager.get_consistent_cut()
        self.assertEqual(manager.apply_update(history.add("a", 9000, dependencies=(("middle", 100),))), {("a", 9000)})
        # Inspect the retained graph without causing a lazy load in this assertion.
        self.assertEqual(set(a._revision_graphs[a.current_revision]), {
            (("a", 9000), ("middle", 100)), (("middle", 100), ("x", 1)),
        })
        self.assertIn(("revision", ("x", 1, 0)), cache.loads)

    def test_single_subscription_conflicting_tree_preserves_current(self):
        history = History()
        history.add("x", 1)
        history.add("x", 2)
        history.add("middle", 100, dependencies=(("x", 1),))
        history.add("a", 100)
        manager = DataSubscriptionManager(HistoryCache(history))
        a = manager.add_subscription("a")
        manager.get_consistent_cut()
        graph = a.current(manager._cache)
        self.assertEqual(manager.apply_update(
            history.add("a", 9000, dependencies=(("middle", 100), ("x", 2)))
        ), set())
        self.assertEqual(a.current_revision, ("a", 100, 0))
        self.assertIs(a.current(manager._cache), graph)
        self.assertNotIn(("a", 9000, 0), a._revision_graphs)
        self.assertEqual(manager.apply_update(
            history.add("middle", 100, 1, dependencies=(("x", 2),))
        ), {("a", 9000)})
        self.assertTrue(history.consistent(current_cut(manager), current_choices(manager)))
