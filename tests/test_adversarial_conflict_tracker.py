"""Second review: successful refresh matrix and explicit remaining gaps.

The shared-cache case is conditional on cache writes outside the owning manager.
Other expected failures use that manager's public update path. Run the adversarial
runner with --strict to expose all five failures.
"""
from itertools import permutations, product
import unittest
from unittest.mock import patch

from hgraph_fabric.data_subscription_manager import DataSubscriptionManager
from adversarial_cases import History, HistoryCache, current_cut


def waiting(topology="direct", counts=False, order=("a", "b", "peer")):
    history = History()
    history.add("x", 1)
    history.add("x", 2)
    history.add("middle", 100, dependencies=(("x", 1),))
    if topology == "direct":
        dependencies = (("middle", 100),)
    elif topology == "nested":
        history.add("outer", 100, dependencies=(("middle", 100),))
        dependencies = (("outer", 100),)
    else:
        for side in ("left", "right"):
            history.add(side, 100, dependencies=(("middle", 100),))
        dependencies = (("left", 100), ("right", 100))
    for root in ("a", "b"):
        history.add(root, 100)
        history.add(root, 100 if counts else 9000, int(counts), dependencies=dependencies)
    history.add("peer", 100, dependencies=(("x", 2),))
    cache = HistoryCache(history)
    manager = DataSubscriptionManager(cache)
    manager.add_subscriptions(order)
    manager.get_consistent_cut()
    return history, cache, manager


class ConflictTrackerReview(unittest.TestCase):
    def test_successful_refresh_matrix_and_reverse_index(self):
        # 3 topologies * 2 root encodings * 6 registration orders * 2 updates.
        for topology, counts, order, omission in product(
            ("direct", "nested", "diamond"), (False, True),
            permutations(("a", "b", "peer")), (False, True),
        ):
            with self.subTest(topology=topology, counts=counts, order=order, omission=omission):
                history, cache, manager = waiting(topology, counts, order)
                self.assertEqual(current_cut(manager), {key: (key, 100, 0) for key in order})
                update = history.add("middle", 100, 1, dependencies=() if omission else (("x", 2),))
                self.assertEqual(manager.apply_update(update), set() if counts else {("a", 9000), ("b", 9000)})
                self.assertEqual(current_cut(manager), {key: history.heads[key] for key in order})
                self.assertTrue(history.consistent(current_cut(manager)))
                expected_index = {}
                for key, sub in manager._subscriptions.items():
                    for revision in {sub.current_revision, sub.latest_revision}:
                        for node, version in history.nodes(revision).items():
                            if node != key:
                                expected_index.setdefault((node, version), set()).add(key)
                self.assertEqual(manager._dependents, expected_index)
                self.assertEqual(manager._conflicts, {})
                with patch("hgraph_fabric.data_subscription_manager.find_consistent_cut") as compare:
                    self.assertEqual(manager.apply_update(update), set())
                compare.assert_not_called()

    def test_gate_deliberately_skips_when_only_a_non_dependent_root_is_behind(self):
        # This is the agreed gate, not reported as an implementation defect.
        history = History()
        history.add("x", 1)
        history.add("x", 2)
        history.add("middle", 100, dependencies=(("x", 1),))
        for root in ("a", "b"):
            history.add(root, 100, dependencies=(("middle", 100),))
        history.add("peer", 100, dependencies=(("x", 1),))
        history.add("peer", 200, dependencies=(("x", 2),))
        manager = DataSubscriptionManager(HistoryCache(history))
        manager.add_subscriptions(("a", "b", "peer"))
        manager.get_consistent_cut()
        before = current_cut(manager)
        self.assertEqual(before["peer"], ("peer", 100, 0))
        with patch("hgraph_fabric.data_subscription_manager.find_consistent_cut") as compare:
            self.assertEqual(manager.apply_update(history.add("middle", 100, 1, dependencies=(("x", 2),))), set())
        compare.assert_not_called()
        self.assertEqual(current_cut(manager), before)
        # Refresh has changed the trees without moving roots. The next actual
        # resolution must revalidate them; the gate does not certify this state.
        self.assertFalse(history.consistent(before))
        self.assertEqual(manager._dirty_subscriptions, {"a", "b"})

    @unittest.expectedFailure
    def test_conflict_tracker_wakes_a_dependent_downgraded_by_an_earlier_comparison(self):
        history = History()
        for version in (1, 2, 3):
            history.add("y", version)
        history.add("x", 2)
        history.add("middle", 100, dependencies=(("y", 1),))
        history.add("a", 100, dependencies=(("y", 1),))
        history.add("a", 200, dependencies=(("y", 2),))
        history.add("b", 100, dependencies=(("middle", 100), ("x", 2)))
        history.add("c", 100, dependencies=(("middle", 100),))
        history.add("c", 200, dependencies=(("x", 2),))
        history.add("c", 300, dependencies=(("y", 3),))
        manager = DataSubscriptionManager(HistoryCache(history))
        manager.add_subscriptions(("a", "b", "c"))
        manager.get_consistent_cut()
        self.assertEqual(current_cut(manager), {key: (key, 100, 0) for key in ("a", "b", "c")})
        # a/b downgrades a, recording middle -> a. Comparing c with that older
        # a then downgrades c to a tree using middle, but middle -> c is missing.
        self.assertEqual(manager._dependents[("middle", 100)], {"b", "c"})
        update = history.add("middle", 100, 1)
        witness = {"a": ("a", 200, 0), "b": ("b", 100, 0), "c": ("c", 200, 0)}
        self.assertTrue(history.consistent(witness))
        # Two dependents, c is behind, and a forward cut became possible.
        self.assertEqual(manager.apply_update(update), {("a", 200), ("c", 200)})
        self.assertEqual(current_cut(manager), witness)

    @unittest.expectedFailure
    def test_duplicate_retries_resolution_that_failed_after_refresh_completed(self):
        history, cache, manager = waiting()
        before = current_cut(manager)
        update = history.add("middle", 100, 1, dependencies=(("x", 2),))
        # Inject at the comparison boundary, after both latest trees refreshed.
        # No outcome from that attempted resolution has been committed.
        with patch("hgraph_fabric.data_subscription_manager.find_consistent_cut",
                   side_effect=OSError("Transient resolution failure")):
            with self.assertRaises(OSError):
                manager.apply_update(update)
        self.assertEqual(current_cut(manager), before)
        self.assertEqual(manager.apply_update(update), {("a", 9000), ("b", 9000)})

    @unittest.expectedFailure
    def test_reading_repaired_trees_does_not_consume_a_pending_retry(self):
        history, cache, manager = waiting()
        before = current_cut(manager)
        update = history.add("middle", 100, 1, dependencies=(("missing", 1), ("x", 2)))
        with self.assertRaises(KeyError):
            manager.apply_update(update)
        self.assertEqual(current_cut(manager), before)
        manager.apply_update(history.add("missing", 1))
        # These public read calls rebuild trees but do not resolve or commit a cut.
        for key in ("a", "b"):
            self.assertTrue(manager._subscriptions[key].latest(cache).has_node(("missing", 1)))
        self.assertEqual(manager.apply_update(update), {("a", 9000), ("b", 9000)})

    @unittest.expectedFailure
    def test_retired_versions_are_removed_while_the_roots_are_still_waiting(self):
        history, cache, manager = waiting()
        for version in range(200, 1200, 100):
            manager.apply_update(history.add("middle", version, dependencies=(("x", 1),)))
            for root in ("a", "b"):
                self.assertEqual(manager.apply_update(history.add(
                    root, 9000 + version, dependencies=(("middle", version),)
                )), set())
        self.assertEqual(current_cut(manager), {key: (key, 100, 0) for key in ("a", "b", "peer")})
        self.assertEqual({key for key in manager._dependents if key[0] == "middle"}, {("middle", 1100)})
        # Only the last version is reachable from any selected current/latest
        # tree. Older entries should not grow with every rejected publication.
        self.assertEqual({key for key in manager._conflicts if key[0] == "middle"}, {("middle", 1100)})

    @unittest.expectedFailure
    def test_shared_cache_refresh_cannot_certify_an_inconsistent_cut(self):
        # Conditional contract: two managers share the same public DataCache.
        # If every manager must receive every dependency notification, document
        # and enforce that instead of claiming this topology is supported.
        history = History()
        history.add("x", 1)
        history.add("x", 2)
        history.add("middle", 100, dependencies=(("x", 1),))
        history.add("a", 100, dependencies=(("middle", 100),))
        history.add("b", 100, dependencies=(("x", 1),))
        history.add("independent", 100)
        cache = HistoryCache(history)
        manager = DataSubscriptionManager(cache)
        manager.add_subscriptions(("a", "b", "independent"))
        manager.get_consistent_cut()
        other_manager = DataSubscriptionManager(cache)
        other_manager.apply_update(history.add("middle", 100, 1, dependencies=(("x", 2),)))
        # Lazy expansion sees a's changed tree, but the owning manager never
        # marks it dirty, so it can publish an unrelated advancement unsafely.
        self.assertEqual(manager.apply_update(history.add("independent", 200)), set())
