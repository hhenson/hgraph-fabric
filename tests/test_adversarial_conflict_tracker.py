"""Conflict-path refresh, cleanup and coordinated delivery regressions."""
from adversarial_cases import current_choices

from itertools import permutations, product
import unittest
from unittest.mock import patch

from hgraph_fabric.data_subscription_manager import DataSubscriptionManager
from adversarial_cases import History, HistoryCache, current_cut


def waiting(topology="direct", counts=False, order=("a", "b", "peer")):
    history = History()
    history.add("x", 1)
    history.add("x", 2)
    history.add("middle", 1, dependencies=(("x", 1),))
    if topology == "direct":
        dependencies = (("middle", 1),)
    elif topology == "nested":
        history.add("outer", 1, dependencies=(("middle", 1),))
        dependencies = (("outer", 1),)
    else:
        for side in ("left", "right"):
            history.add(side, 1, dependencies=(("middle", 1),))
        dependencies = (("left", 1), ("right", 1))
    for root in ("a", "b"):
        history.add(root, 1)
        history.add(root, 1 if counts else 2, int(counts), dependencies=dependencies)
    history.add("peer", 1, dependencies=(("x", 2),))
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
                self.assertEqual(current_cut(manager), {key: (key, 1, 0) for key in order})
                watched = {("middle", 1)}
                if topology == "nested":
                    watched.add(("outer", 1))
                elif topology == "diamond":
                    watched.update((("left", 1), ("right", 1)))
                self.assertEqual(set(manager._conflicts), watched)
                update = history.add("middle", 1, 1, dependencies=() if omission else (("x", 2),))
                self.assertEqual(manager.apply_update(update), set() if counts else {("a", 2), ("b", 2)})
                self.assertEqual(current_cut(manager), {key: history.heads[key] for key in order})
                self.assertTrue(history.consistent(current_cut(manager), current_choices(manager)))
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
        history.add("middle", 1, dependencies=(("x", 1),))
        for root in ("a", "b"):
            history.add(root, 1, dependencies=(("middle", 1),))
        history.add("peer", 1, dependencies=(("x", 1),))
        history.add("peer", 2, dependencies=(("x", 2),))
        manager = DataSubscriptionManager(HistoryCache(history))
        manager.add_subscriptions(("a", "b", "peer"))
        manager.get_consistent_cut()
        before = current_cut(manager)
        self.assertEqual(before["peer"], ("peer", 1, 0))
        with patch("hgraph_fabric.data_subscription_manager.find_consistent_cut") as compare:
            self.assertEqual(manager.apply_update(history.add("middle", 1, 1, dependencies=(("x", 2),))), set())
        compare.assert_not_called()
        self.assertEqual(current_cut(manager), before)
        # Latest expansions disagree, while the committed intermediate choices
        # remain coherent until another complete proposal is accepted.
        self.assertFalse(history.consistent(before, {key: {} for key in before}))
        self.assertTrue(history.consistent(before, {key: sub.current_dependency_revisions
                                                  for key, sub in manager._subscriptions.items()}))
        self.assertEqual(manager._dirty_subscriptions, {"a", "b"})

    def test_conflict_tracker_wakes_a_dependent_downgraded_by_an_earlier_comparison(self):
        history = History()
        for version in (1, 2, 3):
            history.add("y", version)
        history.add("middle", 1, dependencies=(("y", 1),))
        history.add("a", 1, dependencies=(("y", 1),))
        history.add("a", 2, dependencies=(("y", 2),))
        history.add("b", 1, dependencies=(("middle", 1),))
        history.add("c", 1, dependencies=(("middle", 1),))
        history.add("c", 2)
        history.add("c", 3, dependencies=(("y", 3),))
        manager = DataSubscriptionManager(HistoryCache(history))
        manager.add_subscriptions(("a", "b", "c"))
        manager.get_consistent_cut()
        self.assertEqual(current_cut(manager), {key: (key, 1, 0) for key in ("a", "b", "c")})
        # Full trees: a -> y@2 and b -> middle -> y@1. This downgrades a.
        # Watch middle's revisions, not y's: only a consumer can change which
        # y version is referenced. a and b already have root subscriptions.
        self.assertEqual(manager._conflicts, {("middle", 1): {"a"}})
        # Comparing c with the older a also downgrades c onto middle. The
        # dependent index, rather than the original pair, supplies this waiter.
        self.assertEqual(manager._dependents[("middle", 1)], {"b", "c"})
        update = history.add("middle", 1, 1)
        witness = {"a": ("a", 2, 0), "b": ("b", 1, 0), "c": ("c", 2, 0)}
        self.assertTrue(history.consistent(witness))
        # Two dependents, c is behind, and a forward cut became possible.
        self.assertEqual(manager.apply_update(update), {("a", 2), ("c", 2)})
        self.assertEqual(current_cut(manager), witness)

    def test_unexpected_resolution_error_propagates(self):
        history, cache, manager = waiting()
        before = current_cut(manager)
        update = history.add("middle", 1, 1, dependencies=(("x", 2),))
        # Unexpected failures terminate processing; no recovery is promised.
        with patch("hgraph_fabric.data_subscription_manager.find_consistent_cut",
                   side_effect=OSError("Resolution failure")):
            with self.assertRaises(OSError):
                manager.apply_update(update)
        self.assertEqual(current_cut(manager), before)

    def test_missing_dependency_metadata_propagates(self):
        history, cache, manager = waiting()
        before = current_cut(manager)
        update = history.add("middle", 1, 1, dependencies=(("missing", 1), ("x", 2)))
        with self.assertRaises(KeyError):
            manager.apply_update(update)
        self.assertEqual(current_cut(manager), before)

    def test_admissible_historical_versions_remain_watched_while_roots_wait(self):
        history, cache, manager = waiting()
        for version in range(2, 12):
            manager.apply_update(history.add("middle", version, dependencies=(("x", 1),)))
            for root in ("a", "b"):
                self.assertEqual(manager.apply_update(history.add(
                    root, 1 + version, dependencies=(("middle", version),)
                )), set())
        self.assertEqual(current_cut(manager), {key: (key, 1, 0) for key in ("a", "b", "peer")})
        self.assertEqual({key for key in manager._dependents if key[0] == "middle"}, {("middle", 11)})
        # Rejected root candidates still refer to these older versions. Each
        # can receive a revision and permit a forward cut, even after rotation.
        self.assertEqual({key for key in manager._conflicts if key[0] == "middle"},
                         {("middle", version) for version in range(1, 12)})

    def test_coordinated_shared_cache_updates_revalidate_each_manager(self):
        history = History()
        history.add("x", 1)
        history.add("x", 2)
        history.add("middle", 1, dependencies=(("x", 1),))
        history.add("a", 1, dependencies=(("middle", 1),))
        history.add("b", 1, dependencies=(("x", 1),))
        history.add("independent", 1)
        cache = HistoryCache(history)
        managers = [DataSubscriptionManager(cache), DataSubscriptionManager(cache)]
        for manager in managers:
            manager.add_subscriptions(("a", "b", "independent"))
            manager.get_consistent_cut()

        def deliver(update):
            # Cache and every associated manager form one coordinated update.
            cache.apply_update(update)
            return [manager.apply_update(update) for manager in managers]

        self.assertEqual(deliver(history.add("middle", 1, 1, dependencies=(("x", 2),))), [set(), set()])
        self.assertEqual(deliver(history.add("independent", 2)), [{("independent", 2)}] * 2)
        for manager in managers:
            self.assertEqual(manager._subscriptions["independent"].current_revision, ("independent", 2, 0))
            self.assertEqual(manager._subscriptions["a"].current_dependency_revisions[("middle", 1)], ("middle", 1, 0))
        self.assertEqual(deliver(history.add("b", 2, dependencies=(("x", 2),))),
                         [{("b", 2)}] * 2)
        for manager in managers:
            self.assertTrue(history.consistent(current_cut(manager), current_choices(manager)))

    def test_explicit_cleanup_is_idempotent_and_keeps_active_waiters(self):
        history, cache, manager = waiting()
        before_cut = current_cut(manager)
        before_conflicts = {version: roots.copy() for version, roots in manager._conflicts.items()}
        self.assertIn(("middle", 1), before_conflicts)
        self.assertEqual(before_conflicts[("middle", 1)], {"a", "b"})
        manager.cleanup_conflicts()
        manager.cleanup_conflicts()
        self.assertEqual(manager._conflicts, before_conflicts)
        self.assertEqual(current_cut(manager), before_cut)
        self.assertEqual(manager.apply_update(history.add("middle", 1, 1, dependencies=(("x", 2),))),
                         {("a", 2), ("b", 2)})
        self.assertEqual(manager._conflicts, {})

    def test_cleanup_keeps_dependencies_used_only_by_current(self):
        history = History()
        history.add("x", 1)
        history.add("x", 2)
        history.add("middle", 1, dependencies=(("x", 1),))
        for root in ("a", "b"):
            history.add(root, 1, dependencies=(("middle", 1),))
        history.add("peer", 1, dependencies=(("x", 1),))
        cache = HistoryCache(history)
        manager = DataSubscriptionManager(cache)
        manager.add_subscriptions(("a", "b", "peer"))
        manager.get_consistent_cut()
        for root in ("a", "b"):
            self.assertEqual(manager.apply_update(history.add(root, 2, dependencies=(("x", 2),))), set())
            self.assertFalse(manager._subscriptions[root].latest(cache).has_node(("middle", 1)))
        manager.cleanup_conflicts()
        self.assertEqual(manager._dependents[("middle", 1)], {"a", "b"})
        self.assertIn(("middle", 1), manager._conflicts)
        self.assertEqual(current_cut(manager), {key: (key, 1, 0) for key in ("a", "b", "peer")})
