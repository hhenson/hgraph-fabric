"""The exceptional joint-history fallback, including explicit search ceilings."""
from itertools import permutations, product
import unittest
from unittest.mock import patch

from hgraph_fabric.consistent_cut import find_consistent_cut
from hgraph_fabric.data_subscription_manager import DataSubscriptionManager
from adversarial_cases import History, HistoryCache, current_cut


class CrossedHistoryTests(unittest.TestCase):
    def manager(self, history, order=("a", "b")):
        manager = DataSubscriptionManager(HistoryCache(history))
        manager.add_subscriptions(order)
        return manager

    def sources(self):
        history = History()
        for source in ("x", "y"):
            for version in (1, 2, 3):
                history.add(source, version)
        return history

    def test_all_monotonic_two_input_paths_and_proposed_ceilings(self):
        # Six complete paths through the 3x3 input-version lattice. Every input
        # step is recorded. Check all pairs, intermediate ceilings, both argument
        # orders, and sparse-version versus within-version-count histories.
        paths = sorted(set(permutations(("x", "x", "y", "y"))))
        sparse = (100, 700, 9000, 50000, 100000)
        for a_path, b_path, counts in product(paths, paths, (False, True)):
            history = self.sources()
            revisions = {}
            for root, path in (("a", a_path), ("b", b_path)):
                values = {"x": 1, "y": 1}
                revisions[root] = []
                for step, changed in enumerate((None, *path)):
                    if changed:
                        values[changed] += 1
                    version, count = (9000, step) if counts else (sparse[step], 0)
                    history.add(root, version, count, tuple(values.items()))
                    revisions[root].append((root, version, count))
            for a_head, b_head, order in product(range(5), range(5), (("a", "b"), ("b", "a"))):
                with self.subTest(a_path=a_path, b_path=b_path, counts=counts, ceilings=(a_head, b_head), order=order):
                    manager = self.manager(history, order)
                    ceilings = {"a": revisions["a"][a_head], "b": revisions["b"][b_head]}
                    for key, sub in manager._subscriptions.items():
                        sub.latest_revision = ceilings[key]
                    manager.get_consistent_cut()
                    cut = current_cut(manager)
                    self.assertTrue(all(cut[key] <= ceilings[key] for key in cut))
                    self.assertIn(cut, list(history.cuts(ceilings)))

    def test_fallback_keeps_newest_matching_counts_and_does_not_mutate_selections(self):
        history = self.sources()
        for root, final in (("a", (("x", 2), ("y", 1))), ("b", (("x", 1), ("y", 2)))):
            for count, dependencies in enumerate(((("x", 1), ("y", 1)), (("x", 1), ("y", 1)), final)):
                history.add(root, 9000, count, dependencies)
        manager = self.manager(history)
        a, b = manager._subscriptions.values()
        for sub in (a, b):
            sub.proposed_revision = sub.latest_revision
            sub.current_revision = (sub.data_id, 9000, 0)
        self.assertEqual(find_consistent_cut(a, b, manager._cache), (("a", 9000, 1), ("b", 9000, 1)))
        self.assertEqual(find_consistent_cut(b, a, manager._cache), (("b", 9000, 1), ("a", 9000, 1)))
        for sub in (a, b):
            self.assertEqual(sub.proposed_revision, (sub.data_id, 9000, 2))
            self.assertEqual(sub.current_revision, (sub.data_id, 9000, 0))

    def test_joint_exact_match_precedes_newer_dependency_omission(self):
        history = self.sources()
        for root in ("a", "b"):
            history.add(root, 100, dependencies=(("x", 1), ("y", 1)))
        history.add("a", 9000, dependencies=(("x", 2), ("y", 1)))
        history.add("b", 700, dependencies=(("y", 1),))
        history.add("b", 9000, dependencies=(("x", 1), ("y", 2)))
        manager = self.manager(history)
        # b@700 would agree with a@9000 by omitting x. Prefer the older pair
        # that retains both original common dependencies, as in the normal path.
        self.assertEqual(manager.get_consistent_cut(), {("a", 100), ("b", 100)})

    def test_historical_containment_is_rechecked_for_candidate_pairs(self):
        history = self.sources()
        history.add("b", 100, dependencies=(("x", 1), ("y", 1)))
        history.add("a", 100, dependencies=(("b", 100),))
        history.add("a", 9000, dependencies=(("x", 2), ("y", 1)))
        history.add("b", 9000, dependencies=(("x", 1), ("y", 2)))
        manager = self.manager(history)
        self.assertEqual(manager.get_consistent_cut(), {("a", 100), ("b", 100)})

    def test_missing_fallback_history_propagates_without_committing(self):
        history = self.sources()
        for root in ("a", "b"):
            history.add(root, 100, dependencies=(("x", 1), ("y", 1)))
        history.add("a", 9000, dependencies=(("x", 2), ("y", 1)))
        history.add("b", 9000, dependencies=(("x", 1), ("y", 2)))
        del history.records[("b", 100, 0)]
        manager = self.manager(history)
        with self.assertRaises(KeyError):
            manager.get_consistent_cut()
        self.assertEqual(current_cut(manager), {"a": None, "b": None})

    def test_ordinary_search_never_enters_the_fallback(self):
        history = self.sources()
        for version, x in ((100, 1), (9000, 2)):
            history.add("a", version, dependencies=(("x", x),))
        history.add("b", 100, dependencies=(("x", 1),))
        manager = self.manager(history)
        with patch("hgraph_fabric.consistent_cut._search_both_histories", side_effect=AssertionError("Unexpected fallback")):
            self.assertEqual(manager.get_consistent_cut(), {("a", 100), ("b", 100)})

    def test_crossed_updates_keep_current_then_advance_when_both_catch_up(self):
        history = self.sources()
        for root in ("a", "b"):
            history.add(root, 100, dependencies=(("x", 1), ("y", 1)))
        manager = self.manager(history)
        manager.get_consistent_cut()
        before = current_cut(manager)
        for root, dependencies in (("a", (("x", 2), ("y", 1))), ("b", (("x", 1), ("y", 2)))):
            self.assertEqual(manager.apply_update(history.add(root, 9000, dependencies=dependencies)), set())
            self.assertEqual(current_cut(manager), before)
        self.assertEqual(manager.apply_update(history.add("a", 50000, dependencies=(("x", 2), ("y", 2)))), set())
        self.assertEqual(manager.apply_update(history.add("b", 50000, dependencies=(("x", 2), ("y", 2)))), {("a", 50000), ("b", 50000)})
        self.assertTrue(history.consistent(current_cut(manager)))
