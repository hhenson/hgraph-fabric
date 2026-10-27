"""The four original adversarial review findings, now passing regressions."""
import unittest

from hgraph_fabric.data_subscription_manager import DataSubscriptionManager
from adversarial_cases import History, HistoryCache, current_cut


class AdversarialKnownGaps(unittest.TestCase):
    def test_crossed_input_frontiers_find_the_existing_older_cut(self):
        # Both consumers record every consumed input change. They are merely
        # behind on different inputs, which can happen with independent feeds.
        history = History()
        for source in ("x", "y"):
            history.add(source, 1)
            history.add(source, 2)
        for root in ("a", "b"):
            history.add(root, 1, dependencies=(("x", 1), ("y", 1)))
        history.add("a", 2, dependencies=(("x", 2), ("y", 1)))
        history.add("b", 2, dependencies=(("x", 1), ("y", 2)))
        witness = {root: (root, 1, 0) for root in ("a", "b")}
        self.assertTrue(history.consistent(witness))
        manager = DataSubscriptionManager(HistoryCache(history))
        manager.add_subscriptions(("a", "b"))
        self.assertEqual(manager.get_consistent_cut(), {r[:2] for r in witness.values()})

    def test_single_subscription_does_not_commit_unloadable_tree(self):
        history = History()
        history.add("a", 1)
        cache = HistoryCache(history)
        manager = DataSubscriptionManager(cache)
        manager.add_subscription("a")
        manager.get_consistent_cut()
        before = current_cut(manager)
        update = history.add("a", 2, dependencies=(("missing", 1),))
        # Multi-subscription comparisons detect this error before commit.
        # The same failure isolation is needed when there are no pair checks.
        with self.assertRaises(KeyError):
            manager.apply_update(update)
        self.assertEqual(current_cut(manager), before)

    def test_same_revision_cut_is_independent_of_retained_tree_cache(self):
        history = History()
        history.add("x", 1)
        history.add("x", 2)
        history.add("middle", 1, dependencies=(("x", 1),))
        history.add("root", 1, dependencies=(("middle", 1),))
        warm = DataSubscriptionManager(HistoryCache(history))
        warm.add_subscription("root")
        warm.get_consistent_cut()  # Retains root -> middle@1 -> x@1.
        warm.apply_update(history.add("middle", 1, 1, dependencies=(("x", 2),)))
        history.add("peer", 1, dependencies=(("x", 2),))
        warm.add_subscription("peer")
        cold = DataSubscriptionManager(HistoryCache(history))
        cold.add_subscriptions(("root", "peer"))
        expected = cold.get_consistent_cut()
        # Identical available metadata and root revision IDs, different load
        # timing. The cold tree sees middle's latest count; the warm one does not.
        self.assertEqual(warm.get_consistent_cut(), expected)

    def test_unrelated_pending_history_does_not_block_independent_progress(self):
        # A liveness gap in the proposed algorithm, not an unsafe commit:
        # exact-match preference searches below b's committed lower bound even
        # though keeping b's dependency-free current is a valid alternative.
        history = History()
        history.add("x", 1)
        history.add("x", 2)
        history.add("a", 1)
        history.add("b", 1, dependencies=(("x", 1),))
        history.add("b", 2)
        history.add("c", 1, dependencies=(("x", 1),))
        manager = DataSubscriptionManager(HistoryCache(history))
        manager.add_subscriptions(("a", "b", "c"))
        manager.get_consistent_cut()
        self.assertEqual(manager.apply_update(history.add("b", 3, dependencies=(("x", 2),))), set())
        update = history.add("a", 2)
        witness = {"a": ("a", 2, 0), "b": ("b", 2, 0), "c": ("c", 1, 0)}
        self.assertTrue(history.consistent(witness))
        self.assertEqual(manager.apply_update(update), {("a", 2)})
        self.assertEqual(current_cut(manager), witness)
