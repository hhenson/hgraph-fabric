"""Deterministic adversarial matrix: topology, histories, order and live updates."""
from adversarial_cases import current_choices

from itertools import permutations, product
from random import Random
import unittest

from hgraph_fabric.data_subscription_manager import DataSubscriptionManager
from adversarial_cases import History, HistoryCache, current_cut, dag_edges, dag_dependencies, dag_history


class AdversarialStabilizationTests(unittest.TestCase):
    def test_all_forward_four_node_dags_with_every_subscription_subset_and_head(self):
        # 64 topologies * (3**4 - 1) selected-root/head configurations = 5,120.
        for mask in range(64):
            history = dag_history(mask)
            for choices in product((None, 100, 9000), repeat=4):
                roots = [f"n{i}" for i, version in enumerate(choices) if version is not None]
                if not roots:
                    continue
                with self.subTest(topology=mask, heads=choices):
                    cache = HistoryCache(history)
                    manager = DataSubscriptionManager(cache)
                    manager.add_subscriptions(roots)
                    ceilings = {f"n{i}": (f"n{i}", version, 0) for i, version in enumerate(choices) if version}
                    for key, sub in manager._subscriptions.items():
                        sub.latest_revision = ceilings[key]
                    result = manager.get_consistent_cut()
                    cut = current_cut(manager)
                    self.assertTrue(history.consistent(cut), cut)
                    self.assertTrue(all(cut[key] <= ceilings[key] for key in roots))
                    self.assertEqual(result, {revision[:2] for revision in cut.values()})
                    # The independent exhaustive oracle also proves that the
                    # selected cut is a member of the admissible finite space.
                    self.assertIn(cut, list(history.cuts(ceilings)))

    def test_all_topological_publication_orders_on_four_node_dags(self):
        # Each dependency is published before its consumers. Every valid order
        # is exercised; no missing-history errors are excused by this matrix.
        for mask in range(64):
            edges = dag_edges(mask)
            for order in permutations(range(4)):
                if any(order.index(target) > order.index(source) for source, target in edges):
                    continue
                for selected in range(1, 16):
                    subscribed = tuple(f"n{i}" for i in order if selected & (1 << i))
                    with self.subTest(topology=mask, publication_order=order, subscriptions=subscribed):
                        history = History()
                        for node in reversed(range(4)):
                            history.add(f"n{node}", 100, dependencies=dag_dependencies(edges, node, 100))
                        cache = HistoryCache(history)
                        manager = DataSubscriptionManager(cache)
                        manager.add_subscriptions(subscribed)
                        manager.get_consistent_cut()
                        for node in order:
                            before = current_cut(manager)
                            update = history.add(f"n{node}", 9000, dependencies=dag_dependencies(edges, node, 9000))
                            result = manager.apply_update(update)
                            after = current_cut(manager)
                            self.assertTrue(history.consistent(after), after)
                            self.assertTrue(all(after[key] >= before[key] for key in before))
                            self.assertEqual(result, {after[key][:2] for key in before if after[key][1] > before[key][1]})
                            if update.id not in before or after[update.id] == before[update.id]:
                                self.assertEqual(after, before)
                            # Duplicate delivery must neither advance a waiting peer
                            # nor change any current selection.
                            self.assertEqual(manager.apply_update(update), set())
                            self.assertEqual(current_cut(manager), after)
                        self.assertEqual({r[1] for r in current_cut(manager).values()}, {9000})

    def test_seeded_membership_histories_in_every_registration_order(self):
        # Randomness is only a deterministic recipe generator. Every seed is
        # reported on failure; each case has a known compatible oldest cut.
        for seed in range(512):
            rng = Random(seed)
            history = History()
            for source in ("x", "y", "z"):
                for version in (1, 2, 3, 4):
                    history.add(source, version)
            for root in ("a", "b", "c"):
                for step, (version, count) in enumerate(((100, 0), (100, 1), (9000, 0), (9000, 1)), 1):
                    dependencies = tuple((source, step) for source in ("x", "y", "z") if rng.randrange(2))
                    history.add(root, version, count, dependencies, weak=(("unavailable-weak", 99),))
            for order in permutations(("a", "b", "c")):
                with self.subTest(seed=seed, registration_order=order):
                    manager = DataSubscriptionManager(HistoryCache(history))
                    manager.add_subscriptions(order)
                    manager.get_consistent_cut()
                    self.assertTrue(history.consistent(current_cut(manager), current_choices(manager)))
                    self.assertIn(current_cut(manager), list(history.cuts({key: history.heads[key] for key in order})))

    def test_wide_and_deep_shared_dependency_graphs(self):
        for shape in ("chain", "star", "diamond", "disconnected", "cycle"):
            for size in (2, 8, 32):
                with self.subTest(shape=shape, size=size):
                    history = History()
                    for i in reversed(range(size)):
                        targets = {
                            "chain": [i + 1] if i + 1 < size else [],
                            "star": list(range(1, size)) if i == 0 else [],
                            "diamond": ([1, 2] if i == 0 and size > 2 else [size - 1] if 0 < i < size - 1 else []),
                            "disconnected": [],
                            "cycle": [(i + 1) % size],
                        }[shape]
                        history.add(f"n{i}", 100, dependencies=tuple((f"n{j}", 100) for j in targets))
                    manager = DataSubscriptionManager(HistoryCache(history))
                    manager.add_subscriptions(f"n{i}" for i in range(size))
                    self.assertEqual(len(manager.get_consistent_cut()), size)
                    self.assertTrue(history.consistent(current_cut(manager), current_choices(manager)))

    def test_metadata_only_updates_and_shared_source_subscriptions(self):
        for subscribe_source in (False, True):
            for order in permutations(("a", "b", "c")):
                with self.subTest(source_subscribed=subscribe_source, update_order=order):
                    history = History()
                    history.add("source", 1)
                    for root in order:
                        history.add(root, 100, dependencies=(("source", 1),))
                    cache = HistoryCache(history)
                    manager = DataSubscriptionManager(cache)
                    manager.add_subscriptions((*order, "source") if subscribe_source else order)
                    manager.get_consistent_cut()
                    for count, source_version in enumerate((7, 500, 9999), 1):
                        updates = [history.add("source", source_version)]
                        # Publish metadata as it arrives, so a root's current
                        # count advances without changing its dataframe version.
                        for root in (None, *order):
                            update = updates[0] if root is None else history.add(
                                root, 100, count, dependencies=(("source", source_version),)
                            )
                            before = current_cut(manager)
                            result = manager.apply_update(update)
                            after = current_cut(manager)
                            self.assertTrue(history.consistent(after))
                            self.assertTrue(all(after[key] >= before[key] for key in before))
                            self.assertEqual(result, {after[key][:2] for key in before if after[key][1] > before[key][1]})
                        for root in order:
                            self.assertEqual(manager._subscriptions[root].current_revision, (root, 100, count))

    def test_all_registration_orders_for_every_dag_with_mixed_heads(self):
        # Order changes alone must never turn a sound result into a bad cut.
        # We check validity, not identical answers: absence preference permits
        # multiple legitimate cuts with different registration orders.
        for mask in range(64):
            history = dag_history(mask)
            for versions in product((100, 9000), repeat=4):
                for order in permutations(range(4)):
                    with self.subTest(topology=mask, heads=versions, registration_order=order):
                        manager = DataSubscriptionManager(HistoryCache(history))
                        manager.add_subscriptions(f"n{i}" for i in order)
                        for i, version in enumerate(versions):
                            manager._subscriptions[f"n{i}"].latest_revision = (f"n{i}", version, 0)
                        manager.get_consistent_cut()
                        cut = current_cut(manager)
                        self.assertTrue(history.consistent(cut), cut)
                        self.assertTrue(all(cut[f"n{i}"][1] <= versions[i] for i in range(4)))

    def test_initialization_metadata_failure_at_every_read_preserves_current(self):
        from unittest.mock import patch

        def fixture():
            history = History()
            for version in (1, 2, 3):
                history.add("source", version)
            for version, dependency in ((100, 1), (700, 2), (9000, 3)):
                history.add("a", version, dependencies=(("source", dependency),))
            history.add("b", 100, dependencies=(("source", 1),))
            cache = HistoryCache(history)
            manager = DataSubscriptionManager(cache)
            manager.add_subscriptions(("a", "b"))
            for key, sub in manager._subscriptions.items():
                sub.current_revision = (key, 100, 0)
                sub.current(cache)
            return history, cache, manager

        _, cache, manager = fixture()
        with patch.object(cache, "get_revision", wraps=cache.get_revision) as reads:
            manager.get_consistent_cut()
        self.assertGreater(reads.call_count, 0)
        for fail_at in range(1, reads.call_count + 1):
            with self.subTest(metadata_read=fail_at):
                history, cache, manager = fixture()
                before = current_cut(manager)
                graphs = {key: sub.current(cache) for key, sub in manager._subscriptions.items()}
                original = cache.get_revision
                count = 0

                def read(revision):
                    nonlocal count
                    count += 1
                    if count == fail_at:
                        raise OSError("Injected metadata read failure")
                    return original(revision)

                with patch.object(cache, "get_revision", side_effect=read):
                    with self.assertRaises(OSError):
                        manager.get_consistent_cut()
                self.assertEqual(current_cut(manager), before)
                for key, sub in manager._subscriptions.items():
                    self.assertIs(sub.current(cache), graphs[key])
                manager.get_consistent_cut()
                self.assertTrue(history.consistent(current_cut(manager), current_choices(manager)))
