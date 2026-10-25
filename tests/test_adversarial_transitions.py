"""Long-lived public update streams checked against the raw-record model.

An unchanged cut may have dirty trees after the agreed dependency-update gate
skips resolution. A newly committed cut must always agree. Unexpected metadata
errors must propagate; fatal-error cases end there without retrying processing.
"""
from itertools import permutations, product
from random import Random
import unittest
from unittest.mock import patch

from hgraph_fabric.consistent_cut import NoConsistentCutError, find_consistent_cut
from hgraph_fabric.data_subscription_graph import DataSubscriptionGraph
from hgraph_fabric.data_subscription_manager import DataSubscriptionManager
from adversarial_cases import History, HistoryCache, current_cut, current_choices


class Stream:
    """Publish only available records; never change manager selections by hand."""

    def __init__(self, case, history, roots, shared=False):
        self.case = case
        self.history = history
        self.cache = HistoryCache(history)
        self.managers = [DataSubscriptionManager(self.cache) for _ in range(2 if shared else 1)]
        for manager in self.managers:
            manager.add_subscriptions(roots)
            manager.get_consistent_cut()
        self.check()

    def check(self):
        for manager in self.managers:
            expected = {}
            for key, sub in manager._subscriptions.items():
                dependencies = set()
                for revision in {sub.current_revision, sub.latest_revision} - {None}:
                    nodes = self.history.nodes(revision)
                    graph = sub.revision(revision, self.cache)
                    self.case.assertEqual(dict(graph.nodes()) | {key: revision[1]}, nodes)
                    dependencies.update((node, version) for node, version in nodes.items() if node != key)
                # The index also retains paths selected from older intermediate
                # revisions in the committed cut.
                selected = self.history.nodes(sub.current_revision, sub.current_dependency_revisions)
                self.case.assertEqual(dict(sub.current(self.cache).nodes()) | {key: sub.current_revision[1]}, selected)
                self.case.assertEqual(set(sub.current_dependency_revisions),
                                      {(name, version) for name, version in selected.items() if name != key})
                dependencies.update((node, version) for node, version in selected.items() if node != key)
                self.case.assertEqual(manager._subscription_dependencies[key], dependencies)
                for version in dependencies:
                    expected.setdefault(version, set()).add(key)
            self.case.assertEqual(manager._dependents, expected)
            waiting = {key for key, sub in manager._subscriptions.items()
                       if sub.current_revision != sub.latest_revision}
            for version, roots in manager._conflicts.items():
                self.case.assertIn(version, expected)
                self.case.assertTrue(roots)
                self.case.assertLessEqual(roots, waiting)
            before = {version: roots.copy() for version, roots in manager._conflicts.items()}
            manager.cleanup_conflicts()
            self.case.assertEqual(manager._conflicts, before)

    def publish(self, update):
        before = [current_cut(manager) for manager in self.managers]
        eligible = []
        for manager in self.managers:
            dependents = manager._dependents.get((update.id, update.revision[0]), set())
            eligible.append(
                update.id in manager._subscriptions
                or ((update.id, update.revision[0]) in manager._conflicts
                    and len(dependents) >= 2
                    and any(manager._subscriptions[key].current_revision !=
                            manager._subscriptions[key].latest_revision for key in dependents))
            )
        # Shared-cache delivery is coordinated. No tree access occurs between
        # notifications to the associated managers.
        self.cache.apply_update(update)
        results = [manager.apply_update(update) for manager in self.managers]
        for manager, previous, result, can_search in zip(self.managers, before, results, eligible):
            after = current_cut(manager)
            self.case.assertTrue(all(previous[key] <= after[key] <= manager._subscriptions[key].latest_revision
                                     for key in previous), (previous, after))
            self.case.assertEqual(result, {after[key][:2] for key in previous
                                           if after[key][1] > previous[key][1]})
            if after != previous:
                self.case.assertTrue(self.history.consistent(after, current_choices(manager)), (update, previous, after))
            latest = {key: sub.latest_revision for key, sub in manager._subscriptions.items()}
            driver_can_advance = (update.id not in previous or latest[update.id] > previous[update.id])
            if can_search and driver_can_advance and self.history.consistent(latest, {key: {} for key in latest}):
                self.case.assertEqual(after, latest)

            # A dependency duplicate cannot wake a search. A root already at
            # latest cannot drive progress either. A partially advanced root may
            # try again with its newer current bounds; require safety, not a
            # globally maximal first cut or unconditional root idempotence.
            duplicate_result = manager.apply_update(update)
            duplicate_cut = current_cut(manager)
            self.case.assertTrue(all(after[key] <= duplicate_cut[key] <= latest[key] for key in after))
            self.case.assertEqual(duplicate_result, {duplicate_cut[key][:2] for key in after
                                                    if duplicate_cut[key][1] > after[key][1]})
            if update.id not in after or after[update.id] == latest[update.id]:
                self.case.assertEqual(duplicate_cut, after)
            if duplicate_cut != after:
                self.case.assertTrue(self.history.consistent(duplicate_cut, current_choices(manager)))
        self.case.assertTrue(all(result == results[0] for result in results))
        self.check()
        return results[0]


def path(history, prefix, depth, shape, dependency):
    """Build a shared chain or repeated diamonds above a revision-bearing tip."""
    history.add(prefix, 100, dependencies=(dependency,))
    tip = (prefix, 100)
    for level in range(depth):
        if shape == "diamond":
            sides = []
            for side in ("left", "right"):
                name = f"{prefix}-{level}-{side}"
                history.add(name, 100, dependencies=(tip,))
                sides.append((name, 100))
            dependencies = tuple(sides)
        else:
            dependencies = (tip,)
        name = f"{prefix}-{level}"
        history.add(name, 100, dependencies=dependencies)
        tip = name, 100
    return tip


class DeepTransitionTests(unittest.TestCase):
    def test_repeated_block_release_remove_restore_on_deep_paths(self):
        # 2 shapes * 3 depths * 2 root encodings * 6 orders * 2 delivery modes.
        for shape, depth, counts, order, shared in product(
            ("chain", "diamond"), (1, 4, 12), (False, True),
            permutations(("a", "b", "peer")), (False, True),
        ):
            with self.subTest(shape=shape, depth=depth, counts=counts, order=order, shared=shared):
                history = History()
                history.add("x", 1)
                tip = path(history, "middle", depth, shape, ("x", 1))
                for root in ("a", "b", "peer"):
                    history.add(root, 100, dependencies=(("x", 1),) if root == "peer" else ())
                stream = Stream(self, history, order, shared)
                middle_count = 0
                for step, source in enumerate((7, 53, 500, 9999), 1):
                    # A new data version by itself does not change pinned trees.
                    stream.publish(history.add("x", source))
                    root_version, root_count = (100, 2 * step - 1) if counts else (100 + step * step * 137, 0)
                    stream.publish(history.add("peer", root_version, step if counts else 0,
                                               dependencies=(("x", source),)))
                    for root in ("a", "b"):
                        stream.publish(history.add(root, root_version, root_count, dependencies=(tip,)))
                    # Current remains on the previous source. Removing x from
                    # the middle releases every root without new root records.
                    middle_count += 1
                    stream.publish(history.add("middle", 100, middle_count))
                    self.assertEqual(current_cut(stream.managers[0]),
                                     {root: history.heads[root] for root in order})
                    self.assertEqual(stream.managers[0]._conflicts, {})
                    # Restore x, then publish a count-only revision of the tip.
                    # All roots already latest: refresh, with no cut search.
                    middle_count += 1
                    with patch("hgraph_fabric.data_subscription_manager.find_consistent_cut") as compare:
                        stream.publish(history.add("middle", 100, middle_count, dependencies=(("x", source),)))
                    compare.assert_not_called()
                    self.assertTrue(history.consistent(current_cut(stream.managers[0])))
                    # Retire both paths before the next round. This avoids
                    # asking a soft update to wake only a non-dependent peer.
                    for root in ("a", "b"):
                        stream.publish(history.add(root, root_version, root_count + 1))
                    self.assertEqual(stream.managers[0]._conflicts, {})

    def test_seeded_multi_source_membership_and_revision_streams(self):
        # 129 reproducible streams, each with 60 events plus a convergence tail.
        # Seed 2007 also exercises a root duplicate after partial advancement.
        for seed in (*range(128), 2007):
            with self.subTest(seed=seed):
                rng = Random(seed)
                history = History()
                sources = ("x", "y", "z")
                tips = {}
                for source in sources:
                    history.add(source, 1)
                    tips[source] = path(history, f"m-{source}", 1 + seed % 4,
                                        "diamond" if seed % 2 else "chain", (source, 1))
                roots = ["a", "b", "c", "d"]
                for root in roots:
                    history.add(root, 100, dependencies=tuple((source, 1) for source in sources))
                rng.shuffle(roots)
                stream = Stream(self, history, roots, shared=seed % 2 == 0)
                for step in range(60):
                    with self.subTest(step=step):
                        source = rng.choice(sources)
                        if step % 3 == 0:
                            version = history.heads[source][1] + rng.choice((7, 113, 1009))
                            stream.publish(history.add(source, version))
                        elif step % 3 == 1:
                            name = f"m-{source}"
                            count = history.heads[name][2] + 1
                            dependencies = () if rng.randrange(3) == 0 else (history.heads[source][:2],)
                            stream.publish(history.add(name, 100, count, dependencies=dependencies))
                        else:
                            root = rng.choice(roots)
                            _, version, count = history.heads[root]
                            if rng.randrange(2):
                                version, count = version + rng.choice((17, 211, 9001)), 0
                            else:
                                count += 1
                            dependencies = []
                            for source in sources:
                                mode = rng.randrange(3)
                                if mode:
                                    middle = history.records[history.heads[f"m-{source}"]]
                                    fresh = not middle.dependencies or middle.dependencies == (history.heads[source][:2],)
                                    dependencies.append(tips[source] if mode == 1 and fresh else history.heads[source][:2])
                            stream.publish(history.add(root, version, count, dependencies=tuple(dependencies),
                                                       weak=(("never-published", 99),)))
                # A complete final frontier must converge regardless of the
                # prior pattern of omissions, blocked roots, and dirty trees.
                for root in roots:
                    _, version, count = history.heads[root]
                    stream.publish(history.add(root, version + 100003,
                                               dependencies=tuple(history.heads[s][:2] for s in sources)))
                self.assertEqual(current_cut(stream.managers[0]), {root: history.heads[root] for root in roots})
                self.assertEqual(stream.managers[0]._conflicts, {})

    def test_conflict_watch_moves_to_new_nested_path_and_retires_it(self):
        for shared in (False, True):
            for order in permutations(("a", "b", "peer")):
                with self.subTest(shared=shared, order=order):
                    history = History()
                    history.add("x", 1)
                    history.add("x", 7)
                    history.add("m", 100, dependencies=(("x", 1),))
                    for root in ("a", "b"):
                        history.add(root, 100)
                        history.add(root, 9000, dependencies=(("m", 100),))
                    history.add("peer", 100, dependencies=(("x", 7),))
                    stream = Stream(self, history, order, shared)
                    self.assertEqual(set(stream.managers[0]._conflicts), {("m", 100)})
                    # M keeps the mismatch, but moves it down a newly introduced
                    # path. Its next notification must install an N watch.
                    stream.publish(history.add("n", 5003, dependencies=(("x", 1),)))
                    stream.publish(history.add("m", 100, 1, dependencies=(("n", 5003),)))
                    self.assertEqual(set(stream.managers[0]._conflicts), {("m", 100), ("n", 5003)})
                    self.assertEqual(stream.publish(history.add("n", 5003, 1, dependencies=(("x", 7),))),
                                     {("a", 9000), ("b", 9000)})
                    self.assertEqual(stream.managers[0]._conflicts, {})
                    # The committed graph still uses N until another cut is
                    # selected. Retire it after committing the newer M path.
                    stream.publish(history.add("m", 100, 2, dependencies=(("x", 7),)))
                    self.assertIn(("n", 5003), stream.managers[0]._dependents)
                    for manager in stream.managers:
                        manager.get_consistent_cut()
                    self.assertNotIn(("n", 5003), stream.managers[0]._dependents)
                    with patch("hgraph_fabric.data_subscription_manager.find_consistent_cut") as compare:
                        stream.publish(history.add("n", 5003, 2))
                    compare.assert_not_called()

    def test_long_count_search_matches_exhaustive_exact_then_absence_oracle(self):
        # Three sparse versions with 17, 23 and 33 consecutive counts. Test
        # ceilings/floors at version boundaries and inside versions, plus
        # isolated holes and long disappearance/reappearance runs.
        floors = (0, 1, 16, 17, 18, 39, 40, 60, 72)
        for pattern in range(4):
            history = History()
            for version in range(1, 10):
                history.add("x", version)
                dependency = ("x", version)
                for level in reversed(range(4)):
                    history.add(f"hop-{level}", version, dependencies=(dependency,))
                    dependency = (f"hop-{level}", version)
            revisions = []
            values = []
            for version, length in ((100, 17), (5003, 23), (90000001, 33)):
                for count in range(length):
                    index = len(revisions)
                    value = 1 + index // 9
                    absent = (False, index % 3 == 1, index % 13 < 7, 12 <= index < 35 or 45 <= index < 63)[pattern]
                    dependencies = () if absent else (("hop-0", value),)
                    history.add("a", version, count, dependencies=dependencies)
                    revisions.append(("a", version, count))
                    values.append(None if absent else value)
            for target, ceiling, floor, reverse in product(range(1, 10), (16, 39, 72), floors, (False, True)):
                if floor > ceiling:
                    continue
                with self.subTest(pattern=pattern, target=target, ceiling=ceiling, floor=floor, reverse=reverse):
                    # One fixed-side record keeps the oracle independent of
                    # pair traversal preference and joint-history fallback.
                    fixed = History()
                    fixed.records = history.records.copy()
                    fixed.heads = history.heads.copy()
                    fixed.add("b", 71, dependencies=(("x", target),))
                    cache = HistoryCache(fixed)
                    left, right = DataSubscriptionGraph("a"), DataSubscriptionGraph("b")
                    left.proposed_revision, right.proposed_revision = revisions[ceiling], ("b", 71, 0)
                    indices = range(ceiling, floor - 1, -1)
                    if values[ceiling] in (None, target):
                        expected = revisions[ceiling]
                    else:
                        exact = next((i for i in indices if values[i] == target), None)
                        missing = next((i for i in indices if values[i] is None), None)
                        selected = exact if exact is not None else missing
                        expected = revisions[selected] if selected is not None else None
                    pair = (right, left) if reverse else (left, right)
                    bounds = (revisions[floor], ("b", 71, 0))
                    bounds = bounds[::-1] if reverse else bounds
                    if expected is None:
                        with self.assertRaises(NoConsistentCutError):
                            find_consistent_cut(*pair, cache, lower_bounds=bounds)
                    else:
                        result = find_consistent_cut(*pair, cache, lower_bounds=bounds)
                        actual = result[::-1] if reverse else result
                        self.assertEqual(actual, (expected, ("b", 71, 0)))
                        self.assertTrue(fixed.consistent(dict(zip(("a", "b"), actual))))
                    self.assertEqual(left.proposed_revision, revisions[ceiling])
                    self.assertEqual(right.proposed_revision, ("b", 71, 0))
                    self.assertIsNone(left.current_revision)
                    self.assertIsNone(right.current_revision)

    def test_sparse_intermediate_version_rotation_preserves_pinned_trees(self):
        for counts in (False, True):
            with self.subTest(counts=counts):
                history = History()
                history.add("x", 1)
                history.add("m", 100, dependencies=(("x", 1),))
                for root in ("a", "b", "peer"):
                    history.add(root, 100, dependencies=(("x", 1),) if root == "peer" else ())
                stream = Stream(self, history, ("a", "b", "peer"))
                previous_source = 1
                old_versions = [100]
                for step, version in enumerate((137, 5003, 9001, 999983, 10000019, 90000001), 1):
                    current_graphs = {key: sub.current(stream.cache)
                                      for key, sub in stream.managers[0]._subscriptions.items()}
                    # A new M data version cannot mutate references to its
                    # preceding version, even when those trees are retained.
                    with patch("hgraph_fabric.data_subscription_manager.find_consistent_cut") as compare:
                        stream.publish(history.add("m", version, dependencies=(("x", previous_source),)))
                    compare.assert_not_called()
                    for key, graph in current_graphs.items():
                        self.assertIs(stream.managers[0]._subscriptions[key].current(stream.cache), graph)
                    for root in ("a", "b"):
                        _, root_version, count = history.heads[root]
                        stream.publish(history.add(root, root_version, count + 1))
                    source = previous_source + step * 113
                    stream.publish(history.add("x", source))
                    stream.publish(history.add("peer", 100, step, dependencies=(("x", source),)))
                    for root in ("a", "b"):
                        _, root_version, count = history.heads[root]
                        stream.publish(history.add(root, root_version if counts else version + 100000000,
                                                   count + 1 if counts else 0, dependencies=(("m", version),)))
                    self.assertEqual(set(stream.managers[0]._conflicts), {("m", version)})
                    stream.publish(history.add("m", version, 1, dependencies=(("x", source),)))
                    self.assertEqual(current_cut(stream.managers[0]),
                                     {key: history.heads[key] for key in ("a", "b", "peer")})
                    self.assertEqual(stream.managers[0]._conflicts, {})
                    old_versions.append(version)
                    previous_source = source
                # A fresh cache must discover each old version's maximum count
                # by following revision-zero links, without arithmetic on IDs.
                fresh = HistoryCache(history)
                for version in old_versions:
                    expected = max(r for r in history.records if r[:2] == ("m", version))
                    self.assertEqual(fresh.get_latest_revision(("m", version)), expected)

    def test_coordinated_managers_with_different_root_and_dependency_roles(self):
        groups = (("a", "b", "peer"), ("m", "peer"), ("a", "independent"))
        for delivery in permutations(range(3)):
            with self.subTest(delivery=delivery):
                history = History()
                history.add("x", 1)
                history.add("x", 7)
                history.add("m", 100)
                history.add("m", 100, 1, dependencies=(("x", 1),))
                for root in ("a", "b"):
                    history.add(root, 100)
                    history.add(root, 9000, dependencies=(("m", 100),))
                history.add("peer", 100, dependencies=(("x", 7),))
                history.add("independent", 100)
                cache = HistoryCache(history)
                managers = [DataSubscriptionManager(cache) for _ in groups]
                for manager, roots in zip(managers, groups):
                    manager.add_subscriptions(roots)
                    manager.get_consistent_cut()
                self.assertEqual(managers[1]._subscriptions["m"].current_revision, ("m", 100, 0))
                update = history.add("m", 100, 2, dependencies=(("x", 7),))
                cache.apply_update(update)
                results = {}
                for index in delivery:
                    results[index] = managers[index].apply_update(update)
                self.assertEqual(results, {0: set(), 1: set(), 2: set()})
                for manager, roots in zip(managers, groups):
                    self.assertEqual(current_cut(manager), {root: history.heads[root] for root in roots})
                    self.assertTrue(history.consistent(current_cut(manager), current_choices(manager)))
                    for sub in manager._subscriptions.values():
                        self.assertEqual(dict(sub.current(cache).nodes()) | {sub.data_id: sub.current_revision[1]},
                                         history.nodes(sub.current_revision, sub.current_dependency_revisions))
                # The one-dependent manager still needs to validate its dirty
                # A tree when its independent root next advances.
                update = history.add("independent", 5003)
                cache.apply_update(update)
                results = {index: managers[index].apply_update(update) for index in delivery}
                self.assertEqual(results, {0: set(), 1: set(), 2: {("independent", 5003)}})
                self.assertFalse(managers[2]._dirty_subscriptions)

    def test_deep_invalid_dependency_update_propagates_without_publishing_a_cut(self):
        for shape, missing in product(("chain", "diamond"), (False, True)):
            with self.subTest(shape=shape, missing=missing):
                history = History()
                history.add("x", 1)
                history.add("x", 7)
                tip = path(history, "m", 12, shape, ("x", 1))
                for root in ("a", "b"):
                    history.add(root, 100, dependencies=(tip,))
                stream = Stream(self, history, ("a", "b"))
                manager = stream.managers[0]
                before = current_cut(manager)
                dependencies = (("missing", 99),) if missing else (("x", 1), ("x", 7))
                update = history.add("m", 100, 1, dependencies=dependencies)
                with self.assertRaises(KeyError if missing else ValueError):
                    manager.apply_update(update)
                self.assertEqual(current_cut(manager), before)
                # Processing ends here. No retry or repair-after-error contract.

    def test_refreshed_historical_path_does_not_hide_an_exact_count_match(self):
        history = History()
        for version in (1, 3, 5, 7):
            history.add("x", version)
        history.add("m", 100, dependencies=(("x", 1),))
        history.add("a", 100)
        history.add("a", 200, 0, dependencies=(("m", 100),))
        history.add("a", 200, 1, dependencies=(("x", 3),))
        history.add("a", 200, 2, dependencies=(("x", 5),))
        history.add("b", 100, dependencies=(("x", 3),))
        cache = HistoryCache(history)
        manager = DataSubscriptionManager(cache)
        manager.add_subscriptions(("a", "b"))
        manager.get_consistent_cut()
        exact = {"a": ("a", 200, 1), "b": ("b", 100, 0)}
        self.assertEqual(current_cut(manager), exact)
        # M advances after A stopped consuming it. Delivery is coordinated;
        # there are no exceptions, missing records or decreasing source IDs.
        manager.apply_update(history.add("m", 100, 1, dependencies=(("x", 7),)))
        self.assertEqual([history.nodes(("a", 200, count))["x"] for count in range(3)], [7, 3, 5])
        self.assertTrue(history.consistent(exact))
        # A new manager must prefer the still-valid exact count 1 to omitting X
        # at A@100. The version's refreshed count 0 is no longer a lower bound
        # on its descendants' versions, so skipping all of A@200 is unsound.
        fresh = DataSubscriptionManager(cache)
        fresh.add_subscriptions(("a", "b"))
        fresh.get_consistent_cut()
        self.assertTrue(history.consistent(current_cut(fresh), current_choices(fresh)))
        self.assertEqual(current_cut(fresh), exact)

    def test_refreshed_middle_probe_does_not_prune_the_matching_half(self):
        history = History()
        for version in range(1, 10):
            history.add("x", version)
        history.add("m", 100, dependencies=(("x", 4),))
        history.add("a", 100)
        for count, source in enumerate((1, 2, 3, None, 5, 6, 9)):
            history.add("a", 200, count, dependencies=(("m", 100),) if source is None else (("x", source),))
        history.add("b", 100, dependencies=(("x", 6),))
        cache = HistoryCache(history)
        manager = DataSubscriptionManager(cache)
        manager.add_subscriptions(("a", "b"))
        manager.get_consistent_cut()
        exact = {"a": ("a", 200, 5), "b": ("b", 100, 0)}
        self.assertEqual(current_cut(manager), exact)
        manager.apply_update(history.add("m", 100, 1, dependencies=(("x", 7),)))
        self.assertEqual([history.nodes(("a", 200, count))["x"] for count in range(7)], [1, 2, 3, 7, 5, 6, 9])
        self.assertTrue(history.consistent(exact))
        # Endpoints still bracket X@6, but the refreshed middle probe X@7
        # incorrectly discards the upper half containing the exact count 5.
        fresh = DataSubscriptionManager(cache)
        fresh.add_subscriptions(("a", "b"))
        fresh.get_consistent_cut()
        self.assertTrue(history.consistent(current_cut(fresh), current_choices(fresh)))
        self.assertEqual(current_cut(fresh), exact)

    def test_refreshed_counts_choose_newest_exact_before_newest_omission(self):
        # 243 refreshed histories * 6 current floors * 2 argument orders.
        # Each intermediate advances from X@1 to X@2 or X@7. The original
        # root history is ordered; refreshing distinct paths can interleave
        # matches, leads and omissions in any order across its older counts.
        for values in product((None, 2, 7), repeat=5):
            history = History()
            for version in (1, 2, 7, 9):
                history.add("x", version)
            for count, value in enumerate(values):
                if value is not None:
                    history.add(f"m{count}", 100, dependencies=(("x", 1),))
                history.add("a", 100, count,
                            dependencies=() if value is None else ((f"m{count}", 100),))
            history.add("a", 100, 5, dependencies=(("x", 9),))
            history.add("b", 100, dependencies=(("x", 2),))
            for count, value in enumerate(values):
                if value is not None:
                    history.add(f"m{count}", 100, 1, dependencies=(("x", value),))
            for floor, reverse in product(range(6), (False, True)):
                with self.subTest(values=values, floor=floor, reverse=reverse):
                    cache = HistoryCache(history)
                    left, right = DataSubscriptionGraph("a"), DataSubscriptionGraph("b")
                    left.proposed_revision, right.proposed_revision = ("a", 100, 5), ("b", 100, 0)
                    candidates = range(4, floor - 1, -1)
                    exact = next((count for count in candidates if values[count] == 2), None)
                    omitted = next((count for count in candidates if values[count] is None), None)
                    expected = exact if exact is not None else omitted
                    pair = (right, left) if reverse else (left, right)
                    bounds = (("a", 100, floor), ("b", 100, 0))
                    if reverse:
                        bounds = bounds[::-1]
                    if expected is None:
                        with self.assertRaises(NoConsistentCutError):
                            find_consistent_cut(*pair, cache, lower_bounds=bounds)
                    else:
                        result = find_consistent_cut(*pair, cache, lower_bounds=bounds)
                        result = result[::-1] if reverse else result
                        self.assertEqual(result, (("a", 100, expected), ("b", 100, 0)))
                    loaded_counts = [revision[2] for kind, revision in cache.loads
                                     if kind == "revision" and revision[0] == "a"]
                    # The zero/floor probe bounds the search even when an exact
                    # match is newer. Metadata indexing must never cross current.
                    self.assertEqual(min(loaded_counts), floor)
