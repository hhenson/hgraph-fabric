"""Changing containment must change which nodes constrain a candidate cut.

Known gaps are expected failures so ordinary CI can retain the reproductions.
Run run_adversarial.py --strict to expose them as failures. Matrix tests collect
all mismatches before asserting, so an early failure does not truncate coverage.
"""
from itertools import combinations, permutations, product
import unittest

from adversarial_cases import History, HistoryCache, current_cut
from hgraph_fabric.consistent_cut import NoConsistentCutError, find_consistent_cut
from hgraph_fabric.data_subscription_manager import DataSubscriptionManager


def containment_history(states, counts=False, constrain_a=False):
    history = History()
    for version in (1, 2, 3):
        history.add("x", version)
    for version in (1, 2):
        if constrain_a:
            history.add("y", version)
    history.add("a", 100, dependencies=(("x", 1),) + ((("y", 1),) if constrain_a else ()))
    history.add("a", 9000, dependencies=(("x", 3),) + ((("y", 2),) if constrain_a else ()))
    dependencies = {
        "absent": (), "x1": (("x", 1),), "x2": (("x", 2),),
        "x3": (("x", 3),), "a1": (("a", 100),), "a3": (("a", 9000),),
    }
    for index, state in enumerate(states):
        version, count = (50000, index) if counts else ((100, 200, 9000, 50000)[index], 0)
        history.add("b", version, count, dependencies[state])
    return history


def pair(history, floor, reverse=False):
    cache = HistoryCache(history)
    manager = DataSubscriptionManager(cache)
    manager.add_subscriptions(("a", "b"))
    a, b = manager._subscriptions.values()
    a.proposed_revision = ("a", 100, 0)  # An earlier comparison lowered A.
    b.proposed_revision = b.latest_revision
    bounds = (a.proposed_revision, floor)
    args = (b, a) if reverse else (a, b)
    try:
        result = find_consistent_cut(*args, cache, lower_bounds=bounds[::-1] if reverse else bounds)
    except NoConsistentCutError:
        return None
    return dict(zip(("b", "a") if reverse else ("a", "b"), result))


def monotonic_histories():
    # Membership can disappear/reappear, but present X and A versions never
    # decrease. The final consumer contains the newer A; A's proposal is older.
    values = {"absent": None, "x1": 1, "x2": 2, "x3": 3, "a1": 1, "a3": 3}
    for prefix in product(values, repeat=3):
        states = (*prefix, "a3")
        xs = [values[state] for state in states if values[state] is not None]
        roots = [values[state] for state in states if state in ("a1", "a3")]
        if xs == sorted(xs) and roots == sorted(roots):
            yield states


class ContainmentTransitionTests(unittest.TestCase):
    @unittest.expectedFailure
    def test_omitting_contained_root_still_checks_shared_descendants(self):
        history = containment_history(("x1", "x2", "a3"))
        result = pair(history, ("b", 100, 0))
        expected = {"a": ("a", 100, 0), "b": ("b", 100, 0)}
        self.assertTrue(history.consistent(expected))
        self.assertEqual(result, expected)  # B's newer omission still conflicts on X.

    @unittest.expectedFailure
    def test_floor_excluding_only_compatible_omission_rejects_pair(self):
        history = containment_history(("x1", "x2", "a3"))
        floor = history.revisions("b")[1]
        self.assertFalse(any(history.consistent({"a": ("a", 100, 0), "b": revision})
                             for revision in history.revisions("b") if revision >= floor))
        self.assertIsNone(pair(history, floor))

    def check_catalog(self, require_exact):
        failures = []
        comparisons = 0
        for states, counts in product(monotonic_histories(), (False, True)):
            history = containment_history(states, counts)
            revisions = history.revisions("b")
            for floor_index, reverse in product(range(4), (False, True)):
                candidates = revisions[floor_index:]
                # Compute candidate validity from raw records, re-evaluating
                # containment per candidate rather than using the initial tree.
                exact = [r for r in candidates if states[revisions.index(r)] == "a1"]
                if bool(exact) != require_exact:
                    continue
                valid = [r for r in candidates
                         if history.consistent({"a": ("a", 100, 0), "b": r})]
                expected_revision = max(exact or valid, default=None)
                expected = ({"a": ("a", 100, 0), "b": expected_revision}
                            if expected_revision is not None else None)
                actual = pair(history, revisions[floor_index], reverse)
                comparisons += 1
                if actual != expected:
                    failures.append((states, counts, floor_index, reverse, actual, expected))
        self.assertEqual(comparisons, 308 if require_exact else 1548)
        self.assertEqual(len(failures), 0, f"{len(failures)}/{comparisons} mismatches; first: {failures[:1]}")

    def test_exact_containment_catalog_across_counts_sparse_versions_floors_and_orders(self):
        self.check_catalog(require_exact=True)

    @unittest.expectedFailure
    def test_omission_catalog_rechecks_all_shared_nodes_after_containment_disappears(self):
        self.check_catalog(require_exact=False)

    @unittest.expectedFailure
    def test_manager_never_commits_conflicting_omission_in_any_entry_path(self):
        failures = []
        for order, entry in product(permutations(("a", "b", "c")),
                                    ("initial", "update_before_cut", "add_then_cut", "add_then_update")):
            if entry.startswith("add_") and order[-1] != "b":
                continue
            history = containment_history(("x1", "x2", "a3"), constrain_a=True)
            # A's latest Y2 cannot match C's Y1, forcing the earlier proposal.
            history.add("c", 100, dependencies=(("y", 1),))
            manager = DataSubscriptionManager(HistoryCache(history))
            if entry.startswith("add_"):
                manager.add_subscriptions(order[:-1])
                manager.get_consistent_cut()
                manager.add_subscription("b")
            else:
                manager.add_subscriptions(order)
            if entry.endswith("update") or entry == "update_before_cut":
                manager.apply_update(history.add("b", history.heads["b"][1], 1,
                                                 dependencies=(("a", 9000),)))
            else:
                manager.get_consistent_cut()
            cut = current_cut(manager)
            witness = {key: (key, 100, 0) for key in order}
            self.assertTrue(history.consistent(witness))
            if not history.consistent(cut):
                failures.append((order, entry, cut))
        self.assertEqual(len(failures), 0, f"{len(failures)}/16 inconsistent commits; first: {failures[:1]}")

    def check_gaining_containment(self, exclude_omission):
        failures = []
        for counts, reverse in product((False, True), repeat=2):
            history = History()
            for version in (1, 2, 3):
                history.add("x", version)
            history.add("a", 100, dependencies=(("x", 1),))
            history.add("a", 100, 1, dependencies=(("x", 2),))
            # A's explicit proposal pins count zero; consuming A's version
            # expands count one. Matching containment compares only A's version.
            for index, deps in enumerate(((), (("a", 100),), (("a", 100),), (("x", 3),))):
                version, count = (50000, index) if counts else ((100, 200, 9000, 50000)[index], 0)
                history.add("b", version, count, deps)
            revisions = history.revisions("b")
            expected = {"a": ("a", 100, 0), "b": revisions[2]}
            self.assertTrue(history.consistent(expected))
            actual = pair(history, revisions[1 if exclude_omission else 0], reverse)
            if actual != expected:
                failures.append((counts, reverse, actual, expected))
        self.assertEqual(len(failures), 0, f"{len(failures)}/4 mismatches; first: {failures[:1]}")

    @unittest.expectedFailure
    def test_historical_containment_match_takes_precedence_over_older_omission(self):
        self.check_gaining_containment(exclude_omission=False)

    def test_joint_fallback_recognizes_historical_containment_when_omission_is_excluded(self):
        self.check_gaining_containment(exclude_omission=True)


def changing_dag(old_mask, new_mask=None):
    history = History()
    edges = tuple(combinations(range(3), 2))
    versions = ((100, old_mask),) if new_mask is None else ((100, old_mask), (9000, new_mask))
    for version, mask in versions:
        for node in reversed(range(3)):
            history.add(f"n{node}", version, dependencies=tuple(
                (f"n{target}", version) for bit, (source, target) in enumerate(edges)
                if source == node and mask & (1 << bit)))
    return history


class ChangingTopologyTests(unittest.TestCase):
    def test_every_three_node_dag_transition_with_mixed_heads_and_registration_orders(self):
        # All 8 old DAGs x 8 new DAGs x 8 head vectors x 6 orders = 3,072.
        checked = 0
        for old, new, heads, order in product(range(8), range(8),
                                             product((100, 9000), repeat=3), permutations(range(3))):
            with self.subTest(old=old, new=new, heads=heads, order=order):
                history = changing_dag(old, new)
                manager = DataSubscriptionManager(HistoryCache(history))
                manager.add_subscriptions(f"n{i}" for i in order)
                ceilings = {f"n{i}": (f"n{i}", heads[i], 0) for i in order}
                for key, sub in manager._subscriptions.items():
                    sub.latest_revision = ceilings[key]
                result = manager.get_consistent_cut()
                cut = current_cut(manager)
                self.assertTrue(history.consistent(cut), cut)
                self.assertIn(cut, list(history.cuts(ceilings)))
                self.assertEqual(result, {revision[:2] for revision in cut.values()})
                checked += 1
        self.assertEqual(checked, 3072)

    def test_topology_transitions_through_coordinated_publication_in_every_order(self):
        edges = tuple(combinations(range(3), 2))
        checked = 0
        for old, new, order, selected in product(range(8), range(8), permutations(range(3)), range(1, 8)):
            if any(new & (1 << bit) and order.index(target) > order.index(source)
                   for bit, (source, target) in enumerate(edges)):
                continue  # Publish new dependencies before their consumers.
            with self.subTest(old=old, new=new, order=order, selected=selected):
                complete = changing_dag(old, new)
                history = changing_dag(old)
                manager = DataSubscriptionManager(HistoryCache(history))
                manager.add_subscriptions(f"n{i}" for i in order if selected & (1 << i))
                manager.get_consistent_cut()
                for node in order:
                    key = f"n{node}"
                    before = current_cut(manager)
                    record = complete.records[(key, 9000, 0)]
                    update = history.add(key, 9000, dependencies=record.dependencies or ())
                    result = manager.apply_update(update)
                    after = current_cut(manager)
                    self.assertTrue(history.consistent(after), after)
                    self.assertTrue(all(after[k] >= before[k] for k in before))
                    self.assertEqual(result, {after[k][:2] for k in before if after[k][1] > before[k][1]})
                self.assertTrue(all(r[1] == 9000 for r in current_cut(manager).values()))
                checked += 1
        self.assertEqual(checked, 1176)  # Three publications per stream.
