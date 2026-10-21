"""Live DAG changes with nodes serving as both subscriptions and dependencies.

Generation uses raw records to exclude internally conflicting trees before any
cache notification. It never rejects a history because of a resolver result.
Every accepted publication is checked by the existing independent stream oracle.
"""
from itertools import permutations, product
from random import Random
import unittest

from adversarial_cases import History, HistoryCache, current_cut
from hgraph_fabric.consistent_cut import NoConsistentCutError, find_consistent_cut
from hgraph_fabric.data_subscription_graph import DataSubscriptionGraph
from hgraph_fabric.data_subscription_manager import DataSubscriptionManager
from test_adversarial_transitions import Stream


def start_stream(case, seed, branching):
    rng = Random(seed)
    names = [f"n{i}" for i in range(6)]
    rng.shuffle(names)  # Lexical mismatch order is independent of graph topology.
    history = History()
    for index in reversed(range(6)):
        targets = names[index + 1:]
        if branching:
            dependencies = tuple((key, 100) for key in targets if rng.randrange(2))
        else:
            dependencies = ((targets[0], 100),) if targets else ()
        history.add(names[index], 100, dependencies=dependencies)
    roots = rng.sample(names, rng.randrange(2, 7))
    return rng, names, Stream(case, history, roots, shared=seed % 2 == 0)


def next_update(history, rng, names, branching):
    for _ in range(100):
        index = rng.randrange(len(names))
        key = names[index]
        old = history.heads[key]
        _, version, count = old
        if rng.randrange(2):
            version, count = version + rng.choice((7, 113, 1009)), 0
        else:
            count += 1
        targets = names[index + 1:]
        if branching:
            dependencies = tuple(history.heads[k][:2] for k in targets if rng.randrange(3) == 0)
        else:
            dependencies = (() if not targets or rng.randrange(4) == 0
                            else (history.heads[rng.choice(targets)][:2],))
        update = history.add(key, version, count, dependencies, weak=(("unavailable-weak", 999),))
        # A count refresh can make two old paths disagree inside a single tree.
        # That is fatal input, covered separately, not a viable cut-search case.
        try:
            for revision in history.records:
                history.nodes(revision)
        except ValueError:
            del history.records[(key, version, count)]
            history.heads[key] = old
            continue
        return update
    raise AssertionError("Could not generate a coherent publication")


def converge(case, stream, names):
    # Publish an available final chain in dependency-before-consumer order.
    for index in reversed(range(len(names))):
        key = names[index]
        dependencies = (stream.history.heads[names[index + 1]][:2],) if index < len(names) - 1 else ()
        version = stream.history.heads[key][1] + 100003
        stream.publish(stream.history.add(key, version, dependencies=dependencies))
    for manager in stream.managers:
        case.assertEqual(current_cut(manager), {key: stream.history.heads[key] for key in manager._subscriptions})
        case.assertEqual(manager._conflicts, {})


class MixedRoleReviewTests(unittest.TestCase):
    def test_live_forests_with_subscribed_ancestors_and_descendants(self):
        # 128 streams * (64 mixed events + 6 convergence events), plus duplicates.
        for seed in range(128):
            with self.subTest(seed=seed):
                rng, names, stream = start_stream(self, seed, branching=False)
                for step in range(64):
                    with self.subTest(step=step):
                        stream.publish(next_update(stream.history, rng, names, branching=False))
                converge(self, stream, names)

    def test_live_branching_dags_and_exhaustive_bounded_pair_oracle(self):
        # 64 streams * 70 events, then 24 bounded pairs in both argument orders.
        outcomes = set()
        relations = set()
        comparisons = 0
        for seed in range(64):
            with self.subTest(seed=seed):
                rng, names, stream = start_stream(self, seed, branching=True)
                for step in range(64):
                    with self.subTest(step=step):
                        stream.publish(next_update(stream.history, rng, names, branching=True))
                converge(self, stream, names)
                history = stream.history
                for trial in range(24):
                    ids = rng.sample(names, 2)
                    histories = [history.revisions(key) for key in ids]
                    spans = [sorted(rng.choices(range(len(revisions)), k=2)) for revisions in histories]
                    floors = tuple(revisions[span[0]] for revisions, span in zip(histories, spans))
                    ceilings = tuple(revisions[span[1]] for revisions, span in zip(histories, spans))
                    nodes = [history.nodes(revision) for revision in ceilings]
                    if ids[0] in nodes[1] or ids[1] in nodes[0]:
                        relations.add("containment")
                    elif nodes[0].keys() & nodes[1].keys():
                        relations.add("overlap")
                    else:
                        relations.add("disjoint")
                    # Exhaustive raw-record search only checks safety/existence;
                    # it does not assume a unique or globally maximal cut.
                    admissible = [cut for cut in history.cuts(dict(zip(ids, ceilings)))
                                  if all(cut[key] >= floor for key, floor in zip(ids, floors))]
                    cache = HistoryCache(history)
                    subs = [DataSubscriptionGraph(key) for key in ids]
                    for sub, floor, ceiling in zip(subs, floors, ceilings):
                        sub.latest_revision = history.heads[sub.data_id]
                        sub.current_revision = floor
                        sub.proposed_revision = ceiling
                    selections = [(s.latest_revision, s.current_revision, s.proposed_revision) for s in subs]
                    # Reuse the cache in reverse order to exercise warmed trees.
                    for reverse in (False, True):
                        with self.subTest(trial=trial, floors=floors, ceilings=ceilings, reverse=reverse):
                            args = subs[::-1] if reverse else subs
                            bounds = floors[::-1] if reverse else floors
                            try:
                                result = find_consistent_cut(*args, cache, lower_bounds=bounds)
                            except NoConsistentCutError:
                                outcomes.add("no solution")
                                self.assertFalse(admissible)
                            else:
                                outcomes.add("compatible")
                                self.assertIn(dict(zip(ids[::-1] if reverse else ids, result)), admissible)
                            self.assertEqual([(s.latest_revision, s.current_revision, s.proposed_revision)
                                              for s in subs], selections)
                            comparisons += 1
        self.assertEqual(comparisons, 3072)
        self.assertEqual(outcomes, {"compatible", "no solution"})
        self.assertEqual(relations, {"containment", "overlap", "disjoint"})

    def test_late_subscription_to_conflict_path_preserves_committed_bounds(self):
        # A previously soft intermediate becomes a subscription. Exercise both
        # successful release and a still-blocked update before releasing it.
        for order, bulk, blocked_first in product(permutations(("a", "b", "peer")), (False, True), (False, True)):
            with self.subTest(order=order, bulk=bulk, blocked_first=blocked_first):
                history = History()
                history.add("x", 1)
                history.add("x", 2)
                history.add("m", 100, dependencies=(("x", 1),))
                for key in ("a", "b"):
                    history.add(key, 100)
                    history.add(key, 9000, dependencies=(("m", 100),))
                history.add("peer", 100, dependencies=(("x", 2),))
                cache = HistoryCache(history)
                manager = DataSubscriptionManager(cache)
                manager.add_subscriptions(order)
                manager.get_consistent_cut()
                old = current_cut(manager)
                self.assertEqual(set(manager._conflicts), {("m", 100)})
                if bulk:
                    manager.add_subscriptions(("m", "m"))  # Idempotent bulk registration.
                else:
                    manager.add_subscription("m")
                sub = manager.add_subscription("m")
                self.assertIs(sub, manager._subscriptions["m"])
                self.assertIsNone(sub.current_revision)
                count = 0
                if blocked_first:
                    count += 1
                    result = manager.apply_update(history.add("m", 100, count, dependencies=(("x", 1),)))
                    self.assertEqual(result, set())
                    self.assertEqual({key: manager._subscriptions[key].current_revision for key in old}, old)
                    self.assertIsNone(sub.current_revision)
                count += 1
                result = manager.apply_update(history.add("m", 100, count, dependencies=(("x", 2),)))
                self.assertEqual(result, {("a", 9000), ("b", 9000), ("m", 100)})
                self.assertEqual(current_cut(manager), {key: history.heads[key] for key in (*order, "m")})
                self.assertTrue(history.consistent(current_cut(manager)))
                self.assertTrue(all(manager._subscriptions[key].current_revision >= old[key] for key in old))
                self.assertEqual(manager._conflicts, {})
                self.assertEqual(manager.apply_update(history.records[history.heads["m"]]), set())
