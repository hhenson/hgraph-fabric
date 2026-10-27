from datetime import UTC, datetime
import unittest
from unittest.mock import patch

from hgraph_fabric.consistent_cut import find_consistent_cut
from hgraph_fabric.data_cache import DataCache
from hgraph_fabric.data_structures import DataUpdateDetails
from hgraph_fabric.data_subscription_manager import DataSubscriptionManager


AS_OF = datetime(2026, 10, 21, tzinfo=UTC)


class UpdateOnlyCache(DataCache):
    """Persist incoming heads before delivering the real cache notification."""

    def __init__(self):
        super().__init__()
        self.heads = {}

    def apply_update(self, update):
        revision = (update.id, *update.revision)
        self.heads[update.id] = max(self.heads.get(update.id, revision), revision)
        super().apply_update(update)

    def load_latest(self, data_id):
        return self.heads[data_id]

    def load_revision(self, revision_id):
        raise KeyError(revision_id)

    def load_version(self, version_id):
        raise KeyError(version_id)

    def load_latest_revision(self, version_id):
        candidates = [r for r in self._revisions if r[:2] == version_id]
        if not candidates:
            raise KeyError(version_id)
        return max(candidates)

    def write_version(self, version_id, data, details):
        raise AssertionError("Update notifications must not write data")

    def write_revision(self, revision_id, details):
        raise AssertionError("Update notifications must not write revisions")

    def write_latest_version(self, version_id):
        raise AssertionError("Update notifications must not write latest")

    def write_latest_revision(self, revision_id):
        raise AssertionError("Read-only fixture must not write heads")



class SubscriptionUpdateTests(unittest.TestCase):
    def setUp(self):
        self.cache = UpdateOnlyCache()
        self.manager = DataSubscriptionManager(self.cache)

    def update(self, data_id, version, count=0, dependencies=None):
        return DataUpdateDetails(
            id=data_id, revision=(version, count),
            dependencies=dependencies, weak_dependencies=None, as_of=AS_OF,
        )

    def seed_pair(self):
        self.cache.apply_update(self.update("x", 1))
        for data_id in ("a", "b"):
            self.cache.apply_update(self.update(data_id, 1, dependencies=(("x", 1),)))
        self.manager.add_subscriptions(["a", "b"])
        self.manager.get_consistent_cut()
        return self.manager._subscriptions["a"], self.manager._subscriptions["b"]

    def test_unsubscribed_dependency_update_changes_only_cache(self):
        a, b = self.seed_pair()
        graph = a.current(self.cache)
        with patch("hgraph_fabric.data_subscription_manager.find_consistent_cut") as compare:
            self.assertEqual(self.manager.apply_update(self.update("x", 2)), set())
        compare.assert_not_called()
        self.assertEqual(self.cache.latest("x"), ("x", 2, 0))
        self.assertEqual(self.cache.get_latest_revision(("x", 1)), ("x", 1, 0))
        self.assertEqual(a.latest_revision, ("a", 1, 0))
        self.assertEqual(b.latest_revision, ("b", 1, 0))
        self.assertIs(a.current(self.cache), graph)
        self.assertEqual(a.current_revision, ("a", 1, 0))

    def test_sibling_revisions_can_arrive_separately_with_different_versions(self):
        for order in (("left", "right"), ("right", "left")):
            with self.subTest(order=order):
                self.setUp()
                for version in (1, 2):
                    self.cache.apply_update(self.update(
                        "leaf", version
                    ))
                    self.cache.apply_update(self.update(
                        "x", version, dependencies=(("leaf", version),),
                    ))
                for sibling in order:
                    self.cache.apply_update(self.update(
                        sibling, 1, dependencies=(("x", 1),)
                    ))
                self.cache.apply_update(self.update(
                    "root", 1, dependencies=(("left", 1), ("right", 1))
                ))
                root = self.manager.add_subscription("root")
                self.manager.get_consistent_cut()

                # The first sibling temporarily disagrees with the other.
                # Index both paths without trying to select a dependency tree.
                self.assertEqual(self.manager.apply_update(self.update(
                    order[0], 1, 1, dependencies=(("x", 2),)
                )), set())
                self.assertEqual(root.current_revision, ("root", 1, 0))
                for data_id in ("x", "leaf"):
                    for version in (1, 2):
                        self.assertEqual(self.manager._dependents[(data_id, version)], {"root"})

                self.assertEqual(self.manager.apply_update(self.update(
                    order[1], 1, 1, dependencies=(("x", 2),)
                )), set())
                self.assertEqual(self.manager.get_consistent_cut(), {("root", 1)})
                self.assertTrue(root.current(self.cache).has_node(("x", 2)))
                for data_id in ("x", "leaf"):
                    self.assertNotIn((data_id, 1), self.manager._dependents)
                    self.assertEqual(self.manager._dependents[(data_id, 2)], {"root"})

    def test_dependency_index_rejects_conflicting_versions_in_one_record(self):
        for dependencies in ((("x", 1), ("x", 2)), (("middle", 2),)):
            with self.subTest(dependencies=dependencies):
                self.setUp()
                for version in (1, 2):
                    self.cache.apply_update(self.update(
                        "x", version
                    ))
                self.cache.apply_update(self.update("middle", 1, dependencies=(("x", 1),)))
                self.cache.apply_update(self.update("root", 1, dependencies=(("middle", 1),)))
                root = self.manager.add_subscription("root")
                self.manager.get_consistent_cut()
                before = {key: set(value) for key, value in self.manager._dependents.items()}
                with self.assertRaisesRegex(ValueError, "Conflicting version"):
                    self.manager.apply_update(self.update("middle", 1, 1, dependencies=dependencies))
                self.assertEqual(root.current_revision, ("root", 1, 0))
                self.assertEqual(self.manager._dependents, before)

    def test_update_before_first_cut_initializes_subscriptions(self):
        self.cache.apply_update(self.update("a", 1))
        self.cache.apply_update(self.update("b", 1))
        self.manager.add_subscriptions(["a", "b"])
        self.assertEqual(
            self.manager.apply_update(self.update("a", 2)),
            {("a", 2), ("b", 1)},
        )
        self.assertEqual(self.cache.latest("a"), ("a", 2, 0))
        self.assertEqual(self.manager._subscriptions["a"].latest_revision, ("a", 2, 0))
        self.assertEqual(self.manager._subscriptions["a"].current_revision, ("a", 2, 0))
        self.assertEqual(self.manager._subscriptions["b"].current_revision, ("b", 1, 0))

    def test_blocked_update_waits_then_peer_update_unlocks_both(self):
        a, b = self.seed_pair()
        self.assertEqual(self.manager.apply_update(self.update("x", 2)), set())
        self.assertEqual(
            self.manager.apply_update(self.update("a", 2, dependencies=(("x", 2),))),
            set(),
        )
        self.assertEqual(a.latest_revision, ("a", 2, 0))
        self.assertEqual(a.current_revision, ("a", 1, 0))
        self.assertEqual(b.current_revision, ("b", 1, 0))

        self.assertEqual(
            self.manager.apply_update(self.update("b", 2, dependencies=(("x", 2),))),
            {("a", 2), ("b", 2)},
        )
        self.assertEqual(a.current_revision, ("a", 2, 0))
        self.assertEqual(b.current_revision, ("b", 2, 0))
        self.assertEqual(self.cache.latest("a"), a.latest_revision)
        self.assertEqual(self.cache.latest("b"), b.latest_revision)

    def test_unchanged_pairs_are_not_recompared(self):
        a, b = self.seed_pair()
        for data_id in ("c", "d"):
            self.cache.apply_update(self.update(data_id, 1))
            self.manager.add_subscription(data_id)
        self.manager.get_consistent_cut()
        with patch(
            "hgraph_fabric.data_subscription_manager.find_consistent_cut",
            wraps=find_consistent_cut,
        ) as compare:
            self.assertEqual(
                self.manager.apply_update(self.update("a", 1, 1, dependencies=(("x", 1),))),
                set(),
            )
        self.assertEqual(
            {frozenset((args[0].data_id, args[1].data_id)) for args, _ in compare.call_args_list},
            {frozenset(("a", other)) for other in ("b", "c", "d")},
        )
        self.assertEqual(compare.call_count, 3)
        self.assertEqual(a.current_revision, ("a", 1, 1))
        self.assertEqual(b.current_revision, ("b", 1, 0))

    def test_updated_item_returning_to_current_stops_before_other_peers(self):
        a, b = self.seed_pair()
        self.cache.apply_update(self.update("c", 1))
        c = self.manager.add_subscription("c")
        self.manager.get_consistent_cut()
        # An independently improved latest must not be committed if the item
        # driving this update cannot advance. It need not even be compared.
        self.cache.apply_update(self.update("c", 2))
        c.latest_revision = ("c", 2, 0)
        self.cache.apply_update(self.update("x", 2))
        with patch(
            "hgraph_fabric.data_subscription_manager.find_consistent_cut", wraps=find_consistent_cut
        ) as compare:
            self.assertEqual(
                self.manager.apply_update(self.update("a", 2, dependencies=(("x", 2),))),
                set(),
            )
        self.assertEqual(compare.call_count, 1)
        self.assertEqual(tuple(sub.data_id for sub in compare.call_args.args[:2]), ("a", "b"))
        for sub in (a, b, c):
            self.assertEqual(sub.current_revision, (sub.data_id, 1, 0))

    def test_improved_peers_are_stabilized_after_the_updated_item(self):
        for reject in (False, True):
            with self.subTest(reject=reject):
                self.setUp()
                for data_id in ("x", "y", "z"):
                    self.cache.apply_update(self.update(data_id, 1))
                    self.cache.apply_update(self.update(data_id, 2))
                for data_id, dependencies in (
                    ("a", (("x", 1), ("y", 1))),
                    ("b", (("x", 1), ("z", 1))),
                    ("c", (("y", 1), ("z", 1))),
                ):
                    self.cache.apply_update(self.update(data_id, 1, dependencies=dependencies))
                self.manager.add_subscriptions(["a", "b", "c"])
                self.manager.get_consistent_cut()
                self.cache.apply_update(self.update("a", 2, dependencies=(("x", 2), ("y", 1))))
                # Both latest peers match a's new tree, but disagree on z.
                # If b consumed x before z, b=2 resolves that disagreement.
                # Otherwise b must return to 1, which forces a back to 1.
                b_middle = (("x", 1), ("z", 2)) if reject else (("x", 2), ("z", 1))
                self.cache.apply_update(self.update("b", 2, dependencies=b_middle))
                for update in (
                    self.update("b", 3, dependencies=(("x", 2), ("z", 2))),
                    self.update("c", 2, dependencies=(("y", 2), ("z", 1))),
                ):
                    self.cache.apply_update(update)
                    self.manager._subscriptions[update.id].latest_revision = (update.id, *update.revision)

                observed = []

                def compare(left, right, cache, **kwargs):
                    observed.append((left.data_id, right.data_id))
                    for sub in self.manager._subscriptions.values():
                        self.assertEqual(sub.current_revision, (sub.data_id, 1, 0))
                    return find_consistent_cut(left, right, cache, **kwargs)

                with patch("hgraph_fabric.data_subscription_manager.find_consistent_cut", side_effect=compare):
                    advanced = self.manager.apply_update(
                        self.update("a", 3, dependencies=(("x", 2), ("y", 2)))
                    )
                self.assertEqual(observed[:2], [("a", "b"), ("a", "c")])
                self.assertIn(("b", "c"), observed[2:])
                expected = {("a", 3), ("b", 2), ("c", 2)}
                self.assertEqual(advanced, set() if reject else expected)
                self.assertEqual(
                    {sub.current_revision[:2] for sub in self.manager._subscriptions.values()},
                    {(data_id, 1) for data_id in ("a", "b", "c")} if reject else expected,
                )

    def test_lowered_updated_proposal_rechecks_earlier_latest_matches(self):
        for data_id, versions in (("x", (1, 2)), ("y", (0, 1, 2))):
            for version in versions:
                self.cache.apply_update(self.update(data_id, version))
        for data_id, dependencies in (
            ("a", (("x", 1), ("y", 0))), ("b", (("x", 1),)), ("c", (("y", 0),)),
        ):
            self.cache.apply_update(self.update(data_id, 1, dependencies=dependencies))
        self.manager.add_subscriptions(["a", "b", "c"])
        self.manager.get_consistent_cut()
        for version, y in ((2, 1), (3, 2)):
            self.cache.apply_update(
                self.update("a", version, dependencies=(("x", 1), ("y", y)))
            )
        for data_id, dependency in (("b", ("x", 2)), ("c", ("y", 1))):
            self.cache.apply_update(self.update(data_id, 2, dependencies=(dependency,)))
            self.manager._subscriptions[data_id].latest_revision = (data_id, 2, 0)
        with patch(
            "hgraph_fabric.data_subscription_manager.find_consistent_cut", wraps=find_consistent_cut
        ) as compare:
            self.assertEqual(
                self.manager.apply_update(self.update("a", 4, dependencies=(("x", 2), ("y", 2)))),
                {("a", 2), ("c", 2)},
            )
        self.assertEqual(
            [(args[0].data_id, args[1].data_id) for args, _ in compare.call_args_list[:3]],
            [("a", "b"), ("a", "c"), ("a", "b")],
        )
        self.assertEqual(self.manager._subscriptions["b"].current_revision, ("b", 1, 0))

    def test_single_subscription_and_duplicate_update(self):
        self.cache.apply_update(self.update("a", 1))
        a = self.manager.add_subscription("a")
        self.manager.get_consistent_cut()
        update = self.update("a", 2)
        with patch("hgraph_fabric.data_subscription_manager.find_consistent_cut") as compare:
            self.assertEqual(self.manager.apply_update(update), {("a", 2)})
            self.assertEqual(self.manager.apply_update(update), set())
        compare.assert_not_called()
        self.assertEqual(a.current_revision, ("a", 2, 0))
        self.assertEqual(a.latest_revision, ("a", 2, 0))
        self.assertEqual(self.cache.latest("a"), ("a", 2, 0))

    def test_update_comparisons_keep_committed_cut_and_trees_until_commit(self):
        a, b = self.seed_pair()
        self.manager.apply_update(self.update("x", 2))
        a_update = self.update("a", 2, dependencies=(("x", 2),))
        original_graphs = {sub.data_id: sub.current(self.cache) for sub in (a, b)}

        def compare(left, right, cache, **kwargs):
            for sub in (a, b):
                self.assertEqual(sub.current_revision, (sub.data_id, 1, 0))
                self.assertIn((sub.data_id, 1, 0), sub._revision_graphs)
                self.assertIs(sub.current(cache), original_graphs[sub.data_id])
            return find_consistent_cut(left, right, cache, **kwargs)

        with patch("hgraph_fabric.data_subscription_manager.find_consistent_cut", side_effect=compare):
            self.assertEqual(self.manager.apply_update(a_update), set())
            for sub in (a, b):
                self.assertIs(sub.current(self.cache), original_graphs[sub.data_id])
            self.assertEqual(
                self.manager.apply_update(
                    self.update("b", 2, dependencies=(("x", 2),))
                ),
                {("a", 2), ("b", 2)},
            )
        for sub in (a, b):
            self.assertEqual(sub.current_revision, (sub.data_id, 2, 0))
            self.assertNotIn((sub.data_id, 1, 0), sub._revision_graphs)

    def test_failed_proposal_does_not_prune_committed_graphs(self):
        a, b = self.seed_pair()
        graphs = {sub.data_id: sub.current(self.cache) for sub in (a, b)}

        def fail(left, right, cache, **kwargs):
            for sub in (a, b):
                self.assertEqual(sub.current_revision, (sub.data_id, 1, 0))
                self.assertIn((sub.data_id, 1, 0), sub._revision_graphs)
            raise OSError("Unavailable history")

        with patch("hgraph_fabric.data_subscription_manager.find_consistent_cut", side_effect=fail):
            with self.assertRaises(OSError):
                self.manager.apply_update(self.update("a", 2))
        for sub in (a, b):
            self.assertEqual(sub.current_revision, (sub.data_id, 1, 0))
            self.assertIs(sub.current(self.cache), graphs[sub.data_id])
        self.assertEqual(a.latest_revision, ("a", 2, 0))
        self.assertEqual(self.cache.latest("a"), ("a", 2, 0))

    def test_new_subscription_cannot_regress_an_established_current_on_update(self):
        self.cache.apply_update(self.update("x", 1))
        self.cache.apply_update(self.update("x", 2))
        self.cache.apply_update(self.update("a", 1, dependencies=(("x", 1),)))
        self.cache.apply_update(self.update("a", 2, dependencies=(("x", 2),)))
        a = self.manager.add_subscription("a")
        self.manager.get_consistent_cut()
        self.cache.apply_update(self.update("b", 1, dependencies=(("x", 1),)))
        b = self.manager.add_subscription("b")
        self.assertEqual(
            self.manager.apply_update(self.update("b", 1, 1, dependencies=(("x", 1),))), set()
        )
        self.assertEqual(a.current_revision, ("a", 2, 0))
        self.assertEqual(b.current_revision, ("b", 0, 0))

    def test_failed_comparison_retains_update_and_allows_retry(self):
        a, b = self.seed_pair()
        update = self.update("a", 2, dependencies=(("missing", 1),))
        with self.assertRaises(KeyError):
            self.manager.apply_update(update)
        self.assertEqual(a.latest_revision, ("a", 2, 0))
        self.assertEqual(self.cache.latest("a"), ("a", 2, 0))
        self.assertEqual(a.current_revision, ("a", 1, 0))
        self.assertEqual(b.current_revision, ("b", 1, 0))
        self.assertEqual(self.manager.apply_update(self.update("missing", 1)), set())
        self.assertEqual(self.manager.apply_update(update), {("a", 2)})
        self.assertEqual(a.current_revision, ("a", 2, 0))
        self.assertEqual(b.current_revision, ("b", 1, 0))

    def test_proposal_that_regresses_a_selection_keeps_the_previous_cut(self):
        self.cache.apply_update(self.update("a", 1, 0))
        self.cache.apply_update(self.update("a", 1, 1))
        self.cache.apply_update(self.update("b", 1, 0))
        self.manager.add_subscriptions(["a", "b"])
        self.manager.get_consistent_cut()
        a, b = self.manager._subscriptions.values()
        # The pair search can search below its starting revision. The update
        # layer must not publish a proposal below the already established cut.
        with patch(
            "hgraph_fabric.data_subscription_manager.find_consistent_cut",
            return_value=(("a", 1, 0), ("b", 1, 0)),
        ):
            self.assertEqual(
                self.manager.apply_update(self.update("a", 2)),
                set(),
            )
        self.assertEqual(a.latest_revision, ("a", 2, 0))
        self.assertEqual(a.current_revision, ("a", 1, 1))
        self.assertEqual(b.current_revision, ("b", 1, 0))
