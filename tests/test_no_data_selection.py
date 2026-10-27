"""No-data is a coherent selection, not missing publication metadata."""
from itertools import permutations, product
import unittest
from unittest.mock import Mock, patch

from pyarrow import table

from hgraph_fabric.data_fabric_session import DataFabricSession, PollingDataFabricSession
from hgraph_fabric.data_structures import no_data_revision
from test_data_cache import MemoryDataCache


class NoDataSelectionTests(unittest.TestCase):
    def setUp(self):
        self.cache = MemoryDataCache()
        self.session = DataFabricSession(self.cache)

    def publish(self, name, dependencies=(), *, revise=False):
        return self.cache.write(
            None if revise else table({"value": [name]}), id=name,
            dependencies=dependencies,
        )[1]

    def test_unpublished_root_initializes_without_loading_a_value(self):
        sub = self.session.add_subscription("a")
        self.assertIsNone(sub.current_revision)
        self.assertFalse(self.session._subscriptions.is_initialized)
        self.assertEqual(self.session.get_consistent_cut(), {})
        self.assertTrue(self.session._subscriptions.is_initialized)
        self.assertEqual(sub.current_revision, no_data_revision("a"))
        self.assertEqual(self.cache.loads, [("latest", "a")])
        self.assertEqual(self.cache.writes, [])

        update = self.publish("a")
        self.assertEqual(set(self.session.apply_update(update)), {"a"})
        self.assertEqual(sub.current_revision, ("a", 1, 0))
        self.assertEqual(self.session.apply_update(update), {})

    def test_empty_table_is_a_published_value(self):
        self.cache.write(table({"value": []}), id="empty")
        self.session.add_subscriptions(("empty", "absent"))
        cut = self.session.get_consistent_cut()
        self.assertEqual(set(cut), {"empty"})
        self.assertEqual(cut["empty"].num_rows, 0)

    def test_conflicting_first_publications_do_not_block_independent_root(self):
        self.publish("x")
        self.publish("x")
        self.publish("b", (("x", 1),))
        self.publish("c", (("x", 2),))
        self.publish("d")
        for order in permutations(("b", "c", "d")):
            with self.subTest(order=order):
                session = DataFabricSession(self.cache)
                session.add_subscriptions(order)
                cut = session.get_consistent_cut()
                self.assertIn("d", cut)
                self.assertEqual(len(set(cut) & {"b", "c"}), 1)
                self.assertTrue(session._subscriptions.is_initialized)

    def test_waiting_no_data_root_advances_with_peer_and_does_not_rollback(self):
        self.publish("x")
        self.publish("x")
        self.publish("b", (("x", 1),))
        self.publish("c", (("x", 2),))
        self.publish("d")
        self.session.add_subscriptions(("b", "c", "d"))
        self.assertEqual(set(self.session.get_consistent_cut()), {"b", "d"})
        # Advancing b makes c's already-stored first value admissible too.
        update = self.publish("b", (("x", 2),))
        self.assertEqual(set(self.session.apply_update(update)), {"b", "c"})
        self.assertEqual(set(self.session.get_consistent_cut()), {"b", "c", "d"})
        self.publish("x")
        update = self.publish("c", (("x", 3),))
        self.assertEqual(self.session.apply_update(update), {})
        currents = {k: s.current_revision for k, s in self.session._subscriptions._subscriptions.items()}
        self.assertEqual(currents, {"b": ("b", 2, 0), "c": ("c", 1, 0), "d": ("d", 1, 0)})

    def test_no_data_cannot_satisfy_a_strong_dependency(self):
        self.publish("x")
        self.publish("x")
        self.publish("b", (("x", 1),))
        self.publish("c", (("x", 2),))
        self.publish("consumer", (("c", 1),))
        for order in permutations(("b", "c", "consumer")):
            with self.subTest(order=order):
                session = DataFabricSession(self.cache)
                session.add_subscriptions(order)
                cut = session.get_consistent_cut()
                if "consumer" in cut:
                    self.assertIn("c", cut)
                    self.assertNotIn("b", cut)
                if "b" in cut:
                    self.assertNotIn("consumer", cut)
                    self.assertNotIn("c", cut)

    def test_internal_conflict_initializes_at_no_data_then_dependency_revision_releases_it(self):
        self.publish("x")
        self.publish("x")
        self.publish("m", (("x", 1),))
        self.publish("n", (("x", 2),))
        self.publish("root", (("m", 1), ("n", 1)))
        self.publish("d")
        self.session.add_subscriptions(("root", "d"))
        self.assertEqual(set(self.session.get_consistent_cut()), {"d"})
        update = self.publish("m", (("x", 2),), revise=True)
        self.assertEqual(set(self.session.apply_update(update)), {"root"})
        self.assertEqual(self.session.apply_update(update), {})
        self.assertEqual(self.session._subscriptions._pending_cuts, {})

    def test_internal_conflict_searches_published_root_history_before_no_data(self):
        self.publish("x")
        self.publish("x")
        self.publish("m", (("x", 1),))
        self.publish("n", (("x", 2),))
        self.publish("root")
        self.publish("root", (("m", 1), ("n", 1)))
        sub = self.session.add_subscription("root")
        self.assertEqual(set(self.session.get_consistent_cut()), {"root"})
        self.assertEqual(sub.current_revision, ("root", 1, 0))

    def test_older_version_revision_wakes_no_data_through_historical_root(self):
        for _ in range(3):
            self.publish("x")
        self.publish("m", (("x", 1),))
        self.publish("n", (("x", 2),))
        self.publish("root", (("m", 1), ("n", 1)))
        self.publish("m", (("x", 3),))
        self.publish("root", (("m", 2), ("n", 1)))
        sub = self.session.add_subscription("root")
        self.assertEqual(self.session.get_consistent_cut(), {})
        # Latest root omits m@1. Its old version's new revision nevertheless
        # makes the historical root selectable, so its watch must survive.
        _, update = self.cache.write_dependencies("m", (("x", 2),), version=1)
        self.assertEqual(set(self.session.apply_update(update)), {"root"})
        self.assertEqual(sub.current_revision, ("root", 1, 0))
        self.assertEqual(sub.latest_revision, ("root", 2, 0))
        _, update = self.cache.write_dependencies("n", (("x", 3),))
        self.assertEqual(set(self.session.apply_update(update)), {"root"})
        self.assertEqual(sub.current_revision, ("root", 2, 0))
        self.assertEqual(self.session._subscriptions._pending_cuts, {})

    def test_late_subscription_can_wait_without_changing_delivered_values(self):
        self.publish("x")
        self.publish("x")
        self.publish("a", (("x", 2),))
        self.session.add_subscription("a")
        self.session.get_consistent_cut()
        self.publish("b", (("x", 1),))
        sub = self.session.add_subscription("b")
        self.assertEqual(set(self.session.get_consistent_cut()), {"a"})
        self.assertEqual(sub.current_revision, no_data_revision("b"))
        update = self.publish("b", (("x", 2),), revise=True)
        self.assertEqual(set(self.session.apply_update(update)), {"b"})

    def test_missing_dependency_and_storage_failure_are_not_no_data(self):
        self.publish("root", (("missing", 1),))
        with self.assertRaises(KeyError):
            self.session.add_subscription("root")
        self.assertEqual(self.session._subscriptions._subscriptions, {})
        # A failed store lookup must not hide an operational error either.
        with patch.object(self.cache, "load_latest", side_effect=OSError("offline")):
            with self.assertRaises(OSError):
                self.session.add_subscription("other")

    def test_broken_history_cannot_be_skipped_to_no_data(self):
        self.publish("x")
        self.publish("x")
        self.publish("a", (("x", 1),))
        self.publish("a", (("x", 2),))
        self.publish("b", (("x", 1),))
        self.cache._revisions.clear()
        del self.cache.revision_values[("a", 1, 0)]
        self.session.add_subscriptions(("a", "b"))
        with self.assertRaises(KeyError):
            self.session.get_consistent_cut()
        self.assertFalse(self.session._subscriptions.is_initialized)

    def test_joint_published_match_precedes_no_data_fallback(self):
        for _ in range(3):
            self.publish("x")
            self.publish("y")
        self.publish("a", (("x", 1), ("y", 1)))
        self.publish("a", (("x", 2), ("y", 3)))
        self.publish("b", (("x", 1), ("y", 1)))
        self.publish("b", (("x", 3), ("y", 2)))
        for order in permutations(("a", "b")):
            with self.subTest(order=order):
                session = DataFabricSession(self.cache)
                session.add_subscriptions(order)
                self.assertEqual(session._subscriptions.get_consistent_cut(), {("a", 1), ("b", 1)})

    def test_initial_cuts_and_advances_match_raw_records_across_registration_orders(self):
        # Five dependency patterns, three roots, and six orders: 750 starts.
        # Check raw records independently of production graph expansion.
        patterns = ((), (("x", 1),), (("x", 2),), (("y", 1),), (("y", 2),))
        for dependencies in product(patterns, repeat=3):
            for order in permutations(("a", "b", "c")):
                with self.subTest(dependencies=dependencies, order=order):
                    self.setUp()
                    for _ in range(2):
                        self.publish("x")
                        self.publish("y")
                    for root, inputs in zip(("a", "b", "c"), dependencies):
                        self.publish(root, inputs)
                    self.session.add_subscriptions(order)
                    cut = self.session.get_consistent_cut()
                    for root, inputs in zip(("a", "b", "c"), dependencies):
                        if not inputs:
                            self.assertIn(root, cut)
                    before = {k: s.current_revision for k, s in self.session._subscriptions._subscriptions.items()}

                    def validate():
                        nodes = {}
                        for sub in self.session._subscriptions._subscriptions.values():
                            revision = sub.current_revision
                            self.assertGreaterEqual(revision, before[sub.data_id])
                            before[sub.data_id] = revision
                            self.assertEqual(nodes.setdefault(sub.data_id, revision[1]), revision[1])
                            if revision[1]:
                                for name, version in self.cache.revision_values[revision].dependencies:
                                    self.assertEqual(nodes.setdefault(name, version), version)

                    validate()
                    for root in order:
                        self.session.apply_update(self.publish(root, (("x", 2), ("y", 2))))
                        validate()
                    self.assertTrue(all(s.current_revision == s.latest_revision
                                        for s in self.session._subscriptions._subscriptions.values()))

    def test_poll_initializes_empty_cut_then_delivers_first_publication(self):
        transport = Mock()
        transport.poll.return_value = None
        session = PollingDataFabricSession(self.cache, transport)
        session.add_subscription("a")
        self.assertEqual(session.poll(0), {})
        self.assertTrue(session._subscriptions.is_initialized)
        transport.poll.assert_called_once_with(0.0)
        transport.poll.reset_mock()
        transport.poll.return_value = self.publish("a")
        self.assertEqual(set(session.poll(0)), {"a"})
        transport.poll.assert_called_once()
