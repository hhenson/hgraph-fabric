"""Startup snapshot/delta overlap and late notifications at the session boundary."""
from collections import deque
from dataclasses import replace
import unittest
from unittest.mock import patch

from hgraph_fabric.data_fabric_session import DataFabricSession, PollingDataFabricSession
from hgraph_fabric.data_subscription_manager import DataSubscriptionManager
from adversarial_cases import History, HistoryCache, current_cut
from test_data_cache import MemoryDataCache
from test_data_fabric_session import publish


class BufferedTransport:
    def __init__(self):
        self.ready = False
        self.updates = deque()

    def start(self):
        self.ready = True

    def stop(self):
        self.ready = False

    def send(self, update):
        if not self.ready:
            raise AssertionError("Notification reception must precede initialization")
        self.updates.append(update)

    def poll(self, timeout=None):
        return self.updates.popleft() if self.updates else None


class NotificationStartupTests(unittest.TestCase):
    def setUp(self):
        self.writer = MemoryDataCache()
        self.reader = MemoryDataCache()
        self.reader.latest_values = self.writer.latest_values
        self.reader.revision_values = self.writer.revision_values
        self.reader.data_values = self.writer.data_values
        self.publisher = DataFabricSession(self.writer)
        self.transport = BufferedTransport()
        self.session = PollingDataFabricSession(self.reader, self.transport)

    def test_update_after_head_read_is_buffered_until_initial_cut_is_returned(self):
        old, _ = publish(self.publisher, "a", 100)
        load_latest = self.reader.load_latest
        updates = []

        def load_then_publish(data_id):
            self.assertTrue(self.transport.ready)
            before = load_latest(data_id)
            data, update = publish(self.publisher, "a", 200)
            updates.append(data)
            self.transport.send(update)
            return before

        with self.session:
            with patch.object(self.reader, "load_latest", side_effect=load_then_publish):
                sub = self.session.add_subscription("a")
            self.assertIs(self.session.poll(0)["a"], old)
            self.assertIs(self.session.poll(0)["a"], updates[0])
            self.assertEqual(sub.current_revision, ("a", 200, 0))
            self.assertEqual(self.session.poll(0), {})

    def test_overlap_with_fresher_image_cannot_regress_latest_or_current(self):
        old, first = publish(self.publisher, "a", 100)
        with self.session:
            self.transport.send(first)
            latest, second = publish(self.publisher, "a", 200)
            self.transport.send(second)
            sub = self.session.add_subscription("a")
            self.assertIs(self.session.poll(0)["a"], latest)
            for update in (first, second, first, second):
                self.transport.send(update)
            while self.transport.updates:
                self.assertEqual(self.session.poll(0), {})
                self.assertEqual(sub.latest_revision, ("a", 200, 0))
                self.assertEqual(sub.current_revision, ("a", 200, 0))
            self.assertIs(self.session.get_consistent_cut()["a"], latest)
            self.assertEqual(self.reader.latest("a"), ("a", 200, 0))

    def test_notifications_arriving_during_table_load_are_not_lost(self):
        old, _ = publish(self.publisher, "a", 100)
        load_version = self.reader.load_version
        newer = []

        def load_then_publish(version_id):
            data = load_version(version_id)
            next_data, update = publish(self.publisher, "a", 200)
            newer.append(next_data)
            self.transport.send(update)
            return data

        with self.session:
            self.session.add_subscription("a")
            with patch.object(self.reader, "load_version", side_effect=load_then_publish):
                self.assertIs(self.session.poll(0)["a"], old)
            self.assertIs(self.session.poll(0)["a"], newer[0])

    def test_older_revision_count_does_not_regress_root(self):
        _, first = publish(self.publisher, "a", 100)
        newer = replace(first, revision=(100, 3), previous_revision=None)
        with self.session:
            sub = self.session.add_subscription("a")
            self.session.poll(0)
            for update in (newer, first, newer, replace(newer, revision=(100, 1))):
                self.transport.send(update)
                self.assertEqual(self.session.poll(0), {})
                self.assertEqual(sub.current_revision, ("a", 100, 3))
                self.assertEqual(sub.latest_revision, ("a", 100, 3))

    def test_old_dependency_notification_does_not_invalidate_newer_tree(self):
        history = History()
        history.add("x", 1)
        history.add("x", 2)
        older = history.add("middle", 100, dependencies=(("x", 1),))
        newer = history.add("middle", 100, 1, dependencies=(("x", 2),))
        history.add("a", 100, dependencies=(("middle", 100),))
        cache = HistoryCache(history)
        manager = DataSubscriptionManager(cache)
        sub = manager.add_subscription("a")
        manager.get_consistent_cut()
        graph = sub.current(cache)
        for update in (older, newer, older):
            self.assertEqual(manager.apply_update(update), set())
            self.assertIs(sub.current(cache), graph)
            self.assertTrue(graph.has_node(("x", 2)))

    def test_historical_version_revision_can_still_release_waiting_roots(self):
        history = History()
        history.add("x", 1)
        history.add("x", 2)
        history.add("middle", 100, dependencies=(("x", 1),))
        history.add("middle", 200, dependencies=(("x", 2),))
        for root in ("a", "b"):
            history.add(root, 100)
            history.add(root, 200, dependencies=(("middle", 100),))
        history.add("peer", 100, dependencies=(("x", 2),))
        cache = HistoryCache(history)
        manager = DataSubscriptionManager(cache)
        manager.add_subscriptions(("a", "b", "peer"))
        manager.get_consistent_cut()
        update = history.add("middle", 100, 1, dependencies=(("x", 2),))
        self.assertEqual(manager.apply_update(update), {("a", 200), ("b", 200)})
        self.assertEqual(cache.latest("middle"), ("middle", 200, 0))
        self.assertTrue(history.consistent(current_cut(manager)))

    def test_old_notification_cannot_seed_an_incomplete_historical_head(self):
        history = History()
        history.add("x", 1)
        history.add("x", 2)
        older = history.add("middle", 100, dependencies=(("x", 1),))
        history.add("middle", 100, 3, dependencies=(("x", 2),))
        history.add("middle", 200)
        history.add("a", 100, dependencies=(("middle", 100),))
        cache = HistoryCache(history)
        # The image has read the current head but has not needed the old version.
        self.assertEqual(cache.latest("middle"), ("middle", 200, 0))
        cache.apply_update(older)
        manager = DataSubscriptionManager(cache)
        sub = manager.add_subscription("a")
        manager.get_consistent_cut()
        self.assertEqual(cache.get_latest_revision(("middle", 100)), ("middle", 100, 3))
        self.assertTrue(sub.current(cache).has_node(("x", 2)))

    def test_live_revisions_of_unvisited_historical_version_survive_history_lookup(self):
        for counts in ((1,), (1, 3, 1), (3, 1, 3)):
            with self.subTest(counts=counts):
                history = History()
                history.add("x", 1)
                history.add("x", 2)
                history.add("middle", 100, dependencies=(("x", 1),))
                history.add("middle", 200)  # Its history link records count zero.
                history.add("a", 100, dependencies=(("middle", 100),))
                history.add("b", 100, dependencies=(("x", 2),))
                cache = HistoryCache(history)
                manager = DataSubscriptionManager(cache)
                self.assertEqual(cache.latest("middle"), ("middle", 200, 0))
                cache.loads.clear()
                for count in counts:
                    update = history.add("middle", 100, count, dependencies=(("x", 2),))
                    self.assertEqual(manager.apply_update(update), set())
                # Notifications retain their information without eagerly loading
                # history for versions no subscription currently needs.
                self.assertEqual(cache.loads, [])
                self.assertEqual(cache.latest("middle"), ("middle", 200, 0))
                self.assertEqual(cache.get_latest_revision(("middle", 100)),
                                 ("middle", 100, max(counts)))
                manager.add_subscriptions(("a", "b"))
                self.assertEqual(manager.get_consistent_cut(), {("a", 100), ("b", 100)})
                self.assertTrue(history.consistent(current_cut(manager)))

    def test_historical_notification_for_unseen_id_cannot_hide_stored_head(self):
        history = History()
        history.add("x", 1)
        history.add("x", 2)
        history.add("middle", 100, dependencies=(("x", 1),))
        head = history.add("middle", 200)
        update = history.add("middle", 100, 1, dependencies=(("x", 2),))
        history.heads["middle"] = ("middle", *head.revision)
        cache = HistoryCache(history)
        manager = DataSubscriptionManager(cache)
        manager.apply_update(update)
        self.assertEqual(cache.loads, [])
        sub = manager.add_subscription("middle")
        self.assertEqual(sub.latest_revision, ("middle", 200, 0))
        self.assertEqual(manager.get_consistent_cut(), {("middle", 200)})
        self.assertEqual(cache.get_latest_revision(("middle", 100)), ("middle", 100, 1))

    def test_history_iterators_include_pending_and_resolved_revision_heads(self):
        history = History()
        history.add("middle", 100)
        history.add("middle", 200)
        cache = HistoryCache(history)
        self.assertEqual(cache.latest("middle"), ("middle", 200, 0))
        cache.apply_update(history.add("middle", 100, 1))
        expected = [("middle", 200, 0), ("middle", 100, 1)]
        self.assertEqual(list(cache.peek_version_history("middle")), expected[:1])
        self.assertEqual(list(cache.version_history("middle")), expected)
        cache.loads.clear()
        self.assertEqual(list(cache.peek_version_history("middle")), expected)
        self.assertEqual(cache.loads, [])
        self.assertEqual(cache.get_latest_revision(("middle", 100)), expected[1])
        # Resolving the hint into the per-version cache must preserve iteration.
        for history_fn in (cache.version_history, cache.peek_version_history):
            self.assertEqual(list(history_fn("middle")), expected)
        cache.apply_update(history.add("middle", 100, 2))
        expected[1] = ("middle", 100, 2)
        for history_fn in (cache.version_history, cache.peek_version_history):
            self.assertEqual(list(history_fn("middle")), expected)
