"""A temporary disagreement between sibling records must not kill a subscriber."""
from itertools import permutations
import unittest
from unittest.mock import patch

from hgraph_fabric.data_subscription_manager import DataSubscriptionManager
from adversarial_cases import History, HistoryCache, current_cut
import test_notification_startup
from test_data_fabric_session import publish


class InconsistentDependencyUpdateTests(unittest.TestCase):
    def seed(self, peer=False):
        history = History()
        for version in (1, 2):
            history.add("universe", version)
        for sibling in ("market", "window"):
            history.add(sibling, 100, dependencies=(("universe", 1),))
        dependencies = (("market", 100), ("window", 100))
        history.add("root", 100, dependencies=dependencies)
        if peer:
            history.add("peer", 100)
        cache = HistoryCache(history)
        manager = DataSubscriptionManager(cache)
        manager.add_subscriptions(("root", "peer") if peer else ("root",))
        manager.get_consistent_cut()
        return history, cache, manager, dependencies

    def test_pending_cut_waits_and_resumes_on_either_sibling_order(self):
        for order in permutations(("market", "window")):
            for peer in (False, True):
                for version, count in ((200, 0), (100, 1)):
                    with self.subTest(order=order, peer=peer, version=version):
                        history, cache, manager, dependencies = self.seed(peer)
                        before = current_cut(manager)
                        manager.apply_update(history.add(order[0], 100, 1, (("universe", 2),)))
                        update = history.add("root", version, count, dependencies)
                        self.assertEqual(manager.apply_update(update), set())
                        self.assertEqual(current_cut(manager), before)
                        for universe in (1, 2):
                            self.assertEqual(manager._dependents[("universe", universe)], {"root"})
                        # Neither an unrelated item nor another sibling version
                        # can repair the exact dependency path that blocked us.
                        with patch.object(manager, "_try_advance", wraps=manager._try_advance) as advance:
                            manager.apply_update(history.add("unrelated", 1))
                            manager.apply_update(history.add(order[1], 200))
                            advance.assert_not_called()
                        healed = history.add(order[1], 100, 1, (("universe", 2),))
                        self.assertEqual(manager.apply_update(healed), {("root", 200)} if count == 0 else set())
                        self.assertEqual(manager._subscriptions["root"].current_revision, ("root", version, count))
                        self.assertTrue(history.consistent(current_cut(manager)))
                        with patch.object(manager, "_try_advance", wraps=manager._try_advance) as advance:
                            self.assertEqual(manager.apply_update(healed), set())
                            advance.assert_not_called()

    def test_conflict_in_another_subscription_retries_the_original_update(self):
        history, cache, manager, dependencies = self.seed(peer=True)
        manager.apply_update(history.add("market", 100, 1, (("universe", 2),)))
        before = current_cut(manager)
        self.assertEqual(manager.apply_update(history.add("peer", 200)), set())
        self.assertEqual(current_cut(manager), before)
        self.assertEqual(manager.apply_update(history.add("window", 100, 1, (("universe", 2),))),
                         {("peer", 200)})
        self.assertTrue(history.consistent(current_cut(manager)))

    def test_explicit_cut_retains_last_accepted_selection_while_waiting(self):
        history, cache, manager, dependencies = self.seed()
        before = current_cut(manager)
        manager.apply_update(history.add("market", 100, 1, (("universe", 2),)))
        self.assertEqual(manager.get_consistent_cut(), {("root", 100)})
        self.assertEqual(current_cut(manager), before)

    def test_bad_storage_is_not_hidden_by_an_earlier_sibling_mismatch(self):
        for broken in ("missing", "malformed"):
            with self.subTest(broken=broken):
                history, cache, manager, dependencies = self.seed()
                history.add("market", 100, 1, (("universe", 2),))
                history.add("root", 200, dependencies=(*dependencies, ("broken", 1)))
                if broken == "malformed":
                    history.add("broken", 1, dependencies=(("universe", 1), ("universe", 2)))
                fresh = DataSubscriptionManager(HistoryCache(history))
                with self.assertRaises(KeyError if broken == "missing" else ValueError):
                    fresh.add_subscription("root")
                self.assertEqual(fresh._subscriptions, {})


class InconsistentDependencyPollingTests(unittest.TestCase):
    setUp = test_notification_startup.NotificationStartupTests.setUp

    def test_live_subscriber_keeps_old_table_until_dependencies_converge(self):
        publish(self.publisher, "universe", 1)
        publish(self.publisher, "universe", 2)
        for sibling in ("market", "window"):
            publish(self.publisher, sibling, 100, (("universe", 1),))
        dependencies = (("market", 100), ("window", 100))
        initial, _ = publish(self.publisher, "root", 100, dependencies)
        with self.session:
            root = self.session.add_subscription("root")
            self.assertIs(self.session.poll(0)["root"], initial)
            _, market = self.publisher.publish(id="market", dependencies=(("universe", 2),))
            self.transport.send(market)
            self.assertEqual(self.session.poll(0), {})
            newer, update = publish(self.publisher, "root", 200, dependencies)
            self.transport.send(update)
            self.assertEqual(self.session.poll(0), {})
            self.assertEqual(root.current_revision, ("root", 100, 0))
            self.assertIs(self.session.get_consistent_cut()["root"], initial)
            self.assertNotIn(("data", ("root", 200)), self.reader.loads)
            _, window = self.publisher.publish(id="window", dependencies=(("universe", 2),))
            self.transport.send(window)
            self.assertIs(self.session.poll(0)["root"], newer)
            self.assertEqual(root.current_revision, ("root", 200, 0))
            self.transport.send(window)
            self.assertEqual(self.session.poll(0), {})

    def test_startup_waits_for_sibling_and_then_delivers_initial_table(self):
        publish(self.publisher, "universe", 1)
        publish(self.publisher, "universe", 2)
        publish(self.publisher, "market", 100, (("universe", 2),))
        publish(self.publisher, "window", 100, (("universe", 1),))
        initial, _ = publish(self.publisher, "root", 100, (("market", 100), ("window", 100)))
        with self.session:
            root = self.session.add_subscription("root")
            self.assertEqual(self.session.poll(0), {})
            self.assertIsNone(root.current_revision)
            _, healed = self.publisher.publish(id="window", dependencies=(("universe", 2),))
            self.transport.send(healed)
            self.assertIs(self.session.poll(0)["root"], initial)
            self.assertEqual(root.current_revision, ("root", 100, 0))
