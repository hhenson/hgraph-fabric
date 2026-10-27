"""A temporary disagreement between sibling records must not kill a subscriber."""
from adversarial_cases import current_choices

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

    def test_cut_resolves_sibling_revision_history_in_either_delivery_order(self):
        for order in permutations(("market", "window")):
            for peer in (False, True):
                for version, count in ((200, 0), (100, 1)):
                    with self.subTest(order=order, peer=peer, version=version):
                        history, cache, manager, dependencies = self.seed(peer)
                        before = current_cut(manager)
                        manager.apply_update(history.add(order[0], 100, 1, (("universe", 2),)))
                        update = history.add("root", version, count, dependencies)
                        self.assertEqual(manager.apply_update(update), {("root", 200)} if count == 0 else set())
                        self.assertEqual(current_cut(manager)["root"], ("root", version, count))
                        self.assertEqual(manager._subscriptions["root"].current_dependency_revisions[(order[0], 100)],
                                         (order[0], 100, 0))
                        for universe in (1, 2):
                            self.assertEqual(manager._dependents[("universe", universe)], {"root"})
                        # Neither an unrelated item nor another sibling version
                        # can repair the exact dependency path that blocked us.
                        with patch.object(manager, "_try_advance", wraps=manager._try_advance) as advance:
                            manager.apply_update(history.add("unrelated", 1))
                            manager.apply_update(history.add(order[1], 200))
                            advance.assert_not_called()
                        healed = history.add(order[1], 100, 1, (("universe", 2),))
                        self.assertEqual(manager.apply_update(healed), set())
                        self.assertEqual(manager._subscriptions["root"].current_revision, ("root", version, count))
                        self.assertTrue(history.consistent(current_cut(manager), current_choices(manager)))
                        with patch.object(manager, "_try_advance", wraps=manager._try_advance) as advance:
                            self.assertEqual(manager.apply_update(healed), set())
                            advance.assert_not_called()

    def test_resolving_another_subscriptions_history_allows_the_update(self):
        history, cache, manager, dependencies = self.seed(peer=True)
        manager.apply_update(history.add("market", 100, 1, (("universe", 2),)))
        before = current_cut(manager)
        self.assertEqual(manager.apply_update(history.add("peer", 200)), {("peer", 200)})
        self.assertEqual(current_cut(manager), before | {"peer": ("peer", 200, 0)})
        self.assertEqual(manager._subscriptions["root"].current_dependency_revisions[("market", 100)], ("market", 100, 0))
        self.assertEqual(manager.apply_update(history.add("window", 100, 1, (("universe", 2),))), set())
        self.assertTrue(history.consistent(current_cut(manager), current_choices(manager)))

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

    def test_historical_dependency_notification_retries_blocked_proposal(self):
        for initialized in (False, True):
            with self.subTest(initialized=initialized):
                self.setUp()
                for version in (1, 2, 3):
                    publish(self.publisher, "x", version)
                publish(self.publisher, "p", 1, (("x", 3),))
                publish(self.publisher, "m", 1, (("p", 1),))
                self.publisher.publish(id="m", dependencies=(("x", 2),))
                publish(self.publisher, "n", 1, (("x", 1),))
                old, _ = publish(self.publisher, "a", 1)
                with self.session:
                    if initialized:
                        root = self.session.add_subscription("a")
                        self.assertIs(self.session.poll(0)["a"], old)
                    newer, update = publish(self.publisher, "a", 2, (("m", 1), ("n", 1)))
                    if not initialized:
                        root = self.session.add_subscription("a")
                        # Its retained first publication already forms a cut.
                        self.assertIs(self.session.poll(0)["a"], old)
                    self.transport.send(update)
                    self.assertEqual(self.session.poll(0), {})
                    self.assertEqual(root.current_revision, ("a", 1, 0))

                    # Latest M has no P path. Search discovers P only through
                    # M/r0, so its notification must wake the failed proposal.
                    _, healed = self.publisher.publish(id="p", dependencies=())
                    self.transport.send(healed)
                    self.assertIs(self.session.poll(0)["a"], newer)
                    self.assertEqual(root.current_revision, ("a", 2, 0))
                    self.assertEqual(root.current_dependency_revisions[("m", 1)], ("m", 1, 0))
                    self.assertEqual(root.current_dependency_revisions[("p", 1)], ("p", 1, 1))
                    self.transport.send(healed)
                    self.assertEqual(self.session.poll(0), {})

    def test_live_subscriber_selects_new_table_using_coherent_sibling_revisions(self):
        publish(self.publisher, "universe", 1)
        publish(self.publisher, "universe", 2)
        for sibling in ("market", "window"):
            publish(self.publisher, sibling, 1, (("universe", 1),))
        dependencies = (("market", 1), ("window", 1))
        initial, _ = publish(self.publisher, "root", 1, dependencies)
        with self.session:
            root = self.session.add_subscription("root")
            self.assertIs(self.session.poll(0)["root"], initial)
            _, market = self.publisher.publish(id="market", dependencies=(("universe", 2),))
            self.transport.send(market)
            self.assertEqual(self.session.poll(0), {})
            newer, update = publish(self.publisher, "root", 2, dependencies)
            self.transport.send(update)
            self.assertIs(self.session.poll(0)["root"], newer)
            self.assertEqual(root.current_revision, ("root", 2, 0))
            self.assertEqual(root.current_dependency_revisions[("market", 1)], ("market", 1, 0))
            self.assertIs(self.session.get_consistent_cut()["root"], newer)
            _, window = self.publisher.publish(id="window", dependencies=(("universe", 2),))
            self.transport.send(window)
            self.assertEqual(self.session.poll(0), {})
            self.assertEqual(root.current_revision, ("root", 2, 0))
            self.transport.send(window)
            self.assertEqual(self.session.poll(0), {})

    def test_startup_waits_for_sibling_and_then_delivers_initial_table(self):
        publish(self.publisher, "universe", 1)
        publish(self.publisher, "universe", 2)
        publish(self.publisher, "market", 1, (("universe", 2),))
        publish(self.publisher, "window", 1, (("universe", 1),))
        initial, _ = publish(self.publisher, "root", 1, (("market", 1), ("window", 1)))
        with self.session:
            root = self.session.add_subscription("root")
            self.assertEqual(self.session.poll(0), {})
            self.assertEqual(root.current_revision, ("root", 0, 0))
            _, healed = self.publisher.publish(id="window", dependencies=(("universe", 2),))
            self.transport.send(healed)
            self.assertIs(self.session.poll(0)["root"], initial)
            self.assertEqual(root.current_revision, ("root", 1, 0))
