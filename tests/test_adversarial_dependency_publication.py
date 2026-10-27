"""Exercise real dependency publication against the independent raw-record oracle."""
from datetime import timedelta
from itertools import permutations, product
from random import Random
from unittest.mock import Mock, patch
import unittest

from pyarrow import table

from adversarial_cases import AS_OF, History, current_cut
from hgraph_fabric.data_fabric_session import DataFabricTransport, PollingDataFabricSession
from hgraph_fabric.data_subscription_manager import DataSubscriptionManager
from hgraph_fabric.transports.codec import decode_update, encode_update
from test_adversarial_transitions import Stream
from test_data_cache import MemoryDataCache


class PublishedStream(Stream):
    """Use storage writes and wire notifications instead of hand-built revisions.

    Stream's raw-record oracle checks committed cuts, forward-only selections,
    reported versions, dependency indexes, conflict retirement and duplicates.
    Both managers receive each notification before any further tree access.
    """

    def __init__(self, case, separate_reader=False):
        self.case = case
        self.history = History()
        self.writer_cache = MemoryDataCache()
        self.cache = MemoryDataCache() if separate_reader else self.writer_cache
        if separate_reader:
            for name in ("latest_values", "revision_values", "data_values"):
                setattr(self.cache, name, getattr(self.writer_cache, name))
        self.transport = Mock(spec=DataFabricTransport)
        self.writer = PollingDataFabricSession(self.writer_cache, self.transport)
        self.managers = []
        self.tick = 0

    def start(self, roots):
        self.managers = [DataSubscriptionManager(self.cache) for _ in range(2)]
        for manager in self.managers:
            manager.add_subscriptions(roots)
            manager.get_consistent_cut()
        self.check()

    def write(self, key, dependencies=(), weak=(), *, version=None):
        self.tick += 1
        before = self.history.heads.get(key)
        writes = len(self.writer_cache.writes)
        sends = self.transport.send.call_count
        data = None if version is None else table({"value": [version]}, metadata={b"version": str(version).encode()})
        returned, update = self.writer.publish(
            data, id=key, dependencies=dependencies, weak_dependencies=weak,
            as_of=AS_OF + timedelta(seconds=self.tick),
        )
        revision = (key, *update.revision)
        if revision == before:
            self.case.assertEqual(update, self.history.records[before])
            self.case.assertEqual(len(self.writer_cache.writes), writes)
            self.case.assertEqual(self.transport.send.call_count, sends)
            self.case.assertIs(returned, self.writer_cache.get_version(before[:2]))
            return set()
        self.case.assertEqual(self.transport.send.call_count, sends + 1)
        self.transport.send.assert_called_with(update)
        if version is None:
            self.case.assertEqual(revision, (key, before[1], before[2] + 1))
            self.case.assertEqual([entry[0] for entry in self.writer_cache.writes[writes:]], ["revision", "latest_revision"])
            self.case.assertIs(returned, self.writer_cache.get_version(before[:2]))
        self.history.records[revision] = update
        self.history.heads[key] = revision
        # Include the same encoding used by both broker adapters.
        wire_update = decode_update(encode_update(update))
        self.case.assertEqual(wire_update, update)
        return super().publish(wire_update) if self.managers else set()


class DependencyPublicationReviewTests(unittest.TestCase):
    def test_block_release_weak_changes_and_removal_through_real_publication(self):
        for order, separate, new_peer_table in product(permutations(("a", "b", "c")), (False, True), (False, True)):
            with self.subTest(order=order, separate=separate, new_peer_table=new_peer_table):
                stream = PublishedStream(self, separate)
                for version in (1, 2):
                    stream.write("x", version=version)
                stream.write("a", (("x", 1),), version=1)
                stream.write("b", (("x", 1),), version=1)
                stream.write("c", version=1)
                stream.start(order)
                original = current_cut(stream.managers[0])
                self.assertEqual(stream.write("a", (("x", 2),)), set())
                self.assertEqual(current_cut(stream.managers[0]), original)
                # A blocked count-only revision cannot prevent an unrelated data advance.
                self.assertEqual(stream.write("c", version=2), {("c", 2)})
                self.assertEqual(stream.write("b", (("x", 2),), version=2 if new_peer_table else None),
                                 {("b", 2)} if new_peer_table else set())
                self.assertEqual(current_cut(stream.managers[0]), {key: stream.history.heads[key] for key in order})
                self.assertEqual(stream.write("a", (("x", 2),)), set())  # Different timestamp only.
                self.assertEqual(stream.write("a", (("x", 2),), weak=(("missing-weak", 999),)), set())
                self.assertEqual(stream.write("a"), set())
                self.assertEqual(stream.write("a", (("x", 2),)), set())
                self.assertEqual(stream.managers[0]._subscriptions["a"].current_revision, ("a", 1, 4))
                # A pre-revision notification cannot undo the committed count.
                stream.publish(stream.history.records[("a", 1, 0)])
                self.assertEqual(stream.managers[0]._subscriptions["a"].current_revision, ("a", 1, 4))

    def test_soft_revision_releases_waiting_tables_or_counts_and_retires_watch(self):
        for order, separate, counts, diamond, remove in product(
            permutations(("a", "b", "peer")), (False, True), (False, True), (False, True), (False, True),
        ):
            with self.subTest(order=order, separate=separate, counts=counts, diamond=diamond, remove=remove):
                stream = PublishedStream(self, separate)
                for version in (1, 2):
                    stream.write("x", version=version)
                stream.write("middle", (("x", 1),), version=1)
                tip = ("middle", 1)
                if diamond:
                    for side in ("left", "right"):
                        stream.write(side, (tip,), version=1)
                    stream.write("outer", (("left", 1), ("right", 1)), version=1)
                    tip = ("outer", 1)
                for root in ("a", "b"):
                    stream.write(root, version=1)
                    stream.write(root, (tip,), version=None if counts else 2)
                stream.write("peer", (("x", 2),), version=1)
                stream.start(order)
                self.assertEqual(stream.managers[0]._subscriptions["a"].current_revision, ("a", 1, 0))
                self.assertIn(("middle", 1), stream.managers[0]._conflicts)
                result = stream.write("middle", () if remove else (("x", 2),))
                self.assertEqual(result, set() if counts else {("a", 2), ("b", 2)})
                for manager in stream.managers:
                    self.assertEqual(current_cut(manager), {key: stream.history.heads[key] for key in order})
                    self.assertEqual(manager._conflicts, {})
                # No-op protection must not reawaken the retired conflict.
                with patch("hgraph_fabric.data_subscription_manager.find_consistent_cut") as compare:
                    self.assertEqual(stream.write("middle", () if remove else (("x", 2),)), set())
                compare.assert_not_called()

    def test_cold_cut_uses_previous_versions_own_revision_head(self):
        for separate, order in product((False, True), permutations(("a", "b"))):
            with self.subTest(separate=separate, order=order):
                stream = PublishedStream(self, separate)
                for version in (1, 2, 3):
                    stream.write("x", version=version)
                stream.write("a", (("x", 1),), version=1)
                stream.write("a", (("x", 2),))
                stream.write("a", (("x", 3),))
                stream.write("a", (("x", 3),), version=2)
                stream.write("b", (("x", 2),), version=1)
                self.assertEqual(stream.cache.get_latest_revision(("a", 1)), ("a", 1, 2))
                # Force storage reads rather than reusing the writer's cached heads.
                stream.cache = MemoryDataCache()
                for name in ("latest_values", "revision_values", "data_values"):
                    setattr(stream.cache, name, getattr(stream.writer_cache, name))
                stream.start(order)
                self.assertEqual(current_cut(stream.managers[0]), {"a": ("a", 1, 1), "b": ("b", 1, 0)})
                self.assertEqual(stream.write("b", (("x", 3),)), {("a", 2)})
                self.assertEqual(current_cut(stream.managers[0]), {"a": ("a", 2, 0), "b": ("b", 1, 1)})

    def test_seeded_revision_publications_and_timestamp_only_retries(self):
        for seed in range(32):
            with self.subTest(seed=seed):
                rng = Random(seed)
                stream = PublishedStream(self, separate_reader=bool(seed % 2))
                for source in ("x", "y"):
                    stream.write(source, version=1)
                    stream.write("m-" + source, ((source, 1),), version=1)
                roots = ["a", "b", "c"]
                for root in roots:
                    stream.write(root, (("x", 1), ("y", 1)), version=1)
                rng.shuffle(roots)
                stream.start(roots)
                for step in range(48):
                    source = rng.choice(("x", "y"))
                    if step % 3 == 0:
                        stream.write(source, version=stream.history.heads[source][1] + 1)
                    elif step % 3 == 1:
                        stream.write("m-" + source, () if rng.randrange(3) == 0 else (stream.history.heads[source][:2],))
                    else:
                        root = rng.choice(roots)
                        deps = tuple(("m-" + key, 1) if rng.randrange(2) else stream.history.heads[key][:2]
                                     for key in ("x", "y") if rng.randrange(3))
                        stream.write(root, deps, weak=(("missing-weak", step),))
                        # Repeat identical dependency vectors with a newer as_of.
                        stream.write(root, deps, weak=(("missing-weak", step),))
                for root in roots:
                    stream.write(root, tuple(stream.history.heads[key][:2] for key in ("x", "y")))
                self.assertEqual(current_cut(stream.managers[0]), {key: stream.history.heads[key] for key in roots})

    def test_subscribed_dependency_revision_refreshes_containing_root(self):
        for separate, order in product((False, True), permutations(("middle", "root"))):
            with self.subTest(separate=separate, order=order):
                stream = PublishedStream(self, separate)
                for version in (1, 2):
                    stream.write("x", version=version)
                stream.write("middle", (("x", 1),), version=1)
                stream.write("root", (("middle", 1),), version=2)
                stream.start(order)
                self.assertEqual(stream.write("middle", (("x", 2),)), set())
                for manager in stream.managers:
                    self.assertEqual(current_cut(manager), {"middle": ("middle", 1, 1), "root": ("root", 1, 0)})
                    self.assertEqual(manager._subscriptions["root"].current(stream.cache).get_data_version_id("x"), ("x", 2))

    def test_poll_skips_count_only_notifications_and_returns_released_table(self):
        stream = PublishedStream(self)
        for version in (1, 2):
            stream.write("x", version=version)
        for root in ("a", "b"):
            stream.write(root, (("x", 1),), version=1)
        receiver = Mock(spec=DataFabricTransport)
        session = PollingDataFabricSession(stream.cache, receiver)
        session.add_subscriptions(("a", "b"))
        self.assertEqual(set(session.poll(0)), {"a", "b"})
        stream.transport.send.reset_mock()
        stream.write("a", (("x", 2),))
        stream.write("a", (("x", 2),))  # No notification for the timestamp-only retry.
        stream.write("b", (("x", 2),), version=2)
        messages = [decode_update(encode_update(call.args[0])) for call in stream.transport.send.call_args_list]
        self.assertEqual(len(messages), 2)
        receiver.poll.side_effect = messages
        changed = session.poll(1)
        self.assertEqual(set(changed), {"b"})
        self.assertIs(changed["b"], stream.writer_cache.get_version(("b", 2)))
        self.assertEqual(receiver.poll.call_count, 2)
        self.assertEqual(current_cut(session._subscriptions), {"a": ("a", 1, 1), "b": ("b", 2, 0)})

    def test_invalid_dependency_revision_fails_tree_build_without_selecting_current(self):
        for separate, malformed in product((False, True), ("missing", "conflicting")):
            with self.subTest(separate=separate, malformed=malformed):
                stream = PublishedStream(self, separate)
                for version in (1, 2):
                    stream.write("x", version=version)
                stream.write("middle", (("x", 1),), version=1)
                stream.write("a", (("x", 1),), version=1)
                stream.start(("a",))
                deps = (("missing", 1),) if malformed == "missing" else (("middle", 1), ("x", 2))
                # Publication persists metadata; the consuming tree validates it.
                # Unexpected metadata errors are fatal, with no recovery contract.
                with self.assertRaises(KeyError if malformed == "missing" else ValueError):
                    stream.write("a", deps)
                self.assertEqual(stream.managers[0]._subscriptions["a"].current_revision, ("a", 1, 0))

    def test_revision_notifications_queued_while_initial_trees_load(self):
        stream = PublishedStream(self)
        for version in (1, 2):
            stream.write("x", version=version)
        for root in ("a", "b"):
            stream.write(root, (("x", 1),), version=1)
        stream.transport.send.reset_mock()
        reader = MemoryDataCache()
        for name in ("latest_values", "revision_values", "data_values"):
            setattr(reader, name, getattr(stream.writer_cache, name))
        receiver = Mock(spec=DataFabricTransport)
        session = PollingDataFabricSession(reader, receiver)
        original_load = reader.load_revision
        published = False

        def load_revision(revision):
            nonlocal published
            if revision == ("a", 1, 0) and not published:
                published = True
                receiver.start.assert_called_once_with()
                # a's root head was read before these writes; b's will be read
                # afterwards. Reception retains both notifications in between.
                stream.write("a", (("x", 2),))
                stream.write("b", (("x", 2),))
                stream.write("a", (("x", 2),))  # No extra timestamp-only event.
            return original_load(revision)

        with session, patch.object(reader, "load_revision", side_effect=load_revision):
            session.add_subscriptions(("a", "b"))
            self.assertTrue(published)
            self.assertEqual(set(session.poll(0)), {"a", "b"})
            self.assertEqual(current_cut(session._subscriptions), {"a": ("a", 1, 0), "b": ("b", 1, 0)})
            messages = [decode_update(encode_update(call.args[0])) for call in stream.transport.send.call_args_list]
            self.assertEqual(len(messages), 2)
            receiver.poll.side_effect = [*messages, messages[0], stream.history.records[("a", 1, 0)], None]
            self.assertEqual(session.poll(1), {})  # Both revisions advance, neither table does.
            self.assertEqual(current_cut(session._subscriptions), {"a": ("a", 1, 1), "b": ("b", 1, 1)})
            self.assertTrue(stream.history.consistent(current_cut(session._subscriptions)))
            self.assertEqual(receiver.poll.call_count, 5)
        receiver.stop.assert_called_once_with()
