from collections import deque
from dataclasses import replace
from datetime import UTC, datetime
import unittest
from unittest.mock import Mock, patch

from pyarrow import table

from hgraph_fabric.data_fabric_session import (
    DataFabricSession, DataFabricTransport, PollingDataFabricSession,
)
from test_data_cache import MemoryDataCache


AS_OF = datetime(2026, 10, 21, tzinfo=UTC)


def publish(session, data_id, value, dependencies=()):
    return session.publish(
        table({"value": [value]}),
        id=data_id, dependencies=dependencies, as_of=AS_OF,
    )


class DataFabricSessionTests(unittest.TestCase):
    def setUp(self):
        self.cache = MemoryDataCache()
        self.session = DataFabricSession(self.cache)

    def test_registration_and_cut_return_cached_tables(self):
        a, _ = publish(self.session, "a", 100)
        b, _ = publish(self.session, "b", 200)
        subscription = self.session.add_subscription("a")
        self.session.add_subscriptions(iter(["b", "a"]))
        self.assertIs(self.session.add_subscription("a"), subscription)
        self.cache.loads.clear()
        cut = self.session.get_consistent_cut()
        self.assertEqual(set(cut), {"a", "b"})
        self.assertIs(cut["a"], a)
        self.assertIs(cut["b"], b)
        self.assertEqual(self.cache.loads, [])

    def test_publish_returns_cache_result_for_external_delivery(self):
        publish(self.session, "source", 1)
        publish(self.session, "a", 100)
        sub = self.session.add_subscription("a")
        self.session.get_consistent_cut()
        data, update = publish(self.session, "a", 200, (("source", 1),))
        self.assertIs(self.cache.peek_version(("a", 2)), data)
        self.assertEqual(update.dependencies, (("source", 1),))
        self.assertEqual(update.as_of, AS_OF)
        self.assertEqual(sub.current_revision, ("a", 1, 0))
        advanced = self.session.apply_update(update)
        self.assertIs(advanced["a"], data)
        self.assertEqual(sub.current_revision, ("a", 2, 0))

    def test_publish_uses_metadata_and_forwards_weak_dependencies(self):
        original = table({"value": []}, metadata={
            b"id": b"a", b"version": b"7", b"as_of": AS_OF.isoformat().encode(),
        })
        data, update = self.session.publish(original, weak_dependencies=(("reference", 3),))
        self.assertEqual(data.schema.metadata, original.schema.metadata | {b"version": b"1"})
        self.assertEqual(update.id, "a")
        self.assertEqual(update.revision, (1, 0))
        self.assertEqual(update.as_of, AS_OF)
        self.assertEqual(update.dependencies, ())
        self.assertEqual(update.weak_dependencies, (("reference", 3),))

    def test_initial_cut_loads_selected_historical_version(self):
        publish(self.session, "x", 1)
        publish(self.session, "x", 2)
        a100, _ = publish(self.session, "a", 100, (("x", 1),))
        publish(self.session, "a", 200, (("x", 2),))
        b100, _ = publish(self.session, "b", 100, (("x", 1),))
        reader = MemoryDataCache()
        reader.latest_values = self.cache.latest_values
        reader.revision_values = self.cache.revision_values
        reader.data_values = self.cache.data_values
        session = DataFabricSession(reader)
        session.add_subscriptions(["a", "b"])
        cut = session.get_consistent_cut()
        self.assertIs(cut["a"], a100)
        self.assertIs(cut["b"], b100)
        self.assertEqual({key for kind, key in reader.loads if kind == "data"},
                         {("a", 1), ("b", 1)})

    def test_remote_updates_load_only_advanced_tables_and_release_waiting_peer(self):
        publish(self.session, "x", 1)
        publish(self.session, "a", 100, (("x", 1),))
        publish(self.session, "b", 100, (("x", 1),))
        self.session.add_subscriptions(["a", "b"])
        self.session.get_consistent_cut()
        writer = MemoryDataCache()
        writer.latest_values = self.cache.latest_values
        writer.revision_values = self.cache.revision_values
        writer.data_values = self.cache.data_values
        publisher = DataFabricSession(writer)
        self.cache.loads.clear()
        _, x = publish(publisher, "x", 2)
        self.assertEqual(self.session.apply_update(x), {})
        a, update_a = publish(publisher, "a", 200, (("x", 2),))
        self.assertEqual(self.session.apply_update(update_a), {})
        self.assertFalse(any(kind == "data" for kind, _ in self.cache.loads))
        b, update_b = publish(publisher, "b", 200, (("x", 2),))
        advanced = self.session.apply_update(update_b)
        self.assertIs(advanced["a"], a)
        self.assertIs(advanced["b"], b)
        self.assertEqual({key for kind, key in self.cache.loads if kind == "data"},
                         {("a", 2), ("b", 2)})

    def test_revision_only_and_duplicate_updates_do_not_return_data(self):
        _, update = publish(self.session, "a", 100)
        sub = self.session.add_subscription("a")
        self.session.get_consistent_cut()
        revision = replace(update, revision=(1, 1), previous_revision=None)
        with patch.object(self.cache, "get_version", side_effect=AssertionError("No new table")):
            self.assertEqual(self.session.apply_update(revision), {})
            self.assertEqual(self.session.apply_update(revision), {})
        self.assertEqual(sub.current_revision, ("a", 1, 1))

    def test_empty_session_and_unsubscribed_update_return_no_tables(self):
        self.assertEqual(self.session.get_consistent_cut(), {})
        _, update = publish(self.session, "a", 100)
        self.assertEqual(self.session.apply_update(update), {})


class PollingDataFabricSessionTests(unittest.TestCase):
    def setUp(self):
        self.cache = MemoryDataCache()
        self.transport = Mock(spec=DataFabricTransport)
        self.session = PollingDataFabricSession(self.cache, self.transport)

    def test_lifecycle_and_timeout_delegate_to_transport(self):
        self.session.start()
        self.transport.start.assert_called_once_with()
        self.transport.poll.return_value = None
        for timeout in (None, 0, 0.25):
            self.assertEqual(self.session.poll(timeout), {})
            self.transport.poll.assert_called_with(timeout)
        self.session.stop()
        self.transport.stop.assert_called_once_with()

    def test_context_manager_starts_and_stops_session(self):
        with self.session as session:
            self.assertIs(session, self.session)
            self.transport.start.assert_called_once_with()
            self.transport.stop.assert_not_called()
        self.transport.stop.assert_called_once_with()

    def test_poll_initializes_single_or_bulk_subscriptions_without_a_message(self):
        publisher = DataFabricSession(self.cache)
        a, _ = publish(publisher, "a", 100)
        b, _ = publish(publisher, "b", 200)
        for bulk in (False, True):
            with self.subTest(bulk=bulk):
                session = PollingDataFabricSession(self.cache, self.transport)
                if bulk:
                    session.add_subscriptions(iter(["a", "b"]))
                else:
                    session.add_subscription("a")
                result = session.poll()
                self.assertEqual(set(result), {"a", "b"} if bulk else {"a"})
                self.assertIs(result["a"], a)
                if bulk:
                    self.assertIs(result["b"], b)
        self.transport.poll.assert_not_called()
        self.transport.send.assert_not_called()

    def test_poll_initializes_added_subscription_at_no_data_without_downgrading(self):
        publisher = DataFabricSession(self.cache)
        publish(publisher, "x", 1)
        publish(publisher, "x", 2)
        a100, _ = publish(publisher, "a", 100, (("x", 1),))
        a200, _ = publish(publisher, "a", 200, (("x", 2),))
        b100, _ = publish(publisher, "b", 100, (("x", 1),))
        a = self.session.add_subscription("a")
        self.assertIs(self.session.poll(0)["a"], a200)
        b = self.session.add_subscription("b")
        self.assertIsNone(b.current_revision)
        result = self.session.poll(0)
        self.assertEqual(set(result), {"a"})
        self.assertIs(result["a"], a200)
        self.assertEqual(a.current_revision, ("a", 2, 0))
        self.assertEqual(b.current_revision, ("b", 0, 0))
        self.transport.poll.assert_not_called()

    def test_duplicate_registration_does_not_repeat_initialization(self):
        publish(DataFabricSession(self.cache), "a", 100)
        self.session.add_subscription("a")
        self.session.poll(0)
        self.session.add_subscriptions(["a", "a"])
        self.transport.poll.return_value = None
        with patch.object(self.session, "get_consistent_cut", side_effect=AssertionError("Already initialized")):
            self.assertEqual(self.session.poll(0), {})
        self.transport.poll.assert_called_once_with(0)

    def test_explicit_cut_initializes_before_poll(self):
        publish(DataFabricSession(self.cache), "a", 100)
        self.session.add_subscription("a")
        self.session.get_consistent_cut()
        self.transport.poll.return_value = None
        self.assertEqual(self.session.poll(0), {})
        self.transport.poll.assert_called_once_with(0)

    def test_partial_initial_cut_then_update_aligns_roots(self):
        publisher = DataFabricSession(self.cache)
        publish(publisher, "x", 1)
        publish(publisher, "x", 2)
        publish(publisher, "a", 100, (("x", 1),))
        b, _ = publish(publisher, "b", 100, (("x", 2),))
        self.session.add_subscriptions(["a", "b"])
        self.assertEqual(set(self.session.poll(0)), {"a"})
        a, update = publish(publisher, "a", 200, (("x", 2),))
        self.transport.poll.return_value = update
        result = self.session.poll(0)
        self.assertIs(result["a"], a)
        self.assertIs(result["b"], b)
        self.transport.poll.assert_called_once_with(0)

    def test_all_no_data_initial_cut_uses_remaining_timeout(self):
        self.session.add_subscriptions(["a", "b"])
        self.transport.poll.return_value = None
        with patch("hgraph_fabric.data_fabric_session.monotonic", side_effect=[10.0, 10.25]):
            self.assertEqual(self.session.poll(1.0), {})
        self.transport.poll.assert_called_once_with(0.75)
        self.assertEqual(self.session.add_subscription("a").current_revision, ("a", 0, 0))
        self.assertEqual(self.session.add_subscription("b").current_revision, ("b", 0, 0))

    def test_initialization_storage_failure_is_not_treated_as_unavailable_cut(self):
        publish(DataFabricSession(self.cache), "a", 100)
        self.session.add_subscription("a")
        error = OSError("Storage unavailable")
        with patch.object(self.cache, "get_version", side_effect=error):
            with self.assertRaises(OSError) as failure:
                self.session.poll(0)
        self.assertIs(failure.exception, error)
        self.transport.poll.assert_not_called()

    def test_context_manager_stops_without_suppressing_exception(self):
        error = RuntimeError("Processing failed")
        with self.assertRaises(RuntimeError) as raised:
            with self.session:
                raise error
        self.assertIs(raised.exception, error)
        self.transport.start.assert_called_once_with()
        self.transport.stop.assert_called_once_with()

    def test_publish_then_poll_delivers_local_update_once(self):
        messages = deque()
        def send(update):
            self.assertEqual(self.cache.latest_values[update.id], (update.id, *update.revision))
            self.assertIn((update.id, *update.revision), self.cache.revision_values)
            self.assertIn((update.id, update.revision[0]), self.cache.data_values)
            messages.append(update)
        self.transport.send.side_effect = send
        self.transport.poll.side_effect = lambda timeout: messages.popleft() if messages else None
        publish(self.session, "a", 100)
        self.assertEqual(self.session.poll(0), {})
        sub = self.session.add_subscription("a")
        self.session.get_consistent_cut()
        data, update = publish(self.session, "a", 200)
        self.transport.send.assert_called_with(update)
        self.assertEqual(sub.current_revision, ("a", 1, 0))
        result = self.session.poll(0)
        self.assertIs(result["a"], data)
        self.assertEqual(sub.current_revision, ("a", 2, 0))
        self.assertEqual(self.session.poll(0), {})

    def test_poll_consumes_one_remote_update_without_sending_it_again(self):
        writer = MemoryDataCache()
        publisher = DataFabricSession(writer)
        publish(publisher, "a", 100)
        _, update_b = publish(publisher, "b", 200)
        self.cache.latest_values = writer.latest_values
        self.cache.revision_values = writer.revision_values
        self.cache.data_values = writer.data_values
        self.session.add_subscriptions(["a", "b"])
        self.session.get_consistent_cut()
        a, update_a = publish(publisher, "a", 300)
        self.transport.poll.side_effect = [update_a, update_b, None]
        result = self.session.poll(0)
        self.assertIs(result["a"], a)
        self.assertEqual(set(result), {"a"})
        self.assertEqual(self.transport.poll.call_count, 1)
        self.assertEqual(self.session.poll(0), {})
        self.assertEqual(self.session.poll(0), {})
        self.transport.send.assert_not_called()

    def test_poll_retries_empty_deltas_with_remaining_timeout(self):
        publisher = DataFabricSession(self.cache)
        publish(publisher, "a", 100)
        self.session.add_subscription("a")
        self.session.get_consistent_cut()
        _, unrelated = publish(publisher, "other", 100)
        data, advanced = publish(publisher, "a", 200)
        self.transport.poll.side_effect = [unrelated, advanced]
        with patch("hgraph_fabric.data_fabric_session.monotonic", side_effect=[10.0, 10.4]):
            result = self.session.poll(1.0)
        self.assertIs(result["a"], data)
        self.assertEqual(self.transport.poll.call_count, 2)
        self.assertEqual(self.transport.poll.call_args_list[0].args, (1.0,))
        self.assertAlmostEqual(self.transport.poll.call_args_list[1].args[0], 0.6)
        self.transport.send.assert_not_called()

    def test_poll_stops_at_deadline_after_notification_without_delta(self):
        _, update = publish(DataFabricSession(self.cache), "other", 100)
        self.transport.poll.return_value = update
        with patch("hgraph_fabric.data_fabric_session.monotonic", side_effect=[10.0, 11.0]):
            self.assertEqual(self.session.poll(1.0), {})
        self.transport.poll.assert_called_once_with(1.0)

    def test_poll_without_timeout_waits_through_revision_only_updates(self):
        publisher = DataFabricSession(self.cache)
        _, initial = publish(publisher, "a", 100)
        self.session.add_subscription("a")
        self.session.get_consistent_cut()
        revision = replace(initial, revision=(1, 1), previous_revision=None)
        data, update = publish(publisher, "a", 200)
        self.transport.poll.side_effect = [revision, update]
        result = self.session.poll()
        self.assertIs(result["a"], data)
        self.assertEqual([call.args for call in self.transport.poll.call_args_list], [(None,), (None,)])

    def test_negative_timeout_is_rejected_without_receiving(self):
        with self.assertRaises(ValueError):
            self.session.poll(-1)
        self.transport.poll.assert_not_called()

    def test_write_failure_does_not_send_and_transport_errors_propagate(self):
        with patch.object(self.cache, "write_version", side_effect=OSError("Storage unavailable")):
            with self.assertRaises(OSError):
                publish(self.session, "a", 100)
        self.transport.send.assert_not_called()
        self.transport.send.side_effect = OSError("Send failed")
        with self.assertRaises(OSError):
            publish(self.session, "a", 100)
        self.transport.poll.side_effect = OSError("Poll failed")
        with self.assertRaises(OSError):
            self.session.poll(0)

    def test_dependency_publication_sends_only_changed_revisions(self):
        data, initial = publish(self.session, "a", 100)
        self.transport.send.reset_mock()
        self.cache.writes.clear()
        returned, unchanged = self.session.publish(id="a", as_of=AS_OF)
        self.assertIs(returned, data)
        self.assertEqual(unchanged, initial)
        self.transport.send.assert_not_called()
        self.assertEqual(self.cache.writes, [])
        returned, update = self.session.publish(id="a", weak_dependencies=(("reference", 1),), as_of=AS_OF)
        self.assertIs(returned, data)
        self.assertEqual(update.revision, (1, 1))
        self.transport.send.assert_called_once_with(update)
        self.transport.send.reset_mock()
        self.session.publish(id="a", weak_dependencies=(("reference", 1),))
        self.transport.send.assert_not_called()

    def test_failed_dependency_publication_does_not_send(self):
        publish(self.session, "a", 100)
        self.transport.send.reset_mock()
        with patch.object(self.cache, "write_revision", side_effect=OSError("Storage unavailable")):
            with self.assertRaises(OSError):
                self.session.publish(id="a", weak_dependencies=(("reference", 1),))
        self.transport.send.assert_not_called()

    def test_dependency_revision_refreshes_subscription_without_new_table_delta(self):
        publisher = DataFabricSession(self.cache)
        publish(publisher, "source", 1)
        publish(publisher, "source", 2)
        data, _ = publish(publisher, "a", 100, (("source", 1),))
        sub = self.session.add_subscription("a")
        self.assertIs(self.session.poll(0)["a"], data)
        _, update = self.session.publish(id="a", dependencies=(("source", 2),))
        self.transport.poll.return_value = update
        self.assertEqual(self.session.poll(0), {})
        self.assertEqual(sub.current_revision, ("a", 1, 1))
        self.assertEqual(sub.current(self.cache).get_data_version_id("source"), ("source", 2))
