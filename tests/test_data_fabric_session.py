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


def publish(session, data_id, version, dependencies=()):
    return session.publish(
        table({"value": [version]}, metadata={b"version": str(version).encode()}),
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
        self.assertIs(self.cache.peek_version(("a", 200)), data)
        self.assertEqual(update.dependencies, (("source", 1),))
        self.assertEqual(update.as_of, AS_OF)
        self.assertEqual(sub.current_revision, ("a", 100, 0))
        advanced = self.session.apply_update(update)
        self.assertIs(advanced["a"], data)
        self.assertEqual(sub.current_revision, ("a", 200, 0))

    def test_publish_uses_metadata_and_forwards_weak_dependencies(self):
        original = table({"value": []}, metadata={
            b"id": b"a", b"version": b"7", b"as_of": AS_OF.isoformat().encode(),
        })
        data, update = self.session.publish(original, weak_dependencies=(("reference", 3),))
        self.assertEqual(data.schema.metadata, original.schema.metadata)
        self.assertEqual(update.id, "a")
        self.assertEqual(update.revision, (7, 0))
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
                         {("a", 100), ("b", 100)})

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
                         {("a", 200), ("b", 200)})

    def test_revision_only_and_duplicate_updates_do_not_return_data(self):
        _, update = publish(self.session, "a", 100)
        sub = self.session.add_subscription("a")
        self.session.get_consistent_cut()
        revision = replace(update, revision=(100, 1), previous_revision=None)
        with patch.object(self.cache, "get_version", side_effect=AssertionError("No new table")):
            self.assertEqual(self.session.apply_update(revision), {})
            self.assertEqual(self.session.apply_update(revision), {})
        self.assertEqual(sub.current_revision, ("a", 100, 1))

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
        self.assertEqual(sub.current_revision, ("a", 100, 0))
        result = self.session.poll(0)
        self.assertIs(result["a"], data)
        self.assertEqual(sub.current_revision, ("a", 200, 0))
        self.assertEqual(self.session.poll(0), {})

    def test_poll_consumes_one_remote_update_without_sending_it_again(self):
        writer = MemoryDataCache()
        publisher = DataFabricSession(writer)
        a, update_a = publish(publisher, "a", 100)
        b, update_b = publish(publisher, "b", 200)
        self.cache.latest_values = writer.latest_values
        self.cache.revision_values = writer.revision_values
        self.cache.data_values = writer.data_values
        self.session.add_subscriptions(["a", "b"])
        self.transport.poll.side_effect = [update_a, update_b, None]
        result = self.session.poll(0)
        self.assertIs(result["a"], a)
        self.assertIs(result["b"], b)
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
        revision = replace(initial, revision=(100, 1), previous_revision=None)
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
