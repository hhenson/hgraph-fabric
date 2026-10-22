"""Opt-in contract tests against real brokers; also run in the broker CI job."""
from dataclasses import replace
import os
import unittest
from unittest.mock import patch
from uuid import uuid4

from hgraph_fabric.data_fabric_session import PollingDataFabricSession
from hgraph_fabric.transports import KafkaDataFabricTransport, NatsDataFabricTransport
from test_data_cache import MemoryDataCache
from test_data_fabric_session import publish
from test_notification_codec import UPDATE


class BrokerContract:
    def start_transport(self):
        transport = self.make_transport()
        self.addCleanup(transport.stop)
        transport.start()
        return transport

    def test_no_pre_start_replay_and_broadcast_including_local_echo(self):
        sender = self.start_transport()
        sender.send(UPDATE)
        self.assertEqual(sender.poll(5), UPDATE)
        first = self.start_transport()
        second = self.start_transport()
        self.assertIsNone(first.poll(0.05))
        self.assertIsNone(second.poll(0.05))
        update = replace(UPDATE, revision=(UPDATE.revision[0], 4))
        sender.send(update)
        for receiver in (sender, first, second):
            self.assertEqual(receiver.poll(5), update)
            self.assertIsNone(receiver.poll(0))

    def test_updates_during_initialization_and_snapshot_overlap(self):
        writer, reader = MemoryDataCache(), MemoryDataCache()
        reader.latest_values = writer.latest_values
        reader.revision_values = writer.revision_values
        reader.data_values = writer.data_values
        publisher = PollingDataFabricSession(writer, self.start_transport())
        old, first = publish(publisher, "a", 100)
        transport = self.start_transport()
        session = PollingDataFabricSession(reader, transport)
        load_latest = reader.load_latest
        captured = []

        def load_then_publish(data_id):
            old_revision = load_latest(data_id)
            captured.append(publish(publisher, "a", 200))
            return old_revision

        with patch.object(reader, "load_latest", side_effect=load_then_publish):
            sub = session.add_subscription("a")
        self.assertIs(session.poll(0)["a"], old)
        self.assertIs(session.poll(5)["a"], captured[0][0])
        # Already captured by storage, then duplicated and delivered out of date.
        for update in (first, captured[0][1], first):
            publisher._transport.send(update)
            # One notification with no delta: consume it at the transport level
            # so the test need not spend a session timeout per duplicate.
            self.assertEqual(session.apply_update(transport.poll(5)), {})
            self.assertEqual(sub.latest_revision, ("a", 200, 0))
            self.assertEqual(sub.current_revision, ("a", 200, 0))

    def test_image_can_include_a_notification_already_waiting_in_transport(self):
        writer, reader = MemoryDataCache(), MemoryDataCache()
        reader.latest_values = writer.latest_values
        reader.revision_values = writer.revision_values
        reader.data_values = writer.data_values
        publisher = PollingDataFabricSession(writer, self.start_transport())
        receiver = self.start_transport()
        expected, update = publish(publisher, "a", 100)
        session = PollingDataFabricSession(reader, receiver)
        sub = session.add_subscription("a")
        self.assertIs(session.poll(0)["a"], expected)
        self.assertEqual(session.apply_update(receiver.poll(5)), {})
        self.assertEqual(sub.current_revision, ("a", 100, 0))

    def test_restart_uses_a_fresh_live_boundary(self):
        sender = self.start_transport()
        receiver = self.start_transport()
        sender.send(UPDATE)
        receiver.stop()  # Leave its first notification unconsumed.
        receiver.start()
        self.assertIsNone(receiver.poll(0.1))
        next_update = replace(UPDATE, revision=(UPDATE.revision[0], 4))
        sender.send(next_update)
        self.assertEqual(receiver.poll(5), next_update)


@unittest.skipUnless(os.environ.get("HGRAPH_FABRIC_KAFKA"), "Set HGRAPH_FABRIC_KAFKA for broker tests")
class KafkaBrokerTests(BrokerContract, unittest.TestCase):
    def setUp(self):
        from confluent_kafka.admin import AdminClient, NewTopic

        self.servers = os.environ["HGRAPH_FABRIC_KAFKA"]
        self.topic = f"fabric-test-{uuid4().hex}"
        self.admin = AdminClient({"bootstrap.servers": self.servers})
        self.admin.create_topics(
            [NewTopic(self.topic, num_partitions=1, replication_factor=1)], request_timeout=30,
        )[self.topic].result(35)
        self.addCleanup(lambda: self.admin.delete_topics([self.topic])[self.topic].result(15))

    def make_transport(self):
        return KafkaDataFabricTransport(self.servers, self.topic)


@unittest.skipUnless(os.environ.get("HGRAPH_FABRIC_NATS"), "Set HGRAPH_FABRIC_NATS for broker tests")
class NatsBrokerTests(BrokerContract, unittest.TestCase):
    def setUp(self):
        self.subject = f"fabric.test.{uuid4().hex}"

    def make_transport(self):
        return NatsDataFabricTransport(os.environ["HGRAPH_FABRIC_NATS"], self.subject)
