import asyncio
from concurrent.futures import ThreadPoolExecutor
from importlib.util import find_spec
from threading import Event
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

from hgraph_fabric.transports import KafkaDataFabricTransport, NatsDataFabricTransport
from hgraph_fabric.transports.codec import encode_update
from test_notification_codec import UPDATE


@unittest.skipUnless(find_spec("confluent_kafka"), "Install the kafka extra")
class KafkaTransportTests(unittest.TestCase):
    def setUp(self):
        self.consumer = Mock()
        self.consumer.list_topics.return_value = SimpleNamespace(topics={
            "updates": SimpleNamespace(error=None, partitions={0: object()}),
        })
        self.consumer.get_watermark_offsets.return_value = (0, 12)
        self.consumer.poll.return_value = None
        self.producer = Mock()
        self.producer.flush.return_value = 0
        self.consumer_factory = self.enterContext(patch(
            "confluent_kafka.Consumer", return_value=self.consumer,
        ))
        self.producer_factory = self.enterContext(patch(
            "confluent_kafka.Producer", return_value=self.producer,
        ))
        self.transport = KafkaDataFabricTransport("localhost:9092", "updates", timeout=0.2)
        self.addCleanup(self.transport.stop)

    def test_start_pins_numeric_boundary_before_first_poll_and_never_commits(self):
        self.transport.start()
        assigned = self.consumer.assign.call_args.args[0][0]
        self.assertEqual((assigned.topic, assigned.partition, assigned.offset), ("updates", 0, 12))
        # Watermark advances before first poll; reception must remain at 12.
        self.consumer.get_watermark_offsets.return_value = (0, 14)
        self.consumer.poll.return_value = Mock(error=lambda: None, value=lambda: encode_update(UPDATE))
        self.assertEqual(self.transport.poll(0), UPDATE)
        self.consumer.get_watermark_offsets.assert_called_once()
        self.consumer.subscribe.assert_not_called()
        self.consumer.commit.assert_not_called()
        config = self.consumer_factory.call_args.args[0]
        self.assertFalse(config["enable.auto.commit"])
        self.assertEqual(config["auto.offset.reset"], "error")

    def test_poll_timeouts_and_local_send_wire_format(self):
        self.transport.start()
        for timeout, expected in ((None, -1), (0, 0), (0.01, 0.01)):
            self.assertIsNone(self.transport.poll(timeout))
            self.consumer.poll.assert_called_with(expected)
        self.transport.send(UPDATE)
        args = self.producer.produce.call_args
        self.assertEqual(args.args, ("updates",))
        self.assertEqual(args.kwargs["value"], encode_update(UPDATE))
        self.assertEqual(args.kwargs["partition"], 0)
        self.producer.flush.assert_called_with(0.2)

    def test_delivery_failure_and_timeout_propagate(self):
        from confluent_kafka import KafkaError, KafkaException

        self.transport.start()
        self.producer.produce.side_effect = lambda *a, **kw: kw["on_delivery"](
            KafkaError(KafkaError._MSG_TIMED_OUT), None,
        )
        with self.assertRaises(KafkaException):
            self.transport.send(UPDATE)
        with self.assertRaises(KafkaException):
            self.transport.poll(0)
        self.transport.stop()
        self.producer.produce.side_effect = None
        self.transport.start()
        self.producer.flush.return_value = 1
        with self.assertRaises(TimeoutError):
            self.transport.send(UPDATE)

    def test_multiple_partitions_or_start_failure_close_consumer(self):
        self.consumer.list_topics.return_value.topics["updates"].partitions[1] = object()
        with self.assertRaisesRegex(ValueError, "single-partition"):
            self.transport.start()
        self.consumer.close.assert_called_once()
        self.producer_factory.assert_not_called()
        with self.assertRaisesRegex(RuntimeError, "not started"):
            self.transport.poll(0)

    def test_consumer_errors_and_malformed_payloads_propagate(self):
        from confluent_kafka import KafkaError, KafkaException

        self.transport.start()
        self.consumer.poll.return_value = Mock(error=lambda: KafkaError(KafkaError._AUTO_OFFSET_RESET))
        with self.assertRaises(KafkaException):
            self.transport.poll(0)
        self.consumer.poll.return_value = Mock(error=lambda: None, value=lambda: b"invalid-json")
        with self.assertRaises(ValueError):
            self.transport.poll(0)

    def test_advisory_connection_events_do_not_poison_successful_operations(self):
        from confluent_kafka import KafkaError, KafkaException

        self.transport.start()
        callback = self.consumer_factory.call_args.args[0]["error_cb"]
        callback(KafkaError(KafkaError._TRANSPORT))
        self.assertIsNone(self.transport.poll(0))
        callback(KafkaError(KafkaError._FATAL, "fatal client error", fatal=True))
        with self.assertRaises(KafkaException):
            self.transport.poll(0)

    def test_stop_and_restart_capture_a_new_boundary(self):
        self.transport.start()
        with self.assertRaisesRegex(RuntimeError, "already started"):
            self.transport.start()
        self.transport.stop()
        self.transport.stop()
        self.consumer.close.assert_called_once()
        self.consumer.get_watermark_offsets.return_value = (0, 20)
        self.transport.start()
        self.assertEqual(self.consumer.assign.call_args.args[0][0].offset, 20)


@unittest.skipUnless(find_spec("nats"), "Install the nats extra")
class NatsTransportTests(unittest.TestCase):
    def setUp(self):
        self.client = SimpleNamespace(
            connect=AsyncMock(), subscribe=AsyncMock(), flush=AsyncMock(),
            publish=AsyncMock(), close=AsyncMock(),
        )
        self.enterContext(patch("nats.aio.client.Client", return_value=self.client))
        self.transport = NatsDataFabricTransport("nats://localhost:4222", "fabric.updates")
        self.addCleanup(self.transport.stop)

    def receive(self, payload):
        callback = self.client.subscribe.call_args.kwargs["cb"]
        asyncio.run_coroutine_threadsafe(
            callback(SimpleNamespace(data=payload)), self.transport._loop,
        ).result(2)

    def test_start_flushes_subscription_and_buffers_before_first_poll(self):
        async def flush(**kwargs):
            self.client.subscribe.assert_awaited_once()
            await self.client.subscribe.call_args.kwargs["cb"](SimpleNamespace(data=encode_update(UPDATE)))

        self.client.flush.side_effect = flush
        self.transport.start()
        options = self.client.connect.call_args.kwargs
        self.assertFalse(options["allow_reconnect"])
        self.assertFalse(options["no_echo"])
        self.assertNotIn("queue", self.client.subscribe.call_args.kwargs)
        self.assertEqual(self.transport.poll(0), UPDATE)
        self.assertIsNone(self.transport.poll(0))

    def test_send_flushes_and_delivers_own_notification(self):
        self.transport.start()

        async def echo(subject, payload):
            await self.client.subscribe.call_args.kwargs["cb"](SimpleNamespace(data=payload))

        self.client.publish.side_effect = echo
        self.transport.send(UPDATE)
        self.client.publish.assert_awaited_once_with("fabric.updates", encode_update(UPDATE))
        self.assertEqual(self.transport.poll(0.1), UPDATE)
        self.assertEqual(self.client.flush.await_count, 2)

    def test_disconnect_or_slow_consumer_fails_even_with_queued_updates(self):
        for callback in ("disconnected_cb", "error_cb"):
            with self.subTest(callback=callback):
                self.transport.start()
                self.receive(encode_update(UPDATE))
                fn = self.client.connect.call_args.kwargs[callback]
                args = () if callback == "disconnected_cb" else (RuntimeError("slow consumer"),)
                asyncio.run_coroutine_threadsafe(fn(*args), self.transport._loop).result(2)
                with self.assertRaises((ConnectionError, RuntimeError)):
                    self.transport.poll(None)
                with self.assertRaises((ConnectionError, RuntimeError)):
                    self.transport.send(UPDATE)
                self.transport.stop()

    def test_start_failure_cleans_up_loop_and_connection(self):
        self.client.flush.side_effect = TimeoutError("subscription readiness")
        with self.assertRaises(TimeoutError):
            self.transport.start()
        self.client.close.assert_awaited_once()
        self.assertIsNone(self.transport._thread)
        self.assertIsNone(self.transport._loop)

    def test_start_timeout_cancels_pending_tasks_and_joins_thread(self):
        async def never_ready(**kwargs):
            await asyncio.Event().wait()

        self.client.flush.side_effect = never_ready
        self.transport._timeout = 0.05
        with self.assertRaises(TimeoutError):
            self.transport.start()
        self.client.close.assert_awaited_once()
        self.assertIsNone(self.transport._thread)

    def test_disconnect_wakes_an_indefinite_poll(self):
        self.transport.start()
        waiting = Event()
        get = self.transport._messages.get

        def blocking_get(**kwargs):
            waiting.set()
            return get(**kwargs)

        with ThreadPoolExecutor(max_workers=1) as executor:
            with patch.object(self.transport._messages, "get", side_effect=blocking_get):
                result = executor.submit(self.transport.poll)
                self.assertTrue(waiting.wait(2))
                callback = self.client.connect.call_args.kwargs["disconnected_cb"]
                asyncio.run_coroutine_threadsafe(callback(), self.transport._loop).result(2)
                with self.assertRaises(ConnectionError):
                    result.result(2)

    def test_bad_payload_and_send_errors_propagate(self):
        self.transport.start()
        self.receive(b"invalid-json")
        with self.assertRaises(ValueError):
            self.transport.poll(0)
        self.client.publish.side_effect = ConnectionError("send failed")
        with self.assertRaises(ConnectionError):
            self.transport.send(UPDATE)

    def test_stop_and_restart_discard_old_queue(self):
        self.transport.start()
        with self.assertRaisesRegex(RuntimeError, "already started"):
            self.transport.start()
        self.receive(encode_update(UPDATE))
        thread = self.transport._thread
        self.transport.stop()
        self.assertFalse(thread.is_alive())
        self.transport.stop()
        self.transport.start()
        self.assertIsNone(self.transport.poll(0))


class TransportTimeoutTests(unittest.TestCase):
    def test_invalid_timeouts_are_rejected_before_client_access(self):
        factories = (
            lambda **kw: KafkaDataFabricTransport("localhost:9092", "updates", **kw),
            lambda **kw: NatsDataFabricTransport("nats://localhost:4222", "updates", **kw),
        )
        for factory in factories:
            for timeout in (-1, float("inf"), float("nan")):
                with self.subTest(factory=factory, timeout=timeout):
                    with self.assertRaises(ValueError):
                        factory(timeout=timeout)
                    with self.assertRaises(ValueError):
                        factory().poll(timeout)
