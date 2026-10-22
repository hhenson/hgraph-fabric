from concurrent.futures import Future, ThreadPoolExecutor
from datetime import UTC, datetime
from queue import Empty, Queue
from threading import Event, current_thread
import unittest
from unittest.mock import patch

from pyarrow import table

from hgraph_fabric.data_fabric_session import run_data_fabric_session
from test_data_cache import MemoryDataCache


AS_OF = datetime(2026, 10, 21, tzinfo=UTC)


class LoopbackTransport:
    def __init__(self):
        self.messages = Queue()
        self.calls = []
        self.stopped = Event()

    def start(self):
        self.calls.append(("start", current_thread()))

    def stop(self):
        self.calls.append(("stop", current_thread()))
        self.stopped.set()

    def send(self, update):
        self.calls.append(("send", current_thread()))
        self.messages.put(update)

    def poll(self, timeout=None):
        self.calls.append(("poll", current_thread()))
        try:
            return self.messages.get(timeout=timeout)
        except Empty:
            return None


class ThreadedSessionTests(unittest.TestCase):
    def setUp(self):
        self.cache = MemoryDataCache()
        self.transport = LoopbackTransport()

    def seed(self):
        return self.cache.write(table({"value": [0]}), id="a", as_of=AS_OF)

    def test_initial_data_and_updates_callback_on_owner_thread(self):
        initial, _ = self.seed()
        changes = Queue()
        def callback(delta, publish):
            changes.put((delta, current_thread()))
        writes = []
        original = self.cache.write
        def write(*args, **kwargs):
            writes.append(current_thread())
            return original(*args, **kwargs)
        with patch.object(self.cache, "write", side_effect=write):
            with run_data_fabric_session(
                self.cache, self.transport, callback, subscriptions=["a"], poll_interval=0.01,
            ) as publish:
                delta, owner = changes.get(timeout=2)
                self.assertIs(delta["a"], initial)
                self.assertIsNot(owner, current_thread())
                written, update = publish(table({"value": [1]}), id="a", as_of=AS_OF)
                delta, callback_thread = changes.get(timeout=2)
                self.assertIs(delta["a"], written)
                self.assertIs(callback_thread, owner)
                self.assertEqual(update.as_of, AS_OF)
        self.assertEqual(writes, [owner])
        self.assertTrue(all(thread is owner for _, thread in self.transport.calls))
        self.assertEqual(self.transport.calls[0][0], "start")
        self.assertEqual(self.transport.calls[-1][0], "stop")
        self.assertFalse(owner.is_alive())
        with self.assertRaisesRegex(RuntimeError, "closed"):
            publish(table({"value": [2]}), id="a")

    def test_callback_can_publish_without_deadlock(self):
        self.seed()
        changes = Queue()
        def callback(delta, publish):
            value = delta["a"]["value"][0].as_py()
            if value == 1:
                publish(table({"value": [2]}), id="a", as_of=AS_OF)
            changes.put(value)
        with run_data_fabric_session(
            self.cache, self.transport, callback, subscriptions=["a"], poll_interval=0.01,
        ) as publish:
            self.assertEqual(changes.get(timeout=2), 0)
            publish(table({"value": [1]}), id="a", as_of=AS_OF)
            self.assertEqual(changes.get(timeout=2), 1)
            self.assertEqual(changes.get(timeout=2), 2)

    def test_concurrent_publishers_are_serialized_and_keep_write_arguments(self):
        callback = Queue()
        with run_data_fabric_session(
            self.cache, self.transport, lambda delta, publish: callback.put(delta), poll_interval=0.001,
        ) as publish:
            def send(value):
                return publish(
                    table({"value": [value]}), id="a", dependencies=(("x", 1),),
                    weak_dependencies=(("y", 2),), as_of=AS_OF,
                )
            with ThreadPoolExecutor(max_workers=4) as pool:
                results = list(pool.map(send, range(12)))
        versions = {update.revision[0] for _, update in results}
        self.assertEqual(len(versions), 12)
        self.assertEqual(self.cache.latest("a")[1], max(versions))
        for written, update in results:
            self.assertIs(self.cache.peek_version(("a", update.revision[0])), written)
            self.assertEqual(update.dependencies, (("x", 1),))
            self.assertEqual(update.weak_dependencies, (("y", 2),))
            self.assertEqual(update.as_of, AS_OF)
        owner = self.transport.calls[0][1]
        self.assertTrue(all(thread is owner for _, thread in self.transport.calls))
        self.assertTrue(callback.empty())

    def test_initial_callback_can_publish_before_context_entry_returns(self):
        self.seed()
        completed = Event()
        results = Queue()
        original_result = Future.result
        caller = current_thread()
        def delayed_result(future, timeout=None):
            if current_thread() is caller:
                self.assertTrue(completed.wait(2))
            return original_result(future, timeout)
        def callback(delta, publish):
            try:
                written, update = publish(table({"value": [42]}), id="output", as_of=AS_OF)
                results.put((publish, written, update))
            finally:
                completed.set()
        # Force the worker's first callback to precede the caller receiving the
        # publish callable. This is a valid scheduling order, not a timed race.
        with patch.object(Future, "result", delayed_result):
            with run_data_fabric_session(
                self.cache, self.transport, callback, subscriptions=["a"], poll_interval=0.01,
            ) as publish:
                callback_publish, written, update = results.get(timeout=2)
                self.assertIs(callback_publish, publish)
                self.assertEqual(update.id, "output")
                self.assertEqual(written["value"][0].as_py(), 42)

    def test_interleaved_local_echo_cannot_reuse_a_published_version(self):
        def write(cache, version, value):
            return cache.write(
                table({"value": [value]}, metadata={b"version": str(version).encode()}),
                id="input", as_of=AS_OF,
            )
        write(self.cache, 0, 0)
        remote = MemoryDataCache()
        remote.latest_values = self.cache.latest_values
        remote.revision_values = self.cache.revision_values
        remote.data_values = self.cache.data_values
        _, input1 = write(remote, 10, 1)
        _, input2 = write(remote, 20, 2)

        class InterleavedTransport(LoopbackTransport):
            def send(self, update):
                super().send(update)
                if update.id == "output" and update.revision[0] == 1:
                    self.messages.put(input2)

        transport = InterleavedTransport()
        initialized = Event()
        completed = Event()
        updates = []
        def emit(publish, value, dependency):
            _, update = publish(
                table({"value": [value]}, metadata={b"version": b"1"}),
                id="output", dependencies=(("input", dependency),), as_of=AS_OF,
            )
            updates.append(update)
        def callback(changes, publish):
            value = changes["input"]["value"][0].as_py()
            if value == 0:
                initialized.set()
            elif value == 1:
                emit(publish, 11, 10)
                emit(publish, 12, 10)
            else:
                emit(publish, 21, 20)
                completed.set()

        with run_data_fabric_session(
            self.cache, transport, callback, subscriptions=["input"], poll_interval=0.01,
        ):
            self.assertTrue(initialized.wait(2))
            # FIFO delivery is output@1, input@20, output@2. The input callback
            # publishes again after the first echo, while output@2 already exists.
            transport.messages.put(input1)
            self.assertTrue(completed.wait(2))

        self.assertEqual([update.revision for update in updates], [(1, 0), (2, 0), (3, 0)])
        self.assertEqual([update.previous_revision for update in updates], [None, (1, 0), (2, 0)])
        self.assertEqual(self.cache.latest("output"), ("output", 3, 0))
        self.assertEqual(self.cache.latest_values["output"], ("output", 3, 0))
        for version, value, dependency in ((1, 11, 10), (2, 12, 10), (3, 21, 20)):
            self.assertEqual(self.cache.get_version(("output", version))["value"][0].as_py(), value)
            self.assertEqual(self.cache.data_values[("output", version)]["value"][0].as_py(), value)
            self.assertEqual(self.cache.revision_values[("output", version, 0)].dependencies,
                             (("input", dependency),))

    def test_shutdown_allows_in_flight_callback_to_finish_publishing(self):
        self.seed()
        initial = Event()
        entered = Event()
        release = Event()
        def callback(delta, publish):
            value = delta["a"]["value"][0].as_py()
            if value == 0:
                initial.set()
            elif value == 1:
                entered.set()
                if not release.wait(2):
                    raise AssertionError("Test did not release callback")
                publish(table({"value": [2]}), id="a", as_of=AS_OF)
        context = run_data_fabric_session(
            self.cache, self.transport, callback, subscriptions=["a"], poll_interval=0.01,
        )
        publish = context.__enter__()
        try:
            self.assertTrue(initial.wait(2))
            publish(table({"value": [1]}), id="a", as_of=AS_OF)
            self.assertTrue(entered.wait(2))
        finally:
            owner = self.transport.calls[0][1]
            join = owner.join
            def release_and_join():
                release.set()
                join()
            # Exit has closed the external publication queue before joining.
            with patch.object(owner, "join", side_effect=release_and_join):
                context.__exit__(None, None, None)
        latest = self.cache.latest("a")
        self.assertEqual(self.cache.get_version(latest[:2])["value"][0].as_py(), 2)
        self.assertFalse(owner.is_alive())

    def test_context_body_exception_stops_and_joins_worker(self):
        error = ValueError("Caller failed")
        with self.assertRaises(ValueError) as raised:
            with run_data_fabric_session(self.cache, self.transport, lambda _, publish: None, poll_interval=0.01):
                raise error
        self.assertIs(raised.exception, error)
        self.assertTrue(self.transport.stopped.is_set())
        self.assertFalse(self.transport.calls[0][1].is_alive())

    def test_startup_failure_propagates_before_yield(self):
        error = OSError("Start failed")
        with patch.object(self.transport, "start", side_effect=error):
            with self.assertRaises(OSError) as raised:
                with run_data_fabric_session(self.cache, self.transport, lambda _, publish: None):
                    self.fail("A failed session must not be yielded")
        self.assertIs(raised.exception, error)

    def test_subscription_initialization_failure_stops_worker(self):
        with self.assertRaises(KeyError):
            with run_data_fabric_session(self.cache, self.transport, lambda _, publish: None, subscriptions=["missing"]):
                self.fail("Missing subscription must fail initialization")
        self.assertTrue(self.transport.stopped.is_set())
        self.assertFalse(self.transport.calls[0][1].is_alive())

    def test_callback_failure_propagates_on_publish_and_exit(self):
        self.seed()
        error = ValueError("Callback failed")
        def callback(_, publish):
            raise error
        with self.assertRaises(ValueError) as raised:
            with run_data_fabric_session(
                self.cache, self.transport, callback, subscriptions=["a"], poll_interval=0.01,
            ) as publish:
                self.assertTrue(self.transport.stopped.wait(2))
                with self.assertRaises(ValueError) as publish_error:
                    publish(table({"value": [1]}), id="a")
                self.assertIs(publish_error.exception, error)
        self.assertIs(raised.exception, error)

    def test_write_failure_releases_queued_publishers_and_propagates_on_exit(self):
        entered = Event()
        release = Event()
        error = OSError("Storage failed")
        def fail_write(*args, **kwargs):
            entered.set()
            if not release.wait(2):
                raise AssertionError("Test did not release write")
            raise error
        with self.assertRaises(OSError) as raised:
            with patch.object(self.cache, "write", side_effect=fail_write):
                with run_data_fabric_session(
                    self.cache, self.transport, lambda _, publish: None, poll_interval=0.01,
                ) as publish:
                    with ThreadPoolExecutor(max_workers=2) as pool:
                        first = pool.submit(publish, table({"value": [1]}), "a")
                        self.assertTrue(entered.wait(2))
                        second = pool.submit(publish, table({"value": [2]}), "b")
                        release.set()
                        for future in (first, second):
                            with self.assertRaises(OSError) as failure:
                                future.result(timeout=2)
                            self.assertIs(failure.exception, error)
        self.assertIs(raised.exception, error)
        self.assertFalse(any(kind == "send" for kind, _ in self.transport.calls))
        self.assertTrue(self.transport.stopped.is_set())

    def test_poll_failure_propagates_on_exit(self):
        error = OSError("Receive failed")
        with patch.object(self.transport, "poll", side_effect=error):
            with self.assertRaises(OSError) as raised:
                with run_data_fabric_session(self.cache, self.transport, lambda _, publish: None):
                    self.assertTrue(self.transport.stopped.wait(2))
        self.assertIs(raised.exception, error)

    def test_poll_interval_must_be_positive_and_finite(self):
        for interval in (0, -1, float("nan"), float("inf")):
            with self.subTest(interval=interval):
                with self.assertRaises(ValueError):
                    with run_data_fabric_session(
                        self.cache, self.transport, lambda _, publish: None, poll_interval=interval,
                    ):
                        self.fail("Invalid interval must not start a worker")
        self.assertEqual(self.transport.calls, [])

    def test_threaded_dependency_publication_suppresses_as_of_only_updates(self):
        data, original = self.seed()
        with run_data_fabric_session(
            self.cache, self.transport, lambda delta, publish: None, poll_interval=0.001,
        ) as publish:
            returned, unchanged = publish(id="a", as_of=AS_OF)
            self.assertIs(returned, data)
            self.assertEqual(unchanged, original)
            returned, changed = publish(id="a", weak_dependencies=(("reference", 1),))
            self.assertIs(returned, data)
            self.assertEqual(changed.revision, (original.revision[0], 1))
            _, unchanged = publish(id="a", weak_dependencies=(("reference", 1),), as_of=AS_OF)
            self.assertEqual(unchanged, changed)
        self.assertEqual([name for name, _ in self.transport.calls].count("send"), 1)
        owner = self.transport.calls[0][1]
        self.assertTrue(all(thread is owner for _, thread in self.transport.calls))

    def test_initial_callback_can_drop_as_of_only_publication(self):
        _, initial = self.seed()
        completed = Event()
        def callback(delta, publish):
            _, update = publish(id="a")
            self.assertEqual(update, initial)
            completed.set()
        with run_data_fabric_session(
            self.cache, self.transport, callback, subscriptions=["a"], poll_interval=0.001,
        ):
            self.assertTrue(completed.wait(2))
        self.assertNotIn("send", [name for name, _ in self.transport.calls])
