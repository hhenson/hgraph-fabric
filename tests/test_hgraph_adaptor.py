"""Real hgraph execution of the optional Python fabric adaptor."""
from collections import deque
from threading import enumerate as active_threads, get_ident
from time import sleep
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import unittest

import pyarrow as pa

try:
    import hgraph as hg
except ImportError:
    raise unittest.SkipTest("Install the hgraph extra to test the adaptor")

from hgraph.test import eval_node
from hgraph._frame import as_arrow_table
from hgraph_fabric.hgraph_adaptor import fabric_service, publish, subscribe, version_id, VersionId
from test_data_cache import MemoryDataCache


@dataclass(frozen=True)
class Prices(hg.CompoundScalar):
    price: int


@dataclass(frozen=True)
class Labels(hg.CompoundScalar):
    label: str


class LoopbackTransport:
    def __init__(self):
        self.pending = deque()
        self.sent = []
        self.starts = self.stops = self.polls = 0
        self.on_poll = None
        self.threads = set()

    def start(self):
        self.threads.add(get_ident())
        self.starts += 1

    def stop(self):
        self.threads.add(get_ident())
        self.stops += 1

    def send(self, update):
        self.threads.add(get_ident())
        self.sent.append(update)
        self.pending.append(update)

    def poll(self, timeout=None):
        self.threads.add(get_ident())
        if not self.pending:
            sleep(timeout or 0)
        self.polls += 1
        if self.on_poll:
            self.on_poll(self)
        return self.pending.popleft() if self.pending else None


def seed(cache, key, value=1, version=100, dependencies=()):
    return cache.write(pa.table({"price": [value]}, metadata={b"version": str(version).encode()}),
                       id=key, dependencies=dependencies, as_of=datetime(2026, 10, 22, tzinfo=UTC))


class HgraphAdaptorTests(unittest.TestCase):
    def setUp(self):
        self.cache = MemoryDataCache()
        self.transport = LoopbackTransport()

    def run_graph(self, app, *args):
        start = datetime.now(UTC).replace(tzinfo=None)
        return eval_node(app, *args, __run_mode__="real_time", __elide__=True,
                         __start_time__=start, __end_time__=start + timedelta(seconds=.5))

    def test_typed_subscription_and_version_share_one_session(self):
        seed(self.cache, "prices", version=321)
        observed = []

        @hg.sink_node
        def collect(df: hg.TS[hg.Frame[Prices]], version: hg.TS[VersionId]):
            observed.append((as_arrow_table(df.value).to_pydict(), version.value))

        @hg.graph
        def app():
            hg.register_service("fabric", fabric_service, cache=self.cache, transport=self.transport, poll_interval=.001)
            collect(subscribe[Prices]("prices"), version_id("prices"))
            collect(subscribe[Prices]("prices"), version_id("prices"))

        self.run_graph(app)
        self.assertEqual(observed, [({"price": [1]}, ("prices", 321))] * 2)
        self.assertEqual((self.transport.starts, self.transport.stops), (1, 1))
        self.assertEqual(self.transport.sent, [])

    def test_different_schemas_share_the_same_cut_scope(self):
        seed(self.cache, "prices")
        self.cache.write(pa.table({"label": ["one"]}), id="labels")
        observed = []

        @hg.sink_node
        def collect(price: hg.TS[hg.Frame[Prices]], label: hg.TS[hg.Frame[Labels]]):
            observed.append((as_arrow_table(price.value).to_pydict(), as_arrow_table(label.value).to_pydict()))

        @hg.graph
        def app():
            hg.register_service("fabric", fabric_service, cache=self.cache, transport=self.transport, poll_interval=.001)
            collect(subscribe[Prices]("prices"), subscribe[Labels]("labels"))

        self.run_graph(app)
        self.assertEqual(observed, [({"price": [1]}, {"label": ["one"]})])
        self.assertEqual(self.transport.starts, 1)

    def test_publish_new_frames_dynamic_ids_and_explicit_empty_dependencies(self):
        @hg.graph
        def app(df: hg.TS[hg.Frame[Prices]], id: hg.TS[str]):
            hg.register_service("fabric", fabric_service, cache=self.cache, transport=self.transport, poll_interval=.001)
            publish(df, id, dependencies=())

        self.run_graph(app, [pa.table({"price": [1]}), None, pa.table({"price": [2]})],
                       ["one", "two", None])
        self.assertEqual([update.id for update in self.transport.sent], ["one", "two", "two"])
        self.assertEqual([update.revision[1] for update in self.transport.sent], [0, 0, 0])
        self.assertEqual(self.cache.get_version(self.cache.latest("two")[:2]).to_pydict(), {"price": [2]})
        self.assertTrue(all(update.as_of.tzinfo == UTC for update in self.transport.sent))

    def test_dependency_only_ticks_publish_revisions_without_table_rewrites(self):
        @hg.graph
        def app(df: hg.TS[hg.Frame[Prices]], strong: hg.TS[VersionId], weak: hg.TS[VersionId]):
            hg.register_service("fabric", fabric_service, cache=self.cache, transport=self.transport, poll_interval=.001)
            publish(df, "output", hg.TSL.from_ts(strong), hg.TSL.from_ts(weak))

        self.run_graph(app, [pa.table({"price": [1]})],
                       [("source", 1), ("source", 2), None, ("source", 2)],
                       [("reference", 1), None, ("reference", 2)])
        self.assertEqual([u.revision for u in self.transport.sent], [(self.transport.sent[0].revision[0], n) for n in range(3)])
        self.assertEqual(len([w for w in self.cache.writes if w[0] == "version"]), 1)
        self.assertEqual(self.transport.sent[-1].dependencies, (("source", 2),))
        self.assertEqual(self.transport.sent[-1].weak_dependencies, (("reference", 2),))

    def test_waits_for_every_strong_and_weak_dependency(self):
        @hg.graph
        def app(df: hg.TS[hg.Frame[Prices]], a: hg.TS[VersionId], b: hg.TS[VersionId], weak: hg.TS[VersionId]):
            hg.register_service("fabric", fabric_service, cache=self.cache, transport=self.transport, poll_interval=.001)
            publish(df, "output", hg.TSL.from_ts(a, b), hg.TSL.from_ts(weak))

        self.run_graph(app, [pa.table({"price": [1]}), pa.table({"price": [2]})],
                       [("a", 1)], [None, ("b", 1)], [None, None, ("weak", 1)])
        self.assertEqual(len(self.transport.sent), 1)
        self.assertEqual(self.cache.get_version(self.cache.latest("output")[:2]).to_pydict(), {"price": [2]})
        self.assertEqual(self.transport.sent[0].dependencies, (("a", 1), ("b", 1)))

    def test_subscribers_observe_publications_through_the_notification_cut(self):
        seed(self.cache, "output", value=0)

        @hg.graph
        def app(df: hg.TS[hg.Frame[Prices]]) -> hg.TS[hg.Frame[Prices]]:
            hg.register_service("fabric", fabric_service, cache=self.cache, transport=self.transport, poll_interval=.001)
            publish(df, "output", dependencies=())
            return subscribe[Prices]("output")

        results = self.run_graph(app, [pa.table({"price": [7]})])
        self.assertEqual([as_arrow_table(v).to_pydict() for v in results if v is not None],
                         [{"price": [0]}, {"price": [7]}])
        self.assertEqual(len(self.transport.sent), 1)

    def test_dependency_revision_releases_a_consistent_pair_in_one_tick(self):
        seed(self.cache, "x", version=1)
        seed(self.cache, "x", version=7)
        seed(self.cache, "a", dependencies=(("x", 1),))
        seed(self.cache, "b", dependencies=(("x", 1),))
        observed = []

        @hg.sink_node
        def collect(a: hg.TS[hg.Frame[Prices]], b: hg.TS[hg.Frame[Prices]], version: hg.TS[VersionId]):
            observed.append((as_arrow_table(a.value)["price"][0].as_py(),
                             as_arrow_table(b.value)["price"][0].as_py(), version.value))

        @hg.graph
        def app(a: hg.TS[hg.Frame[Prices]], b: hg.TS[hg.Frame[Prices]], dep: hg.TS[VersionId]):
            hg.register_service("fabric", fabric_service, cache=self.cache, transport=self.transport, poll_interval=.001)
            publish(a, "a", (dep,))
            publish(b, "b", (dep,))
            collect(subscribe[Prices]("a"), subscribe[Prices]("b"), version_id("a"))

        self.run_graph(app, [pa.table({"price": [2]})],
                       [None, None, pa.table({"price": [3]})], [("x", 7)])
        self.assertEqual([(a, b) for a, b, _ in observed], [(1, 1), (2, 3)])
        self.assertEqual(observed[-1][2], self.cache.latest("a")[:2])

    def test_named_service_paths_isolate_sessions(self):
        other_cache, other_transport = MemoryDataCache(), LoopbackTransport()
        seed(self.cache, "same", value=1)
        seed(other_cache, "same", value=2)
        observed = []

        @hg.sink_node
        def collect(a: hg.TS[hg.Frame[Prices]], b: hg.TS[hg.Frame[Prices]], c: hg.TS[hg.Frame[Prices]]):
            observed.append(tuple(as_arrow_table(v.value)["price"][0].as_py() for v in (a, b, c)))

        @hg.graph
        def app():
            hg.register_service("fabric", fabric_service, cache=self.cache, transport=self.transport, poll_interval=.001)
            a = subscribe[Prices]("same")
            hg.register_service("other", fabric_service, cache=other_cache, transport=other_transport, poll_interval=.001)
            b = subscribe[Prices]("same", path="other")
            collect(a, b, subscribe[Prices]("same"))

        self.run_graph(app)
        self.assertEqual(observed, [(1, 2, 1)])
        self.assertEqual((other_transport.starts, other_transport.stops), (1, 1))

    def test_missing_service_registration_is_a_wiring_error(self):
        @hg.graph
        def app():
            return subscribe[Prices]("missing")
        with self.assertRaisesRegex(ValueError, "missing implementation"):
            self.run_graph(app)

    def test_start_and_poll_failures_close_transport(self):
        @hg.graph
        def missing():
            hg.register_service("fabric", fabric_service, cache=self.cache, transport=self.transport, poll_interval=.001)
            return subscribe[Prices]("missing")
        with self.assertRaises(RuntimeError):
            self.run_graph(missing)
        self.assertEqual(self.transport.stops, 1)
        self.transport = LoopbackTransport()
        seed(self.cache, "prices")
        def fail(_):
            raise OSError("notification failure")
        self.transport.on_poll = fail
        @hg.graph
        def app():
            hg.register_service("fabric", fabric_service, cache=self.cache, transport=self.transport, poll_interval=.001)
            return subscribe[Prices]("prices")
        with self.assertRaisesRegex(RuntimeError, "notification failure"):
            self.run_graph(app)
        self.assertEqual(self.transport.stops, 1)

    def test_schema_mismatch_fails_instead_of_publishing_a_mistyped_stream(self):
        self.cache.write(pa.table({"label": ["wrong"]}), id="prices")
        @hg.graph
        def app():
            hg.register_service("fabric", fabric_service, cache=self.cache, transport=self.transport, poll_interval=.001)
            return subscribe[Prices]("prices")
        with self.assertRaises(RuntimeError):
            self.run_graph(app)
        self.assertEqual(self.transport.stops, 1)

    def test_component_can_publish_from_subscribed_versions(self):
        seed(self.cache, "input", value=5, version=50)
        seed(self.cache, "output", value=0)
        @hg.graph
        def component():
            data = subscribe[Prices]("input")
            publish(data, "output", dependencies=(version_id("input"),))
        @hg.graph
        def app() -> hg.TS[hg.Frame[Prices]]:
            hg.register_service("fabric", fabric_service, cache=self.cache, transport=self.transport, poll_interval=.001)
            component()
            return subscribe[Prices]("output")
        results = self.run_graph(app)
        self.assertEqual([as_arrow_table(v)["price"][0].as_py() for v in results if v is not None], [0, 5])
        self.assertEqual(self.transport.sent[0].dependencies, (("input", 50),))

    def test_two_graph_runs_create_fresh_sessions_and_close_them(self):
        seed(self.cache, "prices")
        @hg.graph
        def app():
            hg.register_service("fabric", fabric_service, cache=self.cache, transport=self.transport, poll_interval=.001)
            return subscribe[Prices]("prices")
        for _ in range(2):
            self.assertEqual(len(self.run_graph(app)), 1)
        self.assertEqual((self.transport.starts, self.transport.stops), (2, 2))

    def test_push_source_receives_a_later_publication(self):
        from threading import Event
        initial_received = Event()
        seed(self.cache, "prices", value=1)
        def receive(transport):
            if initial_received.is_set():
                initial_received.clear()
                transport.on_poll = None
                _, update = seed(self.cache, "prices", value=2, version=200)
                transport.pending.append(update)
        self.transport.on_poll = receive
        observed = []
        @hg.sink_node
        def collect(df: hg.TS[hg.Frame[Prices]], api: hg.EvaluationEngineApi = None):
            value = as_arrow_table(df.value)["price"][0].as_py()
            observed.append(value)
            if value == 1:
                initial_received.set()
            if value == 2:
                api.request_engine_stop()
        @hg.graph
        def app():
            hg.register_service("fabric", fabric_service, cache=self.cache, transport=self.transport, poll_interval=.001)
            collect(subscribe[Prices]("prices"))
        start = datetime.now(UTC).replace(tzinfo=None)
        eval_node(app, __run_mode__="real_time", __start_time__=start,
                  __end_time__=start + timedelta(seconds=2))
        self.assertEqual(observed, [1, 2])
        self.assertEqual(self.transport.stops, 1)

    def test_revision_only_publications_change_cut_metadata_without_table_ticks(self):
        for version in (1, 7):
            seed(self.cache, "x", version=version)
        for key in ("a", "b"):
            seed(self.cache, key, value=0, dependencies=(("x", 1),))
        observed = []
        @hg.sink_node
        def collect(a: hg.TS[VersionId], b: hg.TS[VersionId]):
            observed.append((a.value, b.value))
        @hg.graph
        def app(a: hg.TS[hg.Frame[Prices]], b: hg.TS[hg.Frame[Prices]],
                dep_a: hg.TS[VersionId], dep_b: hg.TS[VersionId]):
            hg.register_service("fabric", fabric_service, cache=self.cache, transport=self.transport, poll_interval=.001)
            publish(a, "a", (dep_a,))
            publish(b, "b", (dep_b,))
            collect(version_id("a"), version_id("b"))
        self.run_graph(app, [pa.table({"price": [2]})], [pa.table({"price": [3]})],
                       [("x", 1), None, None, ("x", 7)],
                       [("x", 1), None, None, None, None, ("x", 7)])
        self.assertEqual(self.cache.latest("a"), ("a", self.cache.latest("a")[1], 1))
        self.assertEqual(self.cache.latest("b"), ("b", self.cache.latest("b")[1], 1))
        self.assertEqual(observed[-1], (self.cache.latest("a")[:2], self.cache.latest("b")[:2]))
        self.assertEqual(observed.count((self.cache.latest("a")[:2], self.cache.latest("b")[:2])), 1)
        self.assertEqual(len([u for u in self.transport.sent if u.revision[1] == 1]), 2)

    def test_publish_failure_closes_transport_without_notification(self):
        from unittest.mock import patch
        @hg.graph
        def app(df: hg.TS[hg.Frame[Prices]]):
            hg.register_service("fabric", fabric_service, cache=self.cache, transport=self.transport, poll_interval=.001)
            publish(df, "output", dependencies=())
        with patch.object(self.cache, "write_version", side_effect=OSError("write failed")):
            with self.assertRaisesRegex(RuntimeError, "write failed"):
                self.run_graph(app, [pa.table({"price": [1]})])
        self.assertEqual(self.transport.stops, 1)
        self.assertEqual(self.transport.sent, [])

    def test_all_io_runs_on_one_worker_not_the_graph_thread(self):
        from unittest.mock import patch
        seed(self.cache, "prices")
        threads = set()
        original = self.cache.get_version
        def get_version(key):
            threads.add(get_ident())
            return original(key)
        @hg.graph
        def app() -> hg.TS[hg.Frame[Prices]]:
            hg.register_service("fabric", fabric_service, cache=self.cache,
                                transport=self.transport, poll_interval=.001)
            publish(hg.const(pa.table({"price": [2]}), tp=hg.TS[hg.Frame[Prices]]),
                    "prices", dependencies=())
            return subscribe[Prices]("prices")
        with patch.object(self.cache, "get_version", side_effect=get_version):
            self.run_graph(app)
        self.assertEqual(threads, self.transport.threads)
        self.assertEqual(len(threads), 1)
        self.assertNotIn(get_ident(), threads)
        self.assertFalse(any(t.name == "fabric-adaptor" for t in active_threads()))

    def test_publication_replies_route_to_the_requesting_clients(self):
        observed = []
        @hg.sink_node
        def collect(a: hg.TS[VersionId], b: hg.TS[VersionId]):
            observed.append((a.value, b.value))
        @hg.graph
        def app(df: hg.TS[hg.Frame[Prices]]):
            hg.register_service("fabric", fabric_service, cache=self.cache,
                                transport=self.transport, poll_interval=.001)
            collect(publish(df, "a", ()), publish(df, "b", ()))
        self.run_graph(app, [pa.table({"price": [1]})])
        self.assertEqual(observed, [(self.cache.latest("a")[:2], self.cache.latest("b")[:2])])
        self.assertEqual(self.transport.starts, 1)

    def test_dynamic_subscription_id_and_version_id(self):
        seed(self.cache, "a", value=1, version=100)
        seed(self.cache, "b", value=2, version=200)
        # Switch keys only after receiving the first selection, so this tests
        # actual removal/re-subscription rather than two startup key ticks.
        from threading import Event, Thread
        selected = Event()
        observed = []
        threads = []
        @hg.push_queue(hg.TS[str])
        def ids(sender):
            def run():
                sender("a")
                if selected.wait(1):
                    sender("b")
            thread = Thread(target=run)
            threads.append(thread)
            thread.start()
        @ids.stop
        def stop_ids():
            selected.set()
            for thread in threads:
                thread.join()
        @hg.sink_node
        def collect(df: hg.TS[hg.Frame[Prices]], v: hg.TS[VersionId],
                    api: hg.EvaluationEngineApi = None):
            observed.append((as_arrow_table(df.value)["price"][0].as_py(), v.value))
            if v.value[0] == "a":
                selected.set()
            else:
                api.request_engine_stop()
        @hg.graph
        def app():
            hg.register_service("fabric", fabric_service, cache=self.cache,
                                transport=self.transport, poll_interval=.001)
            key = ids()
            collect(subscribe[Prices](key), version_id(key))
        self.run_graph(app)
        self.assertEqual(observed, [(1, ("a", 100)), (2, ("b", 200))])
        self.assertEqual((self.transport.starts, self.transport.stops), (1, 1))

    def test_mapped_components_share_registered_services(self):
        seed(self.cache, "a", value=1)
        seed(self.cache, "b", value=2)
        @hg.graph
        def component(key: hg.TS[str]) -> hg.TS[hg.Frame[Prices]]:
            return subscribe[Prices](key)
        @hg.graph
        def app() -> hg.TSD[str, hg.TS[hg.Frame[Prices]]]:
            hg.register_service("fabric", fabric_service, cache=self.cache,
                                transport=self.transport, poll_interval=.001)
            return hg.map_(component, __keys__=hg.const(frozenset(("a", "b")), tp=hg.TSS[str]))
        results = self.run_graph(app)
        merged = {key: as_arrow_table(value).to_pydict() for delta in results for key, value in delta.items()}
        self.assertEqual(merged, {"a": {"price": [1]}, "b": {"price": [2]}})
        self.assertEqual(self.transport.starts, 1)

    def test_worker_start_failure_is_pushed_to_the_graph(self):
        from unittest.mock import patch
        @hg.graph
        def app() -> hg.TS[VersionId]:
            hg.register_service("fabric", fabric_service, cache=self.cache,
                                transport=self.transport, poll_interval=.001)
            return version_id("a")
        with patch.object(self.transport, "start", side_effect=OSError("start failed")):
            with self.assertRaisesRegex(RuntimeError, "start failed"):
                self.run_graph(app)
        self.assertEqual(self.transport.stops, 1)

    def test_storage_wait_does_not_block_graph_evaluation(self):
        from threading import Event
        from unittest.mock import patch
        writing, release = Event(), Event()
        original = self.cache.write_version
        graph_progress = []
        def slow_write(version, data):
            writing.set()
            if not release.wait(2):
                raise TimeoutError("graph blocked on storage")
            original(version, data)
        @hg.sink_node
        def unblock(tick: hg.TS[bool]):
            if writing.is_set():
                graph_progress.append(True)
                release.set()
        @hg.graph
        def app(df: hg.TS[hg.Frame[Prices]]) -> hg.TS[VersionId]:
            hg.register_service("fabric", fabric_service, cache=self.cache,
                                transport=self.transport, poll_interval=.001)
            unblock(hg.schedule(timedelta(milliseconds=10)))
            return publish(df, "output", ())
        with patch.object(self.cache, "write_version", side_effect=slow_write):
            replies = self.run_graph(app, [pa.table({"price": [1]})])
        self.assertTrue(graph_progress)
        self.assertEqual(replies, [self.cache.latest("output")[:2]])

    def test_publication_service_accepts_an_unparameterized_frame(self):
        from hgraph_fabric.hgraph_adaptor import _publish
        @hg.graph
        def app(df: hg.TS[hg.Frame]) -> hg.TS[VersionId]:
            hg.register_service("fabric", fabric_service, cache=self.cache,
                                transport=self.transport, poll_interval=.001)
            return _publish(
                df, "raw", (), (), hg.last_modified_time(df))
        replies = self.run_graph(app, [pa.table({"label": ["one"]})])
        version = self.cache.latest("raw")[:2]
        self.assertEqual(replies, [version])
        self.assertEqual(self.cache.get_version(version).to_pydict(), {"label": ["one"]})

    def test_empty_tsl_dependencies_allow_publication(self):
        @hg.graph
        def app(df: hg.TS[hg.Frame[Prices]]) -> hg.TS[VersionId]:
            hg.register_service("fabric", fabric_service, cache=self.cache,
                                transport=self.transport, poll_interval=.001)
            empty = hg.const((), tp=hg.TSL[hg.TS[VersionId], hg.Size[0]])
            return publish(df, "output", empty, empty)
        replies = self.run_graph(app, [pa.table({"price": [1]})])
        self.assertEqual(replies, [self.cache.latest("output")[:2]])
        self.assertEqual(len(self.transport.sent), 1)
        self.assertEqual(self.transport.sent[0].dependencies, ())
        self.assertEqual(self.transport.sent[0].weak_dependencies, ())
