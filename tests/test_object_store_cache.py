"""The same persistence contract for memory, filesystem and opt-in S3 stores."""
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from datetime import datetime, timedelta, timezone
import json
import os
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch
from uuid import uuid4

import pyarrow as pa

try:
    import obstore
    from obstore.store import LocalStore, MemoryStore, S3Store
except ImportError:
    raise unittest.SkipTest("Install the storage extra to test object stores")

from hgraph_fabric.object_store_cache import ObjectStoreDataCache
from hgraph_fabric.data_fabric_session import DataFabricSession, PollingDataFabricSession, DataFabricTransport
from hgraph_fabric.data_structures import DataRevisionDetails


AS_OF = datetime(2026, 10, 23, 12, 34, 56, 123456, tzinfo=timezone(timedelta(hours=4)))


class StorageContract:
    def cache(self, namespace="test"):
        return ObjectStoreDataCache(self.store(), namespace)

    def publish(self, cache, value=1, **kwargs):
        return cache.write(pa.table({"price": [1, None]}, metadata={b"version": str(value).encode()}),
                           id="prices", as_of=AS_OF, **kwargs)

    def test_fresh_cache_reads_arrow_schema_metadata_and_values(self):
        schema = pa.schema([
            pa.field("name", pa.dictionary(pa.int8(), pa.string()), metadata={b"field": b"\xff"}),
            pa.field("time", pa.timestamp("us", "UTC")),
        ], metadata={b"custom": b"\x00\xff", b"version": b"1"})
        data = pa.Table.from_pydict({"name": ["one", None], "time": [AS_OF, None]}, schema=schema)
        written, update = self.cache().write(data, id="prices", as_of=AS_OF)
        reader = self.cache()
        self.assertIsNone(reader.peek_latest("prices"))
        self.assertEqual(reader.latest("prices"), ("prices", 1, 0))
        with patch("obstore.get", wraps=obstore.get) as get:
            loaded = reader.get_version(("prices", 1))
            self.assertIs(reader.get_version(("prices", 1)), loaded)
        self.assertEqual(get.call_count, 2)  # Descriptor, then Arrow payload.
        self.assertTrue(loaded.equals(written, check_metadata=True))
        self.assertEqual(reader.get_revision(("prices", 1, 0)).as_of, update.as_of)

    def test_empty_tables_round_trip(self):
        for index, data in enumerate((pa.table({}), pa.table({"a": pa.array([], type=pa.int64())}))):
            with self.subTest(index=index):
                table, update = self.cache().write(data, id=str(index))
                loaded = self.cache().get_version((str(index), update.revision[0]))
                self.assertTrue(loaded.equals(table, check_metadata=True))

    def test_table_larger_than_default_upload_chunk_round_trips(self):
        data = pa.table({"blob": [b"x" * (6 * 1024 * 1024)]})
        table, update = self.cache().write(data, id="large", as_of=AS_OF)
        self.assertTrue(self.cache().get_version(("large", update.revision[0])).equals(table, check_metadata=True))

    def test_revision_metadata_preserves_nullable_dependencies_and_time(self):
        for count, deps in enumerate((None, (), (("source.a", 7), ("b", 99)))):
            with self.subTest(deps=deps):
                details = DataRevisionDetails(deps, deps, (7, 4) if count == 0 else None, AS_OF)
                if count == 0:
                    self.cache().write_version(("prices", 1), pa.table({"x": [1]}), details)
                else:
                    self.cache().write_revision(("prices", 1, count), details)
                self.assertEqual(self.cache().get_revision(("prices", 1, count)), details)

    def test_contiguous_history_and_dependency_only_revisions_survive_restart(self):
        writer = self.cache()
        original, _ = self.publish(writer)
        with patch("obstore.put", wraps=obstore.put) as put:
            writer.write_dependencies("prices", (("source", 7),), (("weak", 8),), AS_OF)
        self.assertEqual(put.call_count, 2)  # Revision + per-version hint; no Arrow rewrite.
        with patch("obstore.put", wraps=obstore.put) as put:
            writer.write_dependencies("prices", (("source", 7),), (("weak", 8),), AS_OF + timedelta(days=1))
        put.assert_not_called()
        self.publish(writer, 2)
        reader = self.cache()
        self.assertEqual(list(reader.version_history("prices")), [("prices", 2, 0), ("prices", 1, 1)])
        self.assertEqual(reader.get_latest_revision(("prices", 1)), ("prices", 1, 1))
        self.assertEqual(reader.get_revision(("prices", 2, 0)).previous_revision, (1, 1))
        self.assertEqual(reader.get_revision(("prices", 1, 1)).weak_dependencies, (("weak", 8),))
        self.assertTrue(reader.get_version(("prices", 1)).equals(original, check_metadata=True))

    def test_readable_namespaces_and_ids_are_isolated(self):
        ids = ("prices.close", "prices-close", "prices_close", "a", "a..b", "9", "x" * 128)
        for namespace in ("market-data", "market.data", "market_data", "market-data/equities", "market-data/fx"):
            for index, data_id in enumerate(ids):
                table = pa.table({"value": [index]})
                _, update = self.cache(namespace).write(table, id=data_id, as_of=AS_OF)
                reader = self.cache(namespace)
                self.assertEqual(reader.get_version((data_id, update.revision[0]))["value"].to_pylist(), [index])
            for index, data_id in enumerate(ids):
                reader = self.cache(namespace)
                self.assertEqual(reader.get_version(reader.latest(data_id)[:2])["value"].to_pylist(), [index])
        with self.assertRaises(KeyError):
            self.cache("absent").latest("a")

    def test_object_paths_preserve_names_verbatim(self):
        self.cache("market-data/equities/us").write(
            pa.table({"value": [1]}, metadata={b"version": b"1"}),
            id="prices.close", as_of=AS_OF,
        )
        paths = {item["path"] for item in obstore.list(self.store()).collect()}
        prefix = "market-data/equities/us/prices.close"
        payload = next(path for path in paths if path.endswith(".arrow"))
        self.assertRegex(payload, rf"{prefix}/versions/1/[0-9a-f]{{32}}\.arrow")
        self.assertEqual(paths, {
            payload, f"{prefix}/revisions/1/0.json",
            f"{prefix}/revisions/1/latest.json", f"{prefix}/latest_version.json",
        })

    def test_invalid_names_are_rejected_before_storage_access(self):
        invalid = ("", ".", "..", "a\\b", "a%2fb", "Prices", "源", "a b",
                   "a.", "a-", "_a", "a\n", "con", "nul.data", "com1", "lpt9.txt",
                   "x" * 129)
        with patch("obstore.get") as get, patch("obstore.put") as put:
            for name in invalid:
                with self.subTest(name=name):
                    with self.assertRaises(ValueError):
                        self.cache(name)
                    with self.assertRaises(ValueError):
                        self.cache(f"market-data/{name}")
                    with self.assertRaises(ValueError):
                        self.cache().write(pa.table({"a": [1]}), id=name)
                    with self.assertRaises(ValueError):
                        self.cache().get_version((name, 1))
                    with self.assertRaises(ValueError):
                        self.cache().get_revision((name, 1, 0))
            get.assert_not_called()
            put.assert_not_called()

    def test_namespace_separators_require_valid_nonempty_segments(self):
        for namespace in ("/a", "a/", "a//b", "a/./b", "a/../b"):
            with self.subTest(namespace=namespace), self.assertRaises(ValueError):
                self.cache(namespace)
        with self.assertRaises(ValueError):
            self.cache("a/b").write(pa.table({"a": [1]}), id="prices/close")

    def test_invalid_dependency_ids_cannot_be_persisted(self):
        for deps, weak in (((("source/a", 7),), ()), ((), (("Source", 7),))):
            with self.subTest(dependencies=deps, weak_dependencies=weak):
                details = DataRevisionDetails(deps, weak, None, AS_OF)
                with patch("obstore.put") as put, self.assertRaises(ValueError):
                    self.cache().write_revision(("prices", 1, 0), details)
                put.assert_not_called()

    def test_invalid_dependencies_allow_retry_with_the_same_version(self):
        for field in ("dependencies", "weak_dependencies"):
            for existing in (False, True):
                with self.subTest(field=field, existing=existing):
                    namespace = f"{field}-{int(existing)}"
                    writer = self.cache(namespace)
                    if existing:
                        self.publish(writer, 1)
                    before = {item["path"] for item in obstore.list(self.store()).collect()}
                    with self.assertRaises(ValueError):
                        self.publish(writer, 200, **{field: (("source/a", 7),)})
                    self.assertEqual(
                        {item["path"] for item in obstore.list(self.store()).collect()}, before,
                    )
                    # Invalid input must not consume a version.
                    _, update = self.publish(writer, 200, **{field: (("source.a", 7),)})
                    self.assertEqual(update.revision, (2 if existing else 1, 0))
                    self.assertEqual(self.cache(namespace).latest("prices"), ("prices", 2 if existing else 1, 0))

    def test_missing_records_raise_key_error_with_requested_key(self):
        reader = self.cache()
        for method, key in ((reader.latest, "missing"), (reader.get_version, ("missing", 1)),
                            (reader.get_revision, ("missing", 1, 0))):
            with self.subTest(key=key), self.assertRaises(KeyError) as error:
                method(key)
            self.assertEqual(error.exception.args, (key,))

    def test_permission_errors_are_not_mistaken_for_new_ids(self):
        with patch("obstore.get", side_effect=PermissionError("denied")), patch("obstore.put") as put:
            with self.assertRaises(PermissionError):
                self.publish(self.cache())
        put.assert_not_called()

    def test_history_objects_cannot_be_overwritten(self):
        writer = self.cache()
        original, _ = self.publish(writer)
        original_details = writer.get_revision(("prices", 1, 0))
        with self.assertRaises(obstore.exceptions.AlreadyExistsError):
            self.cache().write_version(("prices", 1), pa.table({"price": [999]}), original_details)
        writer.write_dependencies("prices", (("source", 1),))
        with self.assertRaises(obstore.exceptions.AlreadyExistsError):
            self.cache().write_revision(("prices", 1, 1), DataRevisionDetails(None, None, None, AS_OF))
        self.assertTrue(self.cache().get_version(("prices", 1)).equals(original, check_metadata=True))
        self.assertEqual(self.cache().get_revision(("prices", 1, 0)), original_details)

    def test_failed_revision_write_does_not_publish_a_new_head(self):
        writer = self.cache()
        self.publish(writer)
        with patch.object(writer, "_put_revision", side_effect=OSError("storage failed")):
            with self.assertRaises(OSError):
                self.publish(writer, 200)
        self.assertEqual(writer.latest("prices"), ("prices", 1, 0))
        self.assertEqual(self.cache().latest("prices"), ("prices", 1, 0))

    def test_missing_stale_invalid_and_ahead_hints_are_rebuilt(self):
        writer = self.cache()
        self.publish(writer)
        self.publish(writer)
        writer.write_dependencies("prices", (("source", 1),))
        for payload in (None, b"not json", b'"1"', b"true", b"0", b"1", b"999"):
            for name, expected in (("latest_version.json", b"2"), ("revisions/2/latest.json", b"1")):
                with self.subTest(payload=payload, name=name):
                    path = writer._path("prices", name)
                    if payload is None:
                        obstore.delete(writer._store, path)
                    else:
                        obstore.put(writer._store, path, payload)
                    self.assertEqual(self.cache().load_latest("prices"), ("prices", 2, 1))
                    self.assertEqual(bytes(obstore.get(writer._store, path).bytes()), expected)

    def test_corrupt_publication_is_not_treated_as_missing(self):
        writer = self.cache()
        self.publish(writer)
        path = writer._path("prices", "revisions/1/0.json")
        for payload in (b"not json", b"{}", b"null"):
            obstore.put(writer._store, path, payload)
            with self.subTest(payload=payload), self.assertRaisesRegex(ValueError, "Invalid stored publication"):
                self.cache().load_latest("prices")

    def test_session_reads_published_versions_from_shared_storage(self):
        writer = self.cache()
        self.publish(writer)
        session = DataFabricSession(self.cache())
        session.add_subscription("prices")
        self.assertEqual(session.get_consistent_cut()["prices"]["price"].to_pylist(), [1, None])
        table, update = self.publish(writer, 200)
        changed = session.apply_update(update)
        self.assertTrue(changed["prices"].equals(table, check_metadata=True))
        self.assertEqual(session.apply_update(update), {})

    def test_versions_are_contiguous_across_ids_restarts_and_metadata(self):
        for key in ("prices", "other"):
            for version, supplied in enumerate((b"9000", b"-3", b"invalid"), 1):
                written, update = self.cache().write(
                    pa.table({"value": [version]}, metadata={b"version": supplied}),
                    id=key, as_of=AS_OF - timedelta(days=version),
                )
                self.assertEqual(update.revision, (version, 0))
                self.assertEqual(written.schema.metadata[b"version"], str(version).encode())
                self.assertEqual(update.previous_revision, (version - 1, 0) if version > 1 else None)

    def test_same_version_race_cannot_mix_table_and_revision_metadata(self):
        for existing in (False, True):
            with self.subTest(existing=existing):
                first, second = self.cache(f"race-{int(existing)}"), self.cache(f"race-{int(existing)}")
                if existing:
                    self.publish(first)
                persist = first._put_revision
                def interleave(revision, details, **extra):
                    second.write(pa.table({"price": [222]}), id="prices", dependencies=(("winner", 2),))
                    persist(revision, details, **extra)
                with patch.object(first, "_put_revision", side_effect=interleave):
                    with self.assertRaises(obstore.exceptions.AlreadyExistsError):
                        first.write(pa.table({"price": [111]}), id="prices", dependencies=(("loser", 1),))
                reader = self.cache(f"race-{int(existing)}")
                head = reader.load_latest("prices")
                self.assertEqual(head, ("prices", 2 if existing else 1, 0))
                self.assertEqual(reader.get_version(head[:2])["price"].to_pylist(), [222])
                self.assertEqual(reader.get_revision(head).dependencies, (("winner", 2),))

    def test_same_revision_race_fails_without_overwriting_or_rebasing(self):
        first, second = self.cache(), self.cache()
        self.publish(first)
        persist = first.write_revision
        def interleave(revision, details):
            second.write_dependencies("prices", (("winner", 1),))
            persist(revision, details)
        with patch.object(first, "write_revision", side_effect=interleave):
            with self.assertRaises(obstore.exceptions.AlreadyExistsError):
                first.write_dependencies("prices", (("loser", 2),))
        reader = self.cache()
        self.assertEqual(reader.load_latest("prices"), ("prices", 1, 1))
        self.assertEqual(reader.get_revision(("prices", 1, 1)).dependencies, (("winner", 1),))
        with self.assertRaises(KeyError):
            reader.get_revision(("prices", 1, 2))

    def test_version_and_revision_writers_can_both_commit_in_either_order(self):
        for first_kind in ("version", "revision"):
            with self.subTest(first_kind=first_kind):
                first, second = self.cache(first_kind), self.cache(first_kind)
                self.publish(first)
                persist = first._put_revision
                def interleave(revision, details, **extra):
                    if first_kind == "version":
                        second.write_dependencies("prices", (("source", 1),))
                    else:
                        self.publish(second)
                    persist(revision, details, **extra)
                with patch.object(first, "_put_revision", side_effect=interleave):
                    if first_kind == "version":
                        self.publish(first)
                    else:
                        first.write_dependencies("prices", (("source", 1),))
                reader = self.cache(first_kind)
                self.assertEqual(reader.load_latest("prices"), ("prices", 2, 0))
                self.assertEqual(reader.load_latest_revision(("prices", 1)), ("prices", 1, 1))
                self.assertEqual(reader.get_revision(("prices", 2, 0)).previous_revision, (1, 0))
                self.assertEqual(list(reader.version_history("prices")), [("prices", 2, 0), ("prices", 1, 1)])

    def test_new_version_captures_stored_predecessor_snapshot(self):
        writer = self.cache()
        self.publish(writer)
        successor = self.cache()
        successor.latest("prices")
        writer.write_dependencies("prices", (("source", 1),))
        writer.write_dependencies("prices", (("source", 2),))
        _, update = self.publish(successor)
        self.assertEqual(update.revision, (2, 0))
        self.assertEqual(update.previous_revision, (1, 2))
        self.assertEqual(list(self.cache().version_history("prices")), [("prices", 2, 0), ("prices", 1, 2)])

    def test_dependency_write_uses_stored_version_and_noop_does_not_notify(self):
        stale = self.cache()
        self.publish(stale)
        current = self.cache()
        self.publish(current)
        current.write_dependencies("prices", (("source", 1),))
        transport = Mock(spec=DataFabricTransport)
        session = PollingDataFabricSession(stale, transport)
        _, unchanged = session.publish(id="prices", dependencies=(("source", 1),))
        self.assertEqual(unchanged.revision, (2, 1))
        transport.send.assert_not_called()
        _, changed = session.publish(id="prices", dependencies=(("source", 2),))
        self.assertEqual(changed.revision, (2, 2))
        transport.send.assert_called_once_with(changed)
        self.assertEqual(self.cache().get_revision(("prices", 2, 0)).previous_revision, (1, 0))

    def test_invalid_direct_dependency_write_does_not_consume_revision(self):
        writer = self.cache()
        self.publish(writer)
        with self.assertRaises(ValueError):
            writer.write_dependencies("prices", (("invalid/id", 1),))
        _, update = writer.write_dependencies("prices", (("valid-id", 1),))
        self.assertEqual(update.revision, (1, 1))

    def test_older_version_revision_and_noop_notify_only_when_changed(self):
        writer = self.cache()
        original, _ = self.publish(writer)
        self.publish(writer)
        transport = Mock(spec=DataFabricTransport)
        session = PollingDataFabricSession(writer, transport)
        data, update = session.publish(id="prices", version=1, expected_revision=0,
                                       dependencies=(("source", 1),))
        self.assertTrue(data.equals(original, check_metadata=True))
        self.assertEqual(update.revision, (1, 1))
        transport.send.assert_called_once_with(update)
        transport.send.reset_mock()
        self.assertEqual(session.publish(id="prices", version=1, dependencies=(("source", 1),))[1], update)
        transport.send.assert_not_called()
        with self.assertRaisesRegex(RuntimeError, "revision has changed"):
            session.publish(id="prices", version=1, expected_revision=0, dependencies=(("source", 2),))
        transport.send.assert_not_called()
        self.assertEqual(writer.latest("prices"), ("prices", 2, 0))
        self.assertEqual(self.cache().load_latest("prices"), ("prices", 2, 0))
        self.assertEqual(self.cache().load_latest_revision(("prices", 1)), ("prices", 1, 1))

    def test_interrupted_payload_does_not_consume_a_version(self):
        for existing in (False, True):
            writer = self.cache(f"interrupted-{int(existing)}")
            if existing:
                self.publish(writer)
            with patch.object(writer, "_put_revision", side_effect=OSError("interrupted")):
                with self.assertRaises(OSError):
                    self.publish(writer)
            fresh = self.cache(f"interrupted-{int(existing)}")
            _, update = self.publish(fresh)
            self.assertEqual(update.revision, (2 if existing else 1, 0))
            paths = [item["path"] for item in obstore.list(writer._store).collect()]
            self.assertFalse(any("/publications/" in path for path in paths))

    def test_committed_publications_recover_after_each_pointer_failure(self):
        for fail_method in ("write_latest_revision", "write_latest_version"):
            for existing in (False, True):
                namespace = f"failure-{fail_method}-{int(existing)}"
                writer = self.cache(namespace)
                if existing:
                    self.publish(writer)
                with patch.object(writer, fail_method, side_effect=OSError("interrupted")):
                    with self.assertRaises(OSError):
                        self.publish(writer)
                fresh = self.cache(namespace)
                version = 2 if existing else 1
                self.assertEqual(fresh.load_latest("prices"), ("prices", version, 0))
                self.assertEqual(self.publish(fresh)[1].revision, (version + 1, 0))

    def test_recovery_returns_old_version_updates_and_does_not_load_tables(self):
        writer = self.cache()
        self.publish(writer)
        self.publish(writer)
        with patch.object(writer, "write_latest_revision", side_effect=OSError("interrupted")):
            with self.assertRaises(OSError):
                writer.write_dependencies("prices", (("source", 1),), version=1)
        reader = self.cache()
        with patch("obstore.get", wraps=obstore.get) as get:
            updates = reader.recover("prices")
        self.assertEqual([u.revision for u in updates], [(1, 1), (2, 0)])
        self.assertEqual(updates[0].dependencies, (("source", 1),))
        self.assertFalse(any(call.args[1].endswith(".arrow") for call in get.call_args_list))
        for update in updates:
            reader.apply_update(update)
        self.assertEqual(reader.latest("prices"), ("prices", 2, 0))
        self.assertEqual(reader.get_latest_revision(("prices", 1)), ("prices", 1, 1))
        self.assertEqual(reader.recover("prices"), updates)
        self.assertEqual(reader.write_dependencies("prices", (("source", 2),), version=1)[1].revision, (1, 2))

    def test_delayed_pointer_writes_are_repaired_without_regressing_resolved_heads(self):
        writer = self.cache()
        self.publish(writer)
        writer.write_dependencies("prices", (("source", 1),))
        self.publish(writer)
        writer.write_dependencies("prices", (("source", 2),), version=1)
        writer.write_latest_version(("prices", 1))
        writer.write_latest_revision(("prices", 1, 0))
        fresh = self.cache()
        self.assertEqual(fresh.load_latest("prices"), ("prices", 2, 0))
        self.assertEqual(fresh.load_latest_revision(("prices", 1)), ("prices", 1, 2))

    def test_actual_parallel_writers_have_one_winner_per_publication(self):
        for kind in ("version", "revision"):
            with self.subTest(kind=kind):
                writers = [self.cache(f"parallel-{kind}") for _ in range(2)]
                if kind == "revision":
                    self.publish(writers[0])
                barrier = Barrier(2)
                def publish(index):
                    cache = writers[index]
                    persist = cache._put_revision
                    def together(*args, **kwargs):
                        barrier.wait(timeout=10)
                        persist(*args, **kwargs)
                    with patch.object(cache, "_put_revision", side_effect=together):
                        try:
                            data = pa.table({"owner": [index]}) if kind == "version" else None
                            return cache.write(data, id="prices", dependencies=((f"owner{index}", 1),))[1]
                        except obstore.exceptions.AlreadyExistsError:
                            return None
                with ThreadPoolExecutor(max_workers=2) as pool:
                    results = list(pool.map(publish, range(2)))
                self.assertEqual(sum(result is not None for result in results), 1)
                winner = next(i for i, result in enumerate(results) if result is not None)
                reader = self.cache(f"parallel-{kind}")
                head = reader.load_latest("prices")
                self.assertEqual(head, ("prices", 1, 0 if kind == "version" else 1))
                self.assertEqual(reader.get_revision(head).dependencies, ((f"owner{winner}", 1),))
                if kind == "version":
                    self.assertEqual(reader.get_version(head[:2])["owner"].to_pylist(), [winner])

    def test_recovered_old_version_notification_releases_waiting_cut(self):
        writer = self.cache()
        for value in (1, 2):
            writer.write(pa.table({"value": [value]}), id="x")
        writer.write(pa.table({"value": [1]}), id="m", dependencies=(("x", 1),))
        writer.write(pa.table({"value": [2]}), id="m", dependencies=(("x", 2),))
        for root in ("a", "c"):
            writer.write(pa.table({"value": [1]}), id=root)
            writer.write(pa.table({"value": [2]}), id=root, dependencies=(("m", 1),))
        writer.write(pa.table({"value": [1]}), id="b", dependencies=(("x", 2),))
        session = DataFabricSession(self.cache())
        session.add_subscriptions(("a", "c", "b"))
        self.assertEqual({key: table["value"][0].as_py() for key, table in session.get_consistent_cut().items()},
                         {"a": 1, "c": 1, "b": 1})
        with patch.object(writer, "write_latest_revision", side_effect=OSError("interrupted")):
            with self.assertRaises(OSError):
                writer.write_dependencies("m", (("x", 2),), version=1)
        deltas = {}
        for update in self.cache().recover("m"):
            deltas.update(session.apply_update(update))
        self.assertEqual({key: table["value"][0].as_py() for key, table in deltas.items()}, {"a": 2, "c": 2})
        for update in self.cache().recover("m"):
            self.assertEqual(session.apply_update(update), {})

    def test_root_history_search_includes_revisions_after_successor_snapshot(self):
        writer = self.cache()
        for value in (1, 2, 3):
            writer.write(pa.table({"value": [value]}), id="x")
        writer.write(pa.table({"value": [1]}), id="a", dependencies=(("x", 1),))
        writer.write(pa.table({"value": [2]}), id="a", dependencies=(("x", 3),))
        writer.write(pa.table({"value": [1]}), id="b", dependencies=(("x", 2),))
        writer.write_dependencies("a", (("x", 2),), version=1)
        self.assertEqual(writer.get_revision(("a", 2, 0)).previous_revision, (1, 0))
        session = DataFabricSession(self.cache())
        session.add_subscriptions(("a", "b"))
        cut = session.get_consistent_cut()
        self.assertEqual(cut["a"].schema.metadata[b"version"], b"1")
        self.assertEqual(session._subscriptions._subscriptions["a"].current_revision, ("a", 1, 1))



class MemoryStoreTests(StorageContract, unittest.TestCase):
    def setUp(self):
        shared = MemoryStore()
        self.store = lambda: shared


class LocalStoreTests(StorageContract, unittest.TestCase):
    def setUp(self):
        directory = self.enterContext(TemporaryDirectory())
        self.store = lambda: LocalStore(directory)


@unittest.skipUnless(os.getenv("HGRAPH_FABRIC_S3_ENDPOINT"), "Set HGRAPH_FABRIC_S3_ENDPOINT for S3 tests")
class S3StoreTests(StorageContract, unittest.TestCase):
    def setUp(self):
        prefix = f"fabric-test-{uuid4().hex}"
        self.store = lambda: S3Store(
            os.environ["HGRAPH_FABRIC_S3_BUCKET"], prefix=prefix,
            endpoint=os.environ["HGRAPH_FABRIC_S3_ENDPOINT"],
            region=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
            virtual_hosted_style_request=False, client_options={"allow_http": True},
        )
        self.addCleanup(self.delete_test_objects)

    def delete_test_objects(self):
        store = self.store()
        for item in obstore.list(store).collect():
            obstore.delete(store, item["path"])
