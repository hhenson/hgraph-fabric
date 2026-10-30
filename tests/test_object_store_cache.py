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
import pyarrow.parquet as pq

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
        with patch("obstore.open_reader", wraps=obstore.open_reader) as get:
            loaded = reader.get_version(("prices", 1))
            self.assertIs(reader.get_version(("prices", 1)), loaded)
        self.assertEqual(get.call_count, 1)
        self.assertTrue(loaded.equals(written, check_metadata=True))
        self.assertEqual(reader.get_revision(("prices", 1, 0)).as_of, update.as_of)

    def test_empty_tables_round_trip(self):
        for index, data in enumerate((pa.table({}), pa.table({"a": pa.array([], type=pa.int64())}))):
            with self.subTest(index=index):
                table, update = self.cache().write(data, id=str(index))
                loaded = self.cache().get_version((str(index), update.revision[0]))
                self.assertTrue(loaded.equals(table, check_metadata=True))

    def test_parquet_normalized_temporal_types_restore_original_schema(self):
        schema = pa.schema([
            pa.field("date", pa.date64(), metadata={b"field": b"date"}),
            pa.field("time", pa.time32("s")),
            pa.field("timestamp", pa.timestamp("s", tz="UTC")),
        ], metadata={b"custom": b"temporal"})
        data = pa.Table.from_arrays([
            pa.array([0, 86400000, None], type=pa.date64()),
            pa.array([0, 60, None], type=pa.time32("s")),
            pa.array([0, 1, None], type=pa.timestamp("s", tz="UTC")),
        ], schema=schema)
        writer = self.cache()
        written, _ = writer.write(data, id="prices", as_of=AS_OF)
        loaded = self.cache().get_version(("prices", 1))
        self.assertTrue(loaded.schema.equals(written.schema, check_metadata=True))
        self.assertTrue(loaded.equals(writer.get_version(("prices", 1)), check_metadata=True))

    def test_no_data_selection_then_first_publication_uses_shared_store(self):
        reader, writer = self.cache(), self.cache()
        session = DataFabricSession(reader)
        sub = session.add_subscription("prices")
        self.assertEqual(session.get_consistent_cut(), {})
        self.assertEqual(sub.current_revision, ("prices", 0, 0))
        data, update = self.publish(writer)
        self.assertTrue(session.apply_update(update)["prices"].equals(data, check_metadata=True))
        self.assertEqual(sub.current_revision, ("prices", 1, 0))
        self.assertEqual(session.apply_update(update), {})

    def test_table_larger_than_default_upload_chunk_round_trips(self):
        data = pa.table({"blob": [os.urandom(6 * 1024 * 1024)]})
        table, update = self.cache().write(data, id="large", as_of=AS_OF)
        self.assertTrue(self.cache().get_version(("large", update.revision[0])).equals(table, check_metadata=True))

    def test_revision_metadata_preserves_nullable_dependencies_and_time(self):
        for count, deps in enumerate((None, (), (("source.a", 7), ("b", 99)))):
            with self.subTest(deps=deps):
                details = DataRevisionDetails(deps, deps, AS_OF)
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
        self.assertNotIn("previous_revision", vars(reader.get_revision(("prices", 2, 0))))
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
        self.assertEqual(paths, {
            f"{prefix}/versions/0000000000001.parquet",
            f"{prefix}/revisions/0000000000001/00000.json",
            f"{prefix}/revisions/0000000000001/latest.json", f"{prefix}/latest_version.json",
        })

    def test_version_and_revision_names_sort_numerically(self):
        writer = self.cache()
        for _ in range(12):
            self.publish(writer)
        for count in range(1, 13):
            writer.write_dependencies("prices", version=1, dependencies=(("source", count),))
        paths = sorted(item["path"] for item in obstore.list(writer._store).collect())
        versions = [path.rsplit("/", 1)[1] for path in paths if path.endswith(".parquet")]
        revisions = [path.rsplit("/", 1)[1] for path in paths
                     if "/revisions/0000000000001/" in path and not path.endswith("latest.json")]
        expected = [f"{number:013d}" for number in range(1, 13)]
        self.assertEqual(versions, [f"{number}.parquet" for number in expected])
        self.assertEqual(revisions, [f"{number:05d}.json" for number in range(13)])
        self.assertIn("test/prices/revisions/0000000000010/latest.json", paths)
        fresh = self.cache()
        self.assertEqual(fresh.load_latest("prices"), ("prices", 12, 0))
        self.assertEqual(fresh.load_latest_revision(("prices", 1)), ("prices", 1, 12))

    def test_parquet_uses_zstd_and_preserves_user_metadata(self):
        data = pa.table({"value": ["repeated"] * 1000}, metadata={
            b"hgraph_fabric.revision_zero": b"user metadata", b"custom": b"\x00\xff",
        })
        writer = self.cache()
        table, update = writer.write(data, id="prices", dependencies=(("source", 1),), as_of=AS_OF)
        path = "test/prices/versions/0000000000001.parquet"
        raw = bytes(obstore.get(writer._store, path).bytes())
        self.assertEqual(raw[:4], b"PAR1")
        parquet = pq.ParquetFile(pa.BufferReader(raw))
        self.assertEqual(parquet.metadata.row_group(0).column(0).compression, "ZSTD")
        initial = json.loads(parquet.metadata.metadata[b"hgraph_fabric.revision_zero"])
        self.assertEqual(initial["dependencies"], [["source", 1]])
        json_path = "test/prices/revisions/0000000000001/00000.json"
        self.assertEqual(json.loads(bytes(obstore.get(writer._store, json_path).bytes())), initial)
        self.assertTrue(self.cache().load_version(("prices", 1)).equals(table, check_metadata=True))
        self.assertEqual(self.cache().load_revision(("prices", 1, 0)).as_of, update.as_of)

    def test_revision_rollover_persists_new_zero_and_advances_subscriber(self):
        writer = self.cache()
        original, _ = self.publish(writer)
        # Seed only the boundary records; earlier revisions are not traversed.
        writer._put_revision(("prices", 1, 99998), DataRevisionDetails((), (), AS_OF))
        writer.write_latest_revision(("prices", 1, 99998))
        writer = self.cache()
        _, last = writer.write_dependencies("prices", weak_dependencies=(("audit", 1),), as_of=AS_OF)
        self.assertEqual(last.revision, (1, 99999))
        consumer = DataFabricSession(self.cache())
        consumer.add_subscription("prices")
        self.assertTrue(consumer.get_consistent_cut()["prices"].equals(original, check_metadata=True))
        transport = Mock(spec=DataFabricTransport)
        publisher = PollingDataFabricSession(self.cache(), transport)
        _, unchanged = publisher.publish(id="prices", weak_dependencies=(("audit", 1),))
        self.assertEqual(unchanged.revision, (1, 99999))
        transport.send.assert_not_called()
        newer_as_of = AS_OF + timedelta(days=1)
        data, update = publisher.publish(id="prices", weak_dependencies=(("audit", 2),), as_of=newer_as_of)
        self.assertEqual(update.revision, (2, 0))
        transport.send.assert_called_once_with(update)
        self.assertTrue(data.equals(original))
        changes = consumer.apply_update(update)
        self.assertEqual(set(changes), {"prices"})
        self.assertTrue(changes["prices"].equals(data, check_metadata=True))
        fresh = self.cache()
        self.assertEqual(list(fresh.version_history("prices")), [("prices", 2, 0), ("prices", 1, 99999)])
        self.assertEqual(fresh.load_revision(("prices", 2, 0)), DataRevisionDetails((), (("audit", 2),), newer_as_of))
        self.assertEqual(fresh.load_revision(("prices", 1, 99999)).weak_dependencies, (("audit", 1),))
        paths = {item["path"] for item in obstore.list(writer._store).collect()}
        self.assertIn("test/prices/revisions/0000000000001/99999.json", paths)
        self.assertIn("test/prices/revisions/0000000000002/00000.json", paths)
        self.assertIn("test/prices/versions/0000000000002.parquet", paths)

    def test_competing_rollovers_cannot_overwrite_new_version_or_revision_zero(self):
        setup = self.cache()
        self.publish(setup)
        setup._put_revision(("prices", 1, 99999), DataRevisionDetails((), (), AS_OF))
        setup.write_latest_revision(("prices", 1, 99999))
        first, second = self.cache(), self.cache()
        persist = first.write_version
        def interleave(*args, **kwargs):
            second.write_dependencies("prices", weak_dependencies=(("winner", 1),))
            persist(*args, **kwargs)
        with patch.object(first, "write_version", side_effect=interleave), patch.object(
                first, "_put_initial_revision", wraps=first._put_initial_revision) as initial:
            with self.assertRaises(obstore.exceptions.AlreadyExistsError):
                first.write_dependencies("prices", weak_dependencies=(("loser", 1),))
        initial.assert_not_called()
        fresh = self.cache()
        self.assertEqual(fresh.load_latest("prices"), ("prices", 2, 0))
        self.assertEqual(fresh.load_revision(("prices", 2, 0)).weak_dependencies, (("winner", 1),))
        self.assertEqual(fresh.load_latest_revision(("prices", 1)), ("prices", 1, 99999))

    def test_revision_zero_reads_metadata_without_downloading_table(self):
        writer = self.cache()
        writer.write(pa.table({"blob": [os.urandom(1024 * 1024)]}), id="prices", as_of=AS_OF)
        # Normally the readable JSON is enough; the footer is the fallback
        # for a publisher interrupted between the table and JSON writes.
        with patch("obstore.open_reader", side_effect=AssertionError("Unnecessary footer read")):
            self.assertEqual(self.cache().load_revision(("prices", 1, 0)).as_of, AS_OF)
        obstore.delete(writer._store, "test/prices/revisions/0000000000001/00000.json")
        path = "test/prices/versions/0000000000001.parquet"
        size = obstore.head(writer._store, path)["size"]
        open_reader = obstore.open_reader
        bytes_read = 0
        def track_reads(*args, **kwargs):
            source = open_reader(*args, **kwargs)
            wrapped = Mock(wraps=source)
            def read(length=-1):
                nonlocal bytes_read
                data = source.read(length)
                bytes_read += len(data)
                return data
            wrapped.read.side_effect = read
            return wrapped
        with patch("obstore.open_reader", side_effect=track_reads), patch.object(
                pq.ParquetFile, "read", side_effect=AssertionError("Table decoded")):
            self.assertEqual(self.cache().load_revision(("prices", 1, 0)).as_of, AS_OF)
        self.assertGreater(bytes_read, 0)
        self.assertLess(bytes_read, size // 2)

    def test_invalid_storage_numbers_cannot_create_objects(self):
        writer = self.cache()
        details = DataRevisionDetails((), (), AS_OF)
        for number in (-1, True, 1.5, 10**20):
            with self.subTest(number=number), patch("obstore.put") as put:
                with self.assertRaises(ValueError):
                    writer.write_version(("prices", number), pa.table({"x": [1]}), details)
                with self.assertRaises(ValueError):
                    writer.write_revision(("prices", 1, number), details)
                put.assert_not_called()
        with patch("obstore.put") as put:
            with self.assertRaises(ValueError):
                writer.write_version(("prices", 10**13), pa.table({"x": [1]}), details)
            with self.assertRaises(ValueError):
                writer.write_revision(("prices", 1, 100000), details)
            put.assert_not_called()

    def test_corrupt_or_missing_initial_metadata_is_not_a_missing_version(self):
        writer = self.cache()
        path = "test/prices/versions/0000000000001.parquet"
        for record in (None, "null", "{}", "not json"):
            with self.subTest(record=record):
                sink = pa.BufferOutputStream()
                data = pa.table({"value": [1]})
                with pq.ParquetWriter(sink, data.schema) as parquet:
                    if record is not None:
                        parquet.add_key_value_metadata({"hgraph_fabric.revision_zero": record})
                    parquet.write_table(data)
                obstore.put(writer._store, path, sink.getvalue().to_pybytes())
                with self.assertRaisesRegex(ValueError, "Invalid stored publication"):
                    self.cache().load_latest("prices")
        obstore.put(writer._store, path, b"not parquet")
        with self.assertRaisesRegex(ValueError, "Invalid stored publication"):
            self.cache().load_latest("prices")

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
                details = DataRevisionDetails(deps, weak, AS_OF)
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
            self.cache().write_revision(("prices", 1, 1), DataRevisionDetails(None, None, AS_OF))
        self.assertTrue(self.cache().get_version(("prices", 1)).equals(original, check_metadata=True))
        self.assertEqual(self.cache().get_revision(("prices", 1, 0)), original_details)

    def test_failed_table_write_does_not_publish_a_new_head(self):
        writer = self.cache()
        self.publish(writer)
        with patch("obstore.put", side_effect=OSError("storage failed")):
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
            for name, expected in (("latest_version.json", b"2"), ("revisions/0000000000002/latest.json", b"1")):
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
        writer.write_dependencies("prices", (("source", 1),))
        path = writer._path("prices", "revisions/0000000000001/00001.json")
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
                self.assertNotIn("previous_revision", vars(update))

    def test_same_version_race_cannot_mix_table_and_revision_metadata(self):
        for existing in (False, True):
            with self.subTest(existing=existing):
                first, second = self.cache(f"race-{int(existing)}"), self.cache(f"race-{int(existing)}")
                if existing:
                    self.publish(first)
                persist = first.write_version
                def interleave(*args, **kwargs):
                    second.write(pa.table({"price": [222]}), id="prices", dependencies=(("winner", 2),))
                    persist(*args, **kwargs)
                with patch.object(first, "write_version", side_effect=interleave), patch.object(
                        first, "_put_initial_revision", wraps=first._put_initial_revision) as initial:
                    with self.assertRaises(obstore.exceptions.AlreadyExistsError):
                        first.write(pa.table({"price": [111]}), id="prices", dependencies=(("loser", 1),))
                initial.assert_not_called()
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
                method = "write_version" if first_kind == "version" else "_put_revision"
                persist = getattr(first, method)
                def interleave(*args, **kwargs):
                    if first_kind == "version":
                        second.write_dependencies("prices", (("source", 1),))
                    else:
                        self.publish(second)
                    persist(*args, **kwargs)
                with patch.object(first, method, side_effect=interleave):
                    if first_kind == "version":
                        self.publish(first)
                    else:
                        first.write_dependencies("prices", (("source", 1),))
                reader = self.cache(first_kind)
                self.assertEqual(reader.load_latest("prices"), ("prices", 2, 0))
                self.assertEqual(reader.load_latest_revision(("prices", 1)), ("prices", 1, 1))
                self.assertNotIn("previous_revision", vars(reader.get_revision(("prices", 2, 0))))
                self.assertEqual(list(reader.version_history("prices")), [("prices", 2, 0), ("prices", 1, 1)])

    def test_new_version_preserves_independent_older_revision_head(self):
        writer = self.cache()
        self.publish(writer)
        successor = self.cache()
        successor.latest("prices")
        writer.write_dependencies("prices", (("source", 1),))
        writer.write_dependencies("prices", (("source", 2),))
        _, update = self.publish(successor)
        self.assertEqual(update.revision, (2, 0))
        self.assertNotIn("previous_revision", vars(update))
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
        self.assertNotIn("previous_revision", vars(self.cache().get_revision(("prices", 2, 0))))

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

    def test_failed_table_create_does_not_consume_a_version(self):
        for existing in (False, True):
            writer = self.cache(f"interrupted-{int(existing)}")
            if existing:
                self.publish(writer)
            before = {item["path"] for item in obstore.list(writer._store).collect()}
            with patch("obstore.put", side_effect=OSError("interrupted")):
                with self.assertRaises(OSError):
                    self.publish(writer)
            self.assertEqual({item["path"] for item in obstore.list(writer._store).collect()}, before)
            fresh = self.cache(f"interrupted-{int(existing)}")
            _, update = self.publish(fresh)
            self.assertEqual(update.revision, (2 if existing else 1, 0))

    def test_recovery_restores_initial_json_from_table_after_failed_second_write(self):
        writer = self.cache()
        with patch.object(writer, "_put_initial_revision", side_effect=OSError("interrupted")):
            with self.assertRaises(OSError):
                writer.write(pa.table({"price": [123]}), id="prices",
                             dependencies=(("source", 1),), weak_dependencies=(("audit", 2),), as_of=AS_OF)
        path = "test/prices/revisions/0000000000001/00000.json"
        self.assertEqual({item["path"] for item in obstore.list(writer._store).collect()}, {
            "test/prices/versions/0000000000001.parquet",
        })
        reader = self.cache()
        initial = reader.load_revision(("prices", 1, 0))
        self.assertEqual(initial, DataRevisionDetails((("source", 1),), (("audit", 2),), AS_OF))
        self.assertEqual(reader.load_version(("prices", 1))["price"].to_pylist(), [123])
        reader.recover("prices")
        record = bytes(obstore.get(writer._store, path).bytes())
        self.assertEqual(json.loads(record), {
            "dependencies": [["source", 1]], "weak_dependencies": [["audit", 2]], "as_of": AS_OF.isoformat(),
        })
        reader.recover("prices")
        self.assertEqual(bytes(obstore.get(writer._store, path).bytes()), record)

    def test_publisher_resumes_after_recovery_creates_initial_json(self):
        writer = self.cache()
        persist = writer._put_initial_revision
        def recover_before_json(version, details):
            self.cache().recover("prices")
            persist(version, details)
        with patch.object(writer, "_put_initial_revision", side_effect=recover_before_json):
            _, update = writer.write(pa.table({"price": [123]}), id="prices", dependencies=(("source", 1),))
        reader = self.cache()
        self.assertEqual(update.revision, (1, 0))
        self.assertEqual(reader.load_revision(("prices", 1, 0)).dependencies, (("source", 1),))
        self.assertEqual(reader.load_version(("prices", 1))["price"].to_pylist(), [123])

    def test_recovery_restores_older_initial_json_without_copying_later_dependencies(self):
        writer = self.cache()
        self.publish(writer, dependencies=(("source", 1),))
        writer.write_dependencies("prices", dependencies=(("source", 2),))
        self.publish(writer, dependencies=(("source", 3),))
        path = "test/prices/revisions/0000000000001/00000.json"
        obstore.delete(writer._store, path)
        reader = self.cache()
        self.assertEqual([u.revision for u in reader.recover("prices")], [(1, 1), (2, 0)])
        self.assertEqual(json.loads(bytes(obstore.get(writer._store, path).bytes()))["dependencies"], [["source", 1]])

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
        with patch.object(pq.ParquetFile, "read", side_effect=AssertionError("Table read during recovery")):
            updates = reader.recover("prices")
        self.assertEqual([u.revision for u in updates], [(1, 1), (2, 0)])
        self.assertEqual(updates[0].dependencies, (("source", 1),))
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
                    method = "write_version" if kind == "version" else "_put_revision"
                    persist = getattr(cache, method)
                    def together(*args, **kwargs):
                        barrier.wait(timeout=10)
                        persist(*args, **kwargs)
                    with patch.object(cache, method, side_effect=together):
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

    def test_root_history_search_includes_revisions_after_successor_publication(self):
        writer = self.cache()
        for value in (1, 2, 3):
            writer.write(pa.table({"value": [value]}), id="x")
        writer.write(pa.table({"value": [1]}), id="a", dependencies=(("x", 1),))
        writer.write(pa.table({"value": [2]}), id="a", dependencies=(("x", 3),))
        writer.write(pa.table({"value": [1]}), id="b", dependencies=(("x", 2),))
        writer.write_dependencies("a", (("x", 2),), version=1)
        self.assertNotIn("previous_revision", vars(writer.get_revision(("a", 2, 0))))
        session = DataFabricSession(self.cache())
        session.add_subscriptions(("a", "b"))
        cut = session.get_consistent_cut()
        self.assertEqual(cut["a"].schema.metadata[b"version"], b"1")
        self.assertEqual(session._subscriptions._subscriptions["a"].current_revision, ("a", 1, 1))

    def test_failure_before_or_after_every_publication_write(self):
        # An error after put returns models a lost acknowledgement: the caller
        # sees failure even though that particular object is durably stored.
        for kind in ("initial", "version", "revision", "older-revision"):
            is_table = kind in ("initial", "version")
            commit = 1
            writes = 4 if is_table else 2
            for boundary in range(1, writes + 1):
                for after in (False, True):
                    with self.subTest(kind=kind, boundary=boundary, after=after):
                        namespace = f"failure-{kind}-{boundary}-{int(after)}"
                        writer = self.cache(namespace)
                        if kind != "initial":
                            writer.write(pa.table({"value": [1]}), id="prices", as_of=AS_OF)
                        if kind == "older-revision":
                            writer.write(pa.table({"value": [2]}), id="prices", as_of=AS_OF)
                        target = (2, 0) if kind == "version" else (1, 0 if kind == "initial" else 1)
                        args = {"id": "prices", "as_of": AS_OF}
                        if is_table:
                            args["data"] = pa.table({"value": [99]})
                        else:
                            args.update(version=1, weak_dependencies=(("audit", 1),))
                        transport = Mock(spec=DataFabricTransport)
                        session = PollingDataFabricSession(writer, transport)
                        put = obstore.put
                        calls = 0
                        def interrupted(*args, **kwargs):
                            nonlocal calls
                            calls += 1
                            if calls == boundary and not after:
                                raise OSError("interrupted before write")
                            result = put(*args, **kwargs)
                            if calls == boundary:
                                raise OSError("acknowledgement lost")
                            return result
                        with patch("obstore.put", side_effect=interrupted):
                            with self.assertRaises(OSError):
                                session.publish(**args)
                        self.assertEqual(calls, boundary)
                        transport.send.assert_not_called()
                        committed = boundary > commit or (boundary == commit and after)
                        reader = self.cache(namespace)
                        if kind == "initial" and not committed:
                            with self.assertRaises(KeyError):
                                reader.recover("prices")
                        else:
                            updates = reader.recover("prices")
                            heads = {u.revision[0]: u.revision for u in updates}
                            expected = {1: (1, 0)}
                            if kind == "older-revision":
                                expected[2] = (2, 0)
                            if committed:
                                expected[target[0]] = target
                            self.assertEqual(heads, expected)
                            for update in updates:
                                reader.apply_update(update)
                            self.assertEqual(reader.latest("prices")[1:], max(expected.values()))
                            if kind == "version" and not committed:
                                with self.assertRaises(KeyError):
                                    reader.load_version(("prices", target[0]))
                            value_version = target[0] if committed else 1
                            self.assertEqual(reader.load_version(("prices", value_version))["value"].to_pylist(),
                                             [99] if committed and is_table else [1])
                        # A failed create consumes no slot. A committed
                        # dependency retry is a no-op.
                        retry_transport = Mock(spec=DataFabricTransport)
                        retry = PollingDataFabricSession(self.cache(namespace), retry_transport)
                        _, result = retry.publish(**args)
                        if is_table:
                            self.assertEqual(result.revision, (target[0] + int(committed), 0))
                            retry_transport.send.assert_called_once_with(result)
                        else:
                            self.assertEqual(result.revision, target)
                            self.assertEqual(retry_transport.send.call_count, int(not committed))

    def test_suspended_publisher_cannot_regress_heads_after_recovery(self):
        for kind in ("version", "revision"):
            with self.subTest(kind=kind):
                slow, fast, reader = (self.cache(f"resume-{kind}") for _ in range(3))
                slow.write(pa.table({"value": [1]}), id="prices", as_of=AS_OF)
                reader.latest("prices")
                transport = Mock(spec=DataFabricTransport)
                session = PollingDataFabricSession(slow, transport)
                method = "write_latest_version" if kind == "version" else "write_latest_revision"
                persist = getattr(slow, method)
                recovery = []
                def resume_after_successor(head):
                    if kind == "version":
                        _, newer = fast.write(pa.table({"value": [3]}), id="prices", as_of=AS_OF)
                    else:
                        _, newer = fast.write_dependencies("prices", weak_dependencies=(("audit", 2),))
                    recovery.extend(self.cache(f"resume-{kind}").recover("prices"))
                    reader.apply_update(newer)
                    persist(head)  # The delayed hint now overwrites a newer one.
                with patch.object(slow, method, side_effect=resume_after_successor):
                    if kind == "version":
                        _, older = session.publish(pa.table({"value": [2]}), id="prices", as_of=AS_OF)
                    else:
                        _, older = session.publish(id="prices", weak_dependencies=(("audit", 1),))
                reader.apply_update(older)  # Notification delivery can also be reordered.
                expected = ("prices", 3, 0) if kind == "version" else ("prices", 1, 2)
                self.assertEqual(reader.latest("prices"), expected)
                self.assertEqual(self.cache(f"resume-{kind}").load_latest("prices"), expected)
                self.assertEqual(recovery[-1].revision, expected[1:])
                transport.send.assert_called_once_with(older)
                if kind == "version":
                    for version in (1, 2, 3):
                        self.assertEqual(reader.get_version(("prices", version))["value"].to_pylist(), [version])
                else:
                    self.assertEqual(reader.get_revision(("prices", 1, 1)).weak_dependencies, (("audit", 1),))
                    self.assertEqual(reader.get_revision(("prices", 1, 2)).weak_dependencies, (("audit", 2),))

    def test_publication_after_recovery_probe_is_visible_on_next_lookup(self):
        for kind in ("version", "revision"):
            with self.subTest(kind=kind):
                namespace = f"recovery-probe-{kind}"
                writer, recovery, reader = (self.cache(namespace) for _ in range(3))
                writer.write(pa.table({"value": [1]}), id="prices", as_of=AS_OF)
                reader.latest("prices")
                probe = ("prices", 2, 0) if kind == "version" else ("prices", 1, 1)
                exists = recovery._exists
                published = []
                def publish_after_missing(revision):
                    found = exists(revision)
                    if revision == probe and not found and not published:
                        if kind == "version":
                            _, update = writer.write(pa.table({"value": [2]}), id="prices", as_of=AS_OF)
                        else:
                            _, update = writer.write_dependencies("prices", weak_dependencies=(("audit", 1),))
                        published.append(update)
                        reader.apply_update(update)
                    return found
                with patch.object(recovery, "_exists", side_effect=publish_after_missing):
                    old_snapshot = recovery.recover("prices")
                self.assertEqual(len(published), 1)
                self.assertEqual(old_snapshot[-1].revision, (1, 0))
                for update in old_snapshot:
                    reader.apply_update(update)
                self.assertEqual(reader.latest("prices"), probe)
                self.assertEqual(self.cache(namespace).load_latest("prices"), probe)
                self.assertEqual(recovery.recover("prices")[-1], published[0])

    def test_recovery_between_version_and_revision_writes_keeps_both_updates(self):
        writer, recovery, reader = (self.cache() for _ in range(3))
        self.publish(writer)
        reader.latest("prices")
        load = recovery._revision_head
        def publish_after_dataset_scan(version, *, rebuild=False):
            # Recovery has already bounded its dataset scan at version one.
            _, new_version = self.publish(writer)
            _, old_revision = writer.write_dependencies("prices", version=1,
                                                       weak_dependencies=(("audit", 1),))
            reader.apply_update(new_version)
            reader.apply_update(old_revision)
            return load(version, rebuild=rebuild)
        with patch.object(recovery, "_revision_head", side_effect=publish_after_dataset_scan):
            snapshot = recovery.recover("prices")
        self.assertEqual([u.revision for u in snapshot], [(1, 1)])
        for update in snapshot:
            reader.apply_update(update)
        self.assertEqual(reader.latest("prices"), ("prices", 2, 0))
        self.assertEqual(reader.get_latest_revision(("prices", 1)), ("prices", 1, 1))
        self.assertEqual([u.revision for u in recovery.recover("prices")], [(1, 1), (2, 0)])

    def test_polling_publisher_pins_version_and_rejects_racing_revision(self):
        for competing in ("version", "revision"):
            with self.subTest(competing=competing):
                first, second = (self.cache(f"selection-race-{competing}") for _ in range(2))
                self.publish(first)
                transport = Mock(spec=DataFabricTransport)
                session = PollingDataFabricSession(first, transport)
                persist = first.write
                def interleave(*args, **kwargs):
                    if competing == "version":
                        self.publish(second)
                    else:
                        second.write_dependencies("prices", weak_dependencies=(("winner", 1),))
                    return persist(*args, **kwargs)
                with patch.object(first, "write", side_effect=interleave):
                    if competing == "version":
                        _, update = session.publish(id="prices", weak_dependencies=(("audit", 1),))
                        self.assertEqual(update.revision, (1, 1))
                        transport.send.assert_called_once_with(update)
                    else:
                        with self.assertRaisesRegex(RuntimeError, "revision has changed"):
                            session.publish(id="prices", weak_dependencies=(("loser", 1),))
                        transport.send.assert_not_called()
                fresh = self.cache(f"selection-race-{competing}")
                if competing == "version":
                    self.assertEqual(fresh.load_latest("prices"), ("prices", 2, 0))
                    self.assertEqual(fresh.load_latest_revision(("prices", 1)), ("prices", 1, 1))
                else:
                    self.assertEqual(fresh.load_latest("prices"), ("prices", 1, 1))
                    self.assertEqual(fresh.load_revision(("prices", 1, 1)).weak_dependencies, (("winner", 1),))

    def test_recovery_failure_at_every_pointer_write_can_be_retried(self):
        for boundary in range(1, 4):  # Dataset hint and two version heads.
            for after in (False, True):
                with self.subTest(boundary=boundary, after=after):
                    namespace = f"recovery-failure-{boundary}-{int(after)}"
                    writer = self.cache(namespace)
                    self.publish(writer)
                    writer.write_dependencies("prices", weak_dependencies=(("audit", 1),))
                    self.publish(writer)
                    expected = writer.recover("prices")
                    paths = [item["path"] for item in obstore.list(writer._store).collect()
                             if not item["path"].endswith(("/latest.json", "/latest_version.json"))]
                    immutable = {path: bytes(obstore.get(writer._store, path).bytes()) for path in paths}
                    put, calls = obstore.put, 0
                    def interrupted(*args, **kwargs):
                        nonlocal calls
                        calls += 1
                        if calls == boundary and not after:
                            raise OSError("recovery interrupted")
                        result = put(*args, **kwargs)
                        if calls == boundary:
                            raise OSError("recovery acknowledgement lost")
                        return result
                    with patch("obstore.put", side_effect=interrupted):
                        with self.assertRaises(OSError):
                            self.cache(namespace).recover("prices")
                    self.assertEqual(calls, boundary)
                    self.assertEqual(self.cache(namespace).recover("prices"), expected)
                    self.assertEqual({path: bytes(obstore.get(writer._store, path).bytes()) for path in paths}, immutable)
                    self.assertEqual(self.cache(namespace).write_dependencies(
                        "prices", version=1, weak_dependencies=(("audit", 2),))[1].revision, (1, 2))

    def test_notification_failure_after_commit_is_recovered_for_existing_session(self):
        for kind in ("version", "revision"):
            with self.subTest(kind=kind):
                namespace = f"notification-failure-{kind}"
                writer, reader = self.cache(namespace), self.cache(namespace)
                self.publish(writer)
                consumer = DataFabricSession(reader)
                consumer.add_subscription("prices")
                consumer.get_consistent_cut()
                transport = Mock(spec=DataFabricTransport)
                transport.send.side_effect = OSError("notification failed")
                session = PollingDataFabricSession(writer, transport)
                with self.assertRaises(OSError):
                    if kind == "version":
                        session.publish(pa.table({"price": [99]}), id="prices", as_of=AS_OF)
                    else:
                        session.publish(id="prices", weak_dependencies=(("audit", 1),))
                updates = self.cache(namespace).recover("prices")
                changes = {}
                for update in updates:
                    changes.update(consumer.apply_update(update))
                expected = ("prices", 2, 0) if kind == "version" else ("prices", 1, 1)
                self.assertEqual(consumer._subscriptions._subscriptions["prices"].current_revision, expected)
                self.assertEqual(set(changes), {"prices"} if kind == "version" else set())
                if changes:
                    self.assertEqual(changes["prices"]["price"].to_pylist(), [99])
                for update in updates:
                    self.assertEqual(consumer.apply_update(update), {})




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
