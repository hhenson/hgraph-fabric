"""The same persistence contract for memory, filesystem and opt-in S3 stores."""
from datetime import datetime, timedelta, timezone
import os
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from uuid import uuid4

import pyarrow as pa

try:
    import obstore
    from obstore.store import LocalStore, MemoryStore, S3Store
except ImportError:
    raise unittest.SkipTest("Install the storage extra to test object stores")

from hgraph_fabric.object_store_cache import ObjectStoreDataCache
from hgraph_fabric.data_fabric_session import DataFabricSession
from hgraph_fabric.data_structures import DataRevisionDetails


AS_OF = datetime(2026, 10, 23, 12, 34, 56, 123456, tzinfo=timezone(timedelta(hours=4)))


class StorageContract:
    def cache(self, namespace="test"):
        return ObjectStoreDataCache(self.store(), namespace)

    def publish(self, cache, version=100, **kwargs):
        return cache.write(pa.table({"price": [1, None]}, metadata={b"version": str(version).encode()}),
                           id="prices", as_of=AS_OF, **kwargs)

    def test_fresh_cache_reads_arrow_schema_metadata_and_values(self):
        schema = pa.schema([
            pa.field("name", pa.dictionary(pa.int8(), pa.string()), metadata={b"field": b"\xff"}),
            pa.field("time", pa.timestamp("us", "UTC")),
        ], metadata={b"custom": b"\x00\xff", b"version": b"100"})
        data = pa.Table.from_pydict({"name": ["one", None], "time": [AS_OF, None]}, schema=schema)
        written, update = self.cache().write(data, id="prices", as_of=AS_OF)
        reader = self.cache()
        self.assertIsNone(reader.peek_latest("prices"))
        self.assertEqual(reader.latest("prices"), ("prices", 100, 0))
        with patch("obstore.get", wraps=obstore.get) as get:
            loaded = reader.get_version(("prices", 100))
            self.assertIs(reader.get_version(("prices", 100)), loaded)
        self.assertEqual(get.call_count, 1)
        self.assertTrue(loaded.equals(written, check_metadata=True))
        self.assertEqual(reader.get_revision(("prices", 100, 0)).as_of, update.as_of)

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
                self.cache().write_revision(("prices", 100, count), details)
                self.assertEqual(self.cache().get_revision(("prices", 100, count)), details)

    def test_sparse_history_and_dependency_only_revisions_survive_restart(self):
        writer = self.cache()
        original, _ = self.publish(writer)
        with patch("obstore.put", wraps=obstore.put) as put:
            writer.write_dependencies("prices", (("source", 7),), (("weak", 8),), AS_OF)
        self.assertEqual(put.call_count, 2)  # Revision + head; no Arrow rewrite.
        with patch("obstore.put", wraps=obstore.put) as put:
            writer.write_dependencies("prices", (("source", 7),), (("weak", 8),), AS_OF + timedelta(days=1))
        put.assert_not_called()
        self.publish(writer, 9000)
        reader = self.cache()
        self.assertEqual(list(reader.version_history("prices")), [("prices", 9000, 0), ("prices", 100, 1)])
        self.assertEqual(reader.get_latest_revision(("prices", 100)), ("prices", 100, 1))
        self.assertEqual(reader.get_revision(("prices", 9000, 0)).previous_revision, (100, 1))
        self.assertEqual(reader.get_revision(("prices", 100, 1)).weak_dependencies, (("weak", 8),))
        self.assertTrue(reader.get_version(("prices", 100)).equals(original, check_metadata=True))

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
            pa.table({"value": [1]}, metadata={b"version": b"100"}),
            id="prices.close", as_of=AS_OF,
        )
        paths = {item["path"] for item in obstore.list(self.store()).collect()}
        self.assertEqual(paths, {
            "market-data/equities/us/prices.close/versions/100.arrow",
            "market-data/equities/us/prices.close/revisions/100/0.json",
            "market-data/equities/us/prices.close/latest.json",
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
                    self.cache().write_revision(("prices", 100, 0), details)
                put.assert_not_called()

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
        original_details = writer.get_revision(("prices", 100, 0))
        with self.assertRaises(obstore.exceptions.AlreadyExistsError):
            self.cache().write_version(("prices", 100), pa.table({"price": [999]}))
        with self.assertRaises(obstore.exceptions.AlreadyExistsError):
            self.cache().write_revision(("prices", 100, 0), DataRevisionDetails(None, None, None, AS_OF))
        self.assertTrue(self.cache().get_version(("prices", 100)).equals(original, check_metadata=True))
        self.assertEqual(self.cache().get_revision(("prices", 100, 0)), original_details)

    def test_failed_revision_write_does_not_publish_a_new_head(self):
        writer = self.cache()
        self.publish(writer)
        with patch.object(writer, "write_revision", side_effect=OSError("storage failed")):
            with self.assertRaises(OSError):
                self.publish(writer, 200)
        self.assertEqual(writer.latest("prices"), ("prices", 100, 0))
        self.assertEqual(self.cache().latest("prices"), ("prices", 100, 0))

    def test_corrupt_head_is_not_mistaken_for_an_absent_id(self):
        writer = self.cache()
        self.publish(writer)
        for payload in (b"not json", b'["100",0]', b'[100,-1]', b'[100,true]'):
            obstore.put(writer._store, writer._path("prices", "latest.json"), payload)
            with self.subTest(payload=payload), patch("obstore.put") as put:
                with self.assertRaises(ValueError):
                    self.publish(self.cache(), 200)
                put.assert_not_called()

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
