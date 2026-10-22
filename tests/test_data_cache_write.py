from datetime import UTC, datetime, timedelta, timezone
from itertools import product
import unittest
from unittest.mock import patch

from pyarrow import table

from hgraph_fabric.data_structures import DataRevisionDetails, DataUpdateDetails, LatestDataDetails
from hgraph_fabric.data_subscription_manager import DataSubscriptionManager
from test_data_cache import MemoryDataCache


class DataCacheWriteTests(unittest.TestCase):
    def test_revision_timestamp_is_readable_without_stored_table(self):
        writer = MemoryDataCache()
        as_of = datetime(2026, 10, 21, 12, 34, 56, 123456, tzinfo=UTC)
        _, update = writer.write(table({"price": [10]}), id="prices", as_of=as_of)
        revision_id = (update.id, *update.revision)
        reader = MemoryDataCache()
        reader.revision_values = writer.revision_values.copy()
        with patch.object(reader, "load_version", side_effect=AssertionError("Table must not be loaded")):
            self.assertEqual(reader.get_revision(revision_id).as_of, as_of)
            self.assertEqual(reader.peek_revision(revision_id).as_of, as_of)
        self.assertEqual(reader.loads, [("revision", revision_id)])

    def test_update_preserves_timestamp_without_loading_table(self):
        cache = MemoryDataCache()
        for count in (0, 1):
            with self.subTest(count=count):
                as_of = datetime(2026, 10, 21, 12, count, tzinfo=UTC)
                update = DataUpdateDetails(
                    id="prices", revision=(7, count), previous_revision=None,
                    dependencies=(), weak_dependencies=(), as_of=as_of,
                )
                cache.apply_update(update)
                self.assertEqual(cache.get_revision(("prices", 7, count)).as_of, as_of)
        self.assertEqual(cache.loads, [])

    def test_peeks_observe_written_empty_tables_and_metadata_without_loading(self):
        cache = MemoryDataCache()
        as_of = datetime(1970, 1, 1, tzinfo=UTC)
        written, update = cache.write(table({"price": []}), id="prices", as_of=as_of)
        cache.loads.clear()
        self.assertIs(cache.peek_version(("prices", 0)), written)
        self.assertEqual(cache.peek_latest("prices"), ("prices", 0, 0))
        self.assertEqual(cache.peek_latest_revision(("prices", 0)), ("prices", 0, 0))
        self.assertEqual(cache.peek_revision(("prices", 0, 0)), DataRevisionDetails((), (), None, as_of=as_of))
        self.assertIsNone(cache.peek_version(("other", 0)))
        self.assertIsNone(cache.peek_version(("prices", 1)))
        self.assertEqual(cache.loads, [])

    def test_first_write_persists_in_order_and_populates_read_caches(self):
        cache = MemoryDataCache()
        original = table({"price": [10, None, 30]}, metadata={b"custom": b"\xff\x00"})
        as_of = datetime(1970, 1, 1, 0, 0, 1, 234567, tzinfo=UTC)
        written, update = cache.write(original, id="prices", as_of=as_of)
        self.assertEqual(written.schema.metadata, {
            b"custom": b"\xff\x00", b"id": b"prices", b"version": b"1234",
            b"as_of": b"1970-01-01T00:00:01.234567+00:00",
        })
        self.assertTrue(written.equals(original))
        self.assertEqual(original.schema.metadata, {b"custom": b"\xff\x00"})
        expected = DataUpdateDetails(id="prices", revision=(1234, 0), previous_revision=None,
                                     dependencies=(), weak_dependencies=(), as_of=as_of)
        self.assertEqual(update, expected)
        self.assertEqual([entry[:2] for entry in cache.writes], [
            ("version", ("prices", 1234)), ("revision", ("prices", 1234, 0)), ("latest", "prices"),
        ])
        self.assertIs(cache.writes[0][2], written)
        self.assertEqual(cache.writes[1][2], DataRevisionDetails((), (), None, as_of=as_of))
        self.assertEqual(cache.writes[2][2], LatestDataDetails((1234, 0)))
        cache.loads.clear()
        self.assertIs(cache.get_version(("prices", 1234)), written)
        self.assertEqual(cache.latest("prices"), ("prices", 1234, 0))
        self.assertEqual(cache.get_latest_revision(("prices", 1234)), ("prices", 1234, 0))
        self.assertEqual(cache.get_revision(("prices", 1234, 0)), DataRevisionDetails((), (), None, as_of=as_of))
        self.assertEqual(cache.loads, [])

    def test_table_metadata_supplies_id_version_and_as_of(self):
        metadata = {b"id": "pricés".encode(), b"version": b"7",
                    b"as_of": b"2026-10-21T12:00:00+04:00", b"custom": b"keep"}
        written, update = MemoryDataCache().write(table({"price": []}, metadata=metadata))
        self.assertEqual(written.schema.metadata, metadata)
        self.assertEqual(update.as_of, datetime.fromisoformat(metadata[b"as_of"].decode()))
        self.assertEqual(update.id, "pricés")
        self.assertEqual(update.revision, (7, 0))  # Supplied version outranks as_of-derived version.

    def test_explicit_id_and_as_of_override_metadata(self):
        cache = MemoryDataCache()
        original = table({"price": [10]}, metadata={
            b"id": b"old-id", b"version": b"7", b"as_of": b"invalid-but-overridden",
        })
        as_of = datetime(1970, 1, 2, tzinfo=UTC)
        written, update = cache.write(original, id="new-id", as_of=as_of,
                                      dependencies=(("source", 100),), weak_dependencies=(("reference", 200),))
        self.assertEqual(written.schema.metadata, {
            b"id": b"new-id", b"version": b"7", b"as_of": b"1970-01-02T00:00:00+00:00",
        })
        self.assertEqual(update, DataUpdateDetails(
            id="new-id", revision=(7, 0), previous_revision=None, as_of=as_of,
            dependencies=(("source", 100),), weak_dependencies=(("reference", 200),),
        ))
        self.assertEqual(cache.loads, [("latest", "new-id")])
        self.assertNotIn("old-id", cache.latest_values)

    def test_derived_version_uses_resolved_as_of_in_epoch_milliseconds(self):
        for as_of, expected in (
            (datetime(1970, 1, 1, 0, 0, 0, 123999, tzinfo=UTC), 123),
            (datetime(1970, 1, 1, 4, 0, 0, 123999, tzinfo=timezone(timedelta(hours=4))), 123),
            (datetime(1970, 1, 1, 0, 0, 0, 123999), 123),  # Naive timestamps mean UTC.
            (datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=UTC), -1),
        ):
            for metadata_time in (False, True):
                with self.subTest(as_of=as_of, metadata_time=metadata_time):
                    metadata = {b"id": b"prices"}
                    if metadata_time:
                        metadata[b"as_of"] = as_of.isoformat().encode()
                    written, update = MemoryDataCache().write(
                        table({"price": [10]}, metadata=metadata),
                        as_of=None if metadata_time else as_of,
                    )
                    self.assertEqual(update.as_of, as_of)
                    self.assertEqual(update.revision, (expected, 0))
                    self.assertEqual(written.schema.metadata[b"version"], str(expected).encode())
                    self.assertEqual(written.schema.metadata[b"as_of"], as_of.isoformat().encode())

    def test_missing_as_of_uses_utc_now_even_with_metadata_version(self):
        now = datetime(1970, 1, 1, 0, 0, 1, 234567, tzinfo=UTC)
        for version in (None, b"7"):
            with self.subTest(version=version):
                metadata = {b"id": b"prices"}
                if version is not None:
                    metadata[b"version"] = version
                with patch("hgraph_fabric.data_cache.datetime", wraps=datetime) as clock:
                    clock.now.return_value = now
                    written, update = MemoryDataCache().write(table({"price": [10]}, metadata=metadata))
                    clock.now.assert_called_once_with(UTC)
                self.assertEqual(update.as_of, now)
                self.assertEqual(written.schema.metadata[b"as_of"], now.isoformat().encode())
                self.assertEqual(update.revision, (1234 if version is None else 7, 0))

    def test_versions_advance_past_latest_and_link_its_max_count(self):
        for candidate, supplied_version, warm in product((999, 1000, 5000), (False, True), (False, True)):
            with self.subTest(candidate=candidate, supplied_version=supplied_version, warm=warm):
                cache = MemoryDataCache()
                cache.latest_values["prices"] = ("prices", 1000, 17)
                old_data = table({"price": [10]})
                cache.data_values[("prices", 1000)] = old_data
                if warm:
                    cache.latest("prices")
                    cache.get_version(("prices", 1000))
                as_of = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=candidate)
                metadata = {b"version": str(candidate).encode()} if supplied_version else None
                written, update = cache.write(table({"price": [20]}, metadata=metadata), id="prices", as_of=as_of)
                expected = max(1001, candidate)
                self.assertEqual(update.revision, (expected, 0))
                self.assertEqual(update.as_of, as_of)
                self.assertEqual(cache.get_revision(("prices", expected, 0)).as_of, as_of)
                self.assertEqual(update.previous_revision, (1000, 17))
                self.assertEqual(written.schema.metadata[b"version"], str(expected).encode())
                self.assertEqual(written.schema.metadata[b"as_of"], as_of.isoformat().encode())
                self.assertIs(cache.get_version(("prices", 1000)), old_data)

    def test_repeated_timestamp_writes_form_a_readable_persisted_history(self):
        cache = MemoryDataCache()
        as_of = datetime(1970, 1, 1, 0, 0, 1, tzinfo=UTC)
        for value in range(3):
            written, update = cache.write(table({"price": [value]}), id="prices", as_of=as_of)
            self.assertEqual(update.revision, (1000 + value, 0))
            self.assertEqual(update.previous_revision, (999 + value, 0) if value else None)
        # A new cache instance must be able to reconstruct the persisted chain.
        reader = MemoryDataCache()
        reader.latest_values = cache.latest_values.copy()
        reader.data_values = cache.data_values.copy()
        reader.revision_values = cache.revision_values.copy()
        for value in range(3):
            self.assertEqual(reader.get_latest_revision(("prices", 1000 + value)), ("prices", 1000 + value, 0))
            self.assertEqual(reader.get_version(("prices", 1000 + value)).to_pydict(), {"price": [value]})

    def test_missing_id_or_invalid_metadata_fails_before_persistence(self):
        for metadata in (None, {b"id": b"prices", b"version": b"not-an-int"},
                         {b"id": b"prices", b"as_of": b"not-a-datetime"}):
            with self.subTest(metadata=metadata):
                cache = MemoryDataCache()
                with self.assertRaises(ValueError):
                    cache.write(table({"price": [10]}, metadata=metadata))
                self.assertEqual(cache.writes, [])

    def test_storage_failures_propagate_without_advancing_cached_latest(self):
        for failed in ("write_version", "write_revision", "write_latest"):
            with self.subTest(failed=failed):
                cache = MemoryDataCache()
                cache.latest_values["prices"] = ("prices", 1000, 17)
                as_of = datetime(1970, 1, 1, 0, 0, 2, tzinfo=UTC)
                with patch.object(cache, failed, side_effect=OSError("Storage unavailable")):
                    with self.assertRaises(OSError):
                        cache.write(table({"price": [20]}), id="prices", as_of=as_of)
                self.assertEqual(cache.latest("prices"), ("prices", 1000, 17))
                self.assertNotIn(("prices", 2000), cache._data)
                self.assertNotIn(("prices", 2000, 0), cache._revisions)
                # Processing stops here: no transaction rollback or retry contract.

    def test_latest_lookup_failure_is_not_treated_as_a_new_id(self):
        cache = MemoryDataCache()
        with patch.object(cache, "load_latest", side_effect=OSError("Storage unavailable")):
            with self.assertRaises(OSError):
                cache.write(table({"price": [10]}), id="prices")
        self.assertEqual(cache.writes, [])

    def test_returned_update_advances_associated_subscription(self):
        cache = MemoryDataCache()
        as_of = datetime(1970, 1, 1, tzinfo=UTC)
        cache.write(table({"price": [10]}), id="prices", as_of=as_of)
        manager = DataSubscriptionManager(cache)
        sub = manager.add_subscription("prices")
        manager.get_consistent_cut()
        written, update = cache.write(table({"price": [20]}), id="prices", as_of=as_of)
        self.assertEqual(sub.current_revision, ("prices", 0, 0))
        self.assertEqual(manager.apply_update(update), {("prices", 1)})
        self.assertEqual(sub.current_revision, ("prices", 1, 0))
        self.assertIs(cache.get_version(("prices", 1)), written)

    def test_as_of_only_dependency_publications_are_dropped_at_any_revision(self):
        cache = MemoryDataCache()
        original_time = datetime(2026, 10, 21, tzinfo=UTC)
        data, original = cache.write(table({"price": [10]}), id="prices", as_of=original_time)
        for deps in ((), (("source", 2),)):
            if deps:
                data, original = cache.write(id="prices", dependencies=deps, as_of=original_time)
            cache.writes.clear()
            for as_of in (None, original_time, original_time + timedelta(days=1)):
                returned, update = cache.write(id="prices", dependencies=deps, as_of=as_of)
                self.assertIs(returned, data)
                self.assertEqual(update, original)
                self.assertEqual(cache.latest("prices"), ("prices", *original.revision))
                self.assertEqual(cache.writes, [])

    def test_dependency_changes_increment_only_revision_and_preserve_version_header(self):
        cache = MemoryDataCache()
        original_time = datetime(2026, 10, 21, tzinfo=UTC)
        cache.write(table({"price": [9]}, metadata={b"version": b"10"}), id="prices", as_of=original_time)
        data, _ = cache.write(table({"price": [10]}, metadata={b"version": b"20"}), id="prices", as_of=original_time)
        header = cache.get_revision(("prices", 20, 0))
        cases = [((("source", 1),), ()), ((("source", 1),), (("reference", 2),)),
                 ((), (("reference", 2),)), ((), ())]
        for count, (deps, weak) in enumerate(cases, 1):
            cache.writes.clear()
            as_of = original_time + timedelta(seconds=count)
            returned, update = cache.write(id="prices", dependencies=deps, weak_dependencies=weak, as_of=as_of)
            self.assertIs(returned, data)
            self.assertEqual(update.revision, (20, count))
            self.assertIsNone(update.previous_revision)
            self.assertEqual(update.as_of, as_of)
            self.assertEqual((update.dependencies, update.weak_dependencies), (deps, weak))
            self.assertEqual([entry[:2] for entry in cache.writes], [
                ("revision", ("prices", 20, count)), ("latest", "prices"),
            ])
            self.assertEqual(cache.get_latest_revision(("prices", 20)), ("prices", 20, count))
        self.assertIs(cache.get_revision(("prices", 20, 0)), header)
        self.assertEqual(data.schema.metadata[b"as_of"], original_time.isoformat().encode())
        reader = MemoryDataCache()
        reader.latest_values = cache.latest_values
        reader.revision_values = cache.revision_values
        self.assertEqual(list(reader.version_history("prices")), [("prices", 20, 4), ("prices", 10, 0)])

    def test_dependency_revision_defaults_as_of_to_utc_now(self):
        cache = MemoryDataCache()
        cache.write(table({"price": [10]}), id="prices")
        now = datetime(2026, 10, 22, tzinfo=UTC)
        with patch("hgraph_fabric.data_cache.datetime", wraps=datetime) as clock:
            clock.now.return_value = now
            _, update = cache.write_dependencies("prices", dependencies=(("source", 1),))
            clock.now.assert_called_once_with(UTC)
        self.assertEqual(update.as_of, now)

    def test_identical_table_publication_still_creates_new_version(self):
        cache = MemoryDataCache()
        data, initial = cache.write(table({"price": [10]}), id="prices")
        _, update = cache.write(data)
        self.assertEqual(update.revision, (initial.revision[0] + 1, 0))
        self.assertEqual(update.previous_revision, initial.revision)

    def test_dependency_publication_requires_existing_id(self):
        cache = MemoryDataCache()
        with self.assertRaises(ValueError):
            cache.write()
        with self.assertRaises(KeyError):
            cache.write(id="missing")
        self.assertEqual(cache.writes, [])

    def test_failed_dependency_persistence_does_not_advance_cached_head(self):
        for failed in ("write_revision", "write_latest"):
            with self.subTest(failed=failed):
                cache = MemoryDataCache()
                _, initial = cache.write(table({"price": [10]}), id="prices")
                with patch.object(cache, failed, side_effect=OSError("Storage unavailable")):
                    with self.assertRaises(OSError):
                        cache.write(id="prices", dependencies=(("source", 1),))
                self.assertEqual(cache.latest("prices"), ("prices", *initial.revision))
