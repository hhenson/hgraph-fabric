from datetime import UTC, datetime
import unittest

from pyarrow import table

from hgraph_fabric.data_structures import DataRevisionDetails
from test_data_cache import MemoryDataCache


AS_OF = datetime(2026, 10, 21, tzinfo=UTC)


class DataCacheHistoryTests(unittest.TestCase):
    def setUp(self):
        self.cache = MemoryDataCache()
        self.cache.latest_values["prices"] = ("prices", 3, 7)
        self.cache.revision_values.update({
            ("prices", 3, 0): DataRevisionDetails((), (), (2, 23), as_of=AS_OF),
            ("prices", 2, 0): DataRevisionDetails((), (), (1, 0), as_of=AS_OF),
            ("prices", 1, 0): DataRevisionDetails((), (), None, as_of=AS_OF),
        })
        self.expected = [("prices", 3, 7), ("prices", 2, 23), ("prices", 1, 0)]

    def test_cached_ids_include_payloads_and_metadata_without_loading(self):
        self.assertEqual(self.cache.cached_ids(), set())  # Storage is not the cache.
        self.cache.latest("prices")
        self.cache.data_values[("payload-only", 2)] = table({"x": []})
        self.cache.get_version(("payload-only", 2))
        self.cache.revision_values[("revision-only", 3, 2)] = DataRevisionDetails((), (), None, as_of=AS_OF)
        self.cache.get_revision(("revision-only", 3, 2))
        self.cache.loads.clear()
        expected = {"prices", "payload-only", "revision-only"}
        snapshot = self.cache.cached_ids()
        self.assertEqual(snapshot, expected)
        snapshot.clear()
        self.assertEqual(self.cache.cached_ids(), expected)
        self.assertEqual(self.cache.loads, [])
        self.assertEqual(self.cache.writes, [])

    def test_loading_history_is_lazy_and_follows_version_headers_with_final_counts(self):
        history = self.cache.version_history("prices")
        self.assertEqual(self.cache.loads, [])
        self.assertEqual(next(history), self.expected[0])
        self.assertEqual(self.cache.loads, [("latest", "prices")])
        self.assertEqual(next(history), self.expected[1])
        self.assertEqual(self.cache.loads[-1], ("revision", ("prices", 3, 0)))
        self.assertEqual(list(history), self.expected[2:])
        self.assertEqual(self.cache.loads, [
            ("latest", "prices"), ("revision", ("prices", 3, 0)),
            ("revision", ("prices", 2, 0)), ("revision", ("prices", 1, 0)),
        ])
        self.cache.loads.clear()
        self.assertEqual(list(self.cache.version_history("prices")), self.expected)
        self.assertEqual(self.cache.loads, [])

    def test_peek_history_stops_at_first_uncached_link(self):
        self.assertEqual(list(self.cache.peek_version_history("prices")), [])
        self.assertEqual(self.cache.loads, [])
        self.cache.latest("prices")
        self.cache.loads.clear()
        self.assertEqual(list(self.cache.peek_version_history("prices")), self.expected[:1])
        self.assertEqual(self.cache.loads, [])
        # A disconnected older header must not let a peek skip the missing link.
        self.cache.get_revision(("prices", 2, 0))
        self.cache.loads.clear()
        self.assertEqual(list(self.cache.peek_version_history("prices")), self.expected[:1])
        self.assertEqual(self.cache.loads, [])
        self.cache.get_revision(("prices", 3, 0))
        self.cache.loads.clear()
        # A previous-version pointer includes the maximum count. Yield its ID
        # even if that version's header is not yet available to continue walking.
        self.assertEqual(list(self.cache.peek_version_history("prices")), self.expected)
        self.assertIsNone(self.cache.peek_revision(("prices", 1, 0)))
        self.assertIsNone(self.cache.peek_latest_revision(("prices", 1)))
        self.assertEqual(self.cache.loads, [])
        self.assertEqual(self.cache.writes, [])

    def test_loading_history_and_peek_agree_after_cache_is_warmed(self):
        self.assertEqual(list(self.cache.version_history("prices")), self.expected)
        self.cache.loads.clear()
        self.assertEqual(list(self.cache.peek_version_history("prices")), self.expected)
        self.assertEqual(self.cache.loads, [])

    def test_loading_history_propagates_missing_head_or_header(self):
        with self.assertRaises(KeyError):
            next(self.cache.version_history("unknown"))
        del self.cache.revision_values[("prices", 2, 0)]
        history = self.cache.version_history("prices")
        self.assertEqual(next(history), self.expected[0])
        self.assertEqual(next(history), self.expected[1])
        with self.assertRaises(KeyError) as error:
            next(history)
        self.assertEqual(error.exception.args, (("prices", 2, 0),))

    def test_history_rejects_non_decreasing_links_in_both_modes(self):
        for previous in ((3, 6), (4, 1)):
            for peek in (False, True):
                with self.subTest(previous=previous, peek=peek):
                    cache = MemoryDataCache()
                    cache.latest_values["prices"] = ("prices", 3, 7)
                    cache.revision_values[("prices", 3, 0)] = DataRevisionDetails((), (), previous, as_of=AS_OF)
                    if peek:
                        cache.latest("prices")
                        cache.get_revision(("prices", 3, 0))
                    iterator = (cache.peek_version_history if peek else cache.version_history)("prices")
                    self.assertEqual(next(iterator), self.expected[0])
                    with self.assertRaises(ValueError):
                        next(iterator)

    def test_history_rejects_foreign_head_in_both_modes(self):
        self.cache.latest_values["prices"] = ("other", 3, 7)
        self.cache.latest("prices")
        for history in (self.cache.version_history, self.cache.peek_version_history):
            with self.assertRaises(ValueError):
                next(history("prices"))
