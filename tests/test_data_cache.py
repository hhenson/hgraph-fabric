import unittest

from pyarrow import table

from hgraph_fabric.data_cache import DataCache
from hgraph_fabric.data_structures import (
    DataRevisionDetails,
    DataUpdateDetails,
    LatestDataDetails,
)


class MemoryDataCache(DataCache):
    def __init__(self):
        super().__init__()
        self.latest_values = {}
        self.data_values = {}
        self.revision_values = {}
        self.loads = []

    def load_latest(self, data_id):
        self.loads.append(("latest", data_id))
        return self.latest_values[data_id]

    def load_data(self, version_id):
        self.loads.append(("data", version_id))
        return self.data_values[version_id]

    def load_revision(self, revision_id):
        self.loads.append(("revision", revision_id))
        return self.revision_values[revision_id]


class DataCacheTests(unittest.TestCase):
    def setUp(self):
        self.cache = MemoryDataCache()
        self.cache.latest_values["prices"] = LatestDataDetails(("prices", 1, 1), None)
        self.cache.data_values[("prices", 1)] = table({"price": [10]})
        self.cache.revision_values[("prices", 1, 1)] = DataRevisionDetails(None, None)

    def test_repeated_reads_load_each_resource_once(self):
        for getter, key, expected in (
            (self.cache.latest, "prices", self.cache.latest_values["prices"]),
            (self.cache.get_data, ("prices", 1), self.cache.data_values[("prices", 1)]),
            (
                self.cache.get_revision,
                ("prices", 1, 1),
                self.cache.revision_values[("prices", 1, 1)],
            ),
        ):
            self.assertIs(getter(key), expected)
            self.assertIs(getter(key), expected)
        self.assertEqual(
            self.cache.loads,
            [("latest", "prices"), ("data", ("prices", 1)), ("revision", ("prices", 1, 1))],
        )

    def test_update_replaces_latest_and_retains_old_resources(self):
        old_latest = self.cache.latest("prices")
        old_data = self.cache.get_data(("prices", 1))
        old_revision = self.cache.get_revision(("prices", 1, 1))
        self.cache.loads.clear()
        update = DataUpdateDetails(
            revision_id=("prices", 2, 2),
            previous_revision_id=old_latest.revision_id,
            dependencies=(("source", 3),),
            weak_dependencies=None,
        )
        self.cache.apply_update(update)
        self.assertEqual(
            self.cache.latest("prices"),
            LatestDataDetails(("prices", 2, 2), ("prices", 1, 1)),
        )
        self.assertEqual(
            self.cache.get_revision(("prices", 2, 2)),
            DataRevisionDetails((("source", 3),), None),
        )
        self.assertIs(self.cache.get_data(("prices", 1)), old_data)
        self.assertIs(self.cache.get_revision(("prices", 1, 1)), old_revision)
        self.assertEqual(self.cache.loads, [])
        new_data = table({"price": [20]})
        self.cache.data_values[("prices", 2)] = new_data
        self.assertIs(self.cache.get_data(("prices", 2)), new_data)
        self.assertIs(self.cache.get_data(("prices", 2)), new_data)
        self.assertEqual(self.cache.loads, [("data", ("prices", 2))])

    def test_update_seeds_empty_cache_and_same_version_revision(self):
        for revision in (1, 2):
            update = DataUpdateDetails(
                revision_id=("prices", 1, revision),
                previous_revision_id=("prices", 1, 1) if revision == 2 else None,
                dependencies=None,
                weak_dependencies=(("reference", revision),),
            )
            self.cache.apply_update(update)
            self.assertEqual(self.cache.latest("prices").revision_id, update.revision_id)
            self.assertEqual(
                self.cache.get_revision(update.revision_id).weak_dependencies,
                update.weak_dependencies,
            )
        self.assertEqual(self.cache.loads, [])

    def test_failed_loads_are_retried(self):
        for getter, backing, key, value, kind in (
            (self.cache.latest, self.cache.latest_values, "other",
             LatestDataDetails(("other", 1, 1), None), "latest"),
            (self.cache.get_data, self.cache.data_values, ("other", 1),
             table({"price": [30]}), "data"),
            (self.cache.get_revision, self.cache.revision_values, ("other", 1, 1),
             DataRevisionDetails(None, None), "revision"),
        ):
            with self.subTest(resource=kind):
                with self.assertRaises(KeyError):
                    getter(key)
                backing[key] = value
                self.assertIs(getter(key), value)
                self.assertIs(getter(key), value)
                self.assertEqual(self.cache.loads.count((kind, key)), 2)

    def test_cache_instances_are_independent(self):
        other = MemoryDataCache()
        other.latest_values["prices"] = LatestDataDetails(("prices", 9, 9), None)
        self.assertEqual(self.cache.latest("prices").revision_id, ("prices", 1, 1))
        self.assertEqual(other.latest("prices").revision_id, ("prices", 9, 9))
