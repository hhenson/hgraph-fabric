import unittest

from pyarrow import table

from hgraph_fabric.data_cache import DataCache
from hgraph_fabric.data_structures import (
    DataRevisionDetails,
    DataUpdateDetails,
)
from hgraph_fabric.data_subscription_graph import DataSubscriptionGraph


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
        self.cache.latest_values["prices"] = ("prices", 1, 1)
        self.cache.data_values[("prices", 1)] = table({"price": [10]})
        self.cache.revision_values[("prices", 1, 1)] = DataRevisionDetails(None, None, None)

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
            previous_revision_id=old_latest,
            dependencies=(("source", 3),),
            weak_dependencies=None,
        )
        self.cache.apply_update(update)
        self.assertEqual(
            self.cache.latest("prices"),
            ("prices", 2, 2),
        )
        self.assertEqual(
            self.cache.get_revision(("prices", 2, 2)),
            DataRevisionDetails((("source", 3),), None, ("prices", 1, 1)),
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
            self.assertEqual(
                self.cache.get_latest_revision(("prices", 1)), update.revision_id
            )
            self.assertEqual(self.cache.latest("prices"), update.revision_id)
            self.assertEqual(
                self.cache.get_revision(update.revision_id).weak_dependencies,
                update.weak_dependencies,
            )
        self.assertEqual(self.cache.loads, [])

    def test_failed_loads_are_retried(self):
        for getter, backing, key, value, kind in (
            (self.cache.latest, self.cache.latest_values, "other",
             ("other", 1, 1), "latest"),
            (self.cache.get_data, self.cache.data_values, ("other", 1),
             table({"price": [30]}), "data"),
            (self.cache.get_revision, self.cache.revision_values, ("other", 1, 1),
             DataRevisionDetails(None, None, None), "revision"),
        ):
            with self.subTest(resource=kind):
                with self.assertRaises(KeyError):
                    getter(key)
                backing[key] = value
                self.assertIs(getter(key), value)
                self.assertIs(getter(key), value)
                self.assertEqual(self.cache.loads.count((kind, key)), 2)

    def test_latest_revision_lookup_is_scoped_to_version_and_cached(self):
        self.cache.latest_values["prices"] = ("prices", 3, 8)
        self.cache.revision_values.update({
            ("prices", 3, 0): DataRevisionDetails(None, None, ("prices", 2, 5)),
            ("prices", 2, 0): DataRevisionDetails(None, None, ("prices", 1, 3)),
        })
        for version, revision in ((3, 8), (2, 5), (1, 3)):
            for _ in range(2):
                self.assertEqual(
                    self.cache.get_latest_revision(("prices", version)),
                    ("prices", version, revision),
                )
        self.assertEqual(self.cache.loads, [
            ("latest", "prices"),
            ("revision", ("prices", 3, 0)),
            ("revision", ("prices", 2, 0)),
        ])
        self.cache.apply_update(DataUpdateDetails(
            revision_id=("prices", 3, 9),
            previous_revision_id=("prices", 3, 8),
            dependencies=None,
            weak_dependencies=None,
        ))
        self.assertEqual(self.cache.get_latest_revision(("prices", 3)), ("prices", 3, 9))
        self.assertEqual(self.cache.get_latest_revision(("prices", 1)), ("prices", 1, 3))
        self.assertEqual(len(self.cache.loads), 3)

    def test_version_lookup_raises_when_chain_ends_before_requested_version(self):
        self.cache.latest_values["prices"] = ("prices", 3, 8)
        self.cache.revision_values[("prices", 3, 0)] = DataRevisionDetails(
            None, None, ("prices", 1, 3)
        )
        self.cache.revision_values[("prices", 1, 0)] = DataRevisionDetails(None, None, None)
        for version in (2, 9):
            with self.subTest(version=version):
                with self.assertRaises(KeyError) as error:
                    self.cache.get_latest_revision(("prices", version))
                self.assertEqual(error.exception.args, (("prices", version),))
                self.assertNotIn(("prices", version), self.cache._latest_revisions)

    def test_version_lookup_can_retry_a_missing_header(self):
        self.cache.latest_values["prices"] = ("prices", 2, 5)
        with self.assertRaises(KeyError):
            self.cache.get_latest_revision(("prices", 1))
        self.assertNotIn(("prices", 1), self.cache._latest_revisions)
        self.cache.revision_values[("prices", 2, 0)] = DataRevisionDetails(
            None, None, ("prices", 1, 3)
        )
        self.assertEqual(self.cache.get_latest_revision(("prices", 1)), ("prices", 1, 3))
        self.assertEqual(self.cache.loads.count(("revision", ("prices", 2, 0))), 2)

    def test_version_lookup_rejects_cross_data_links_and_cycles(self):
        for previous in (("other", 1, 3), ("prices", 2, 4)):
            with self.subTest(previous=previous):
                cache = MemoryDataCache()
                cache.latest_values["prices"] = ("prices", 2, 5)
                cache.revision_values[("prices", 2, 0)] = DataRevisionDetails(
                    None, None, previous
                )
                with self.assertRaises(ValueError):
                    cache.get_latest_revision(("prices", 1))
                self.assertNotIn(("prices", 1), cache._latest_revisions)

    def test_dependency_expansion_uses_historical_version_header(self):
        self.cache.latest_values.update({"source": ("source", 2, 8), "leaf": ("leaf", 7, 4)})
        self.cache.revision_values.update({
            ("prices", 1, 1): DataRevisionDetails((("source", 1),), None, None),
            ("source", 2, 0): DataRevisionDetails(None, None, ("source", 1, 3)),
            ("source", 1, 3): DataRevisionDetails((("leaf", 7),), None, None),
            ("leaf", 7, 4): DataRevisionDetails(None, None, None),
        })
        graph = DataSubscriptionGraph("prices").revision(("prices", 1, 1), self.cache)
        self.assertEqual(set(graph), {
            (("prices", 1), ("source", 1)),
            (("source", 1), ("leaf", 7)),
        })
        self.assertIn(("revision", ("source", 2, 0)), self.cache.loads)
        self.assertNotIn(("revision", ("source", 2, 8)), self.cache.loads)

    def test_cache_instances_are_independent(self):
        other = MemoryDataCache()
        other.latest_values["prices"] = ("prices", 9, 9)
        self.assertEqual(self.cache.latest("prices"), ("prices", 1, 1))
        self.assertEqual(other.latest("prices"), ("prices", 9, 9))
