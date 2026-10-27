import unittest
from dataclasses import FrozenInstanceError, fields
from datetime import UTC, datetime

from hgraph_fabric.data_structures import (
    DataRevisionDetails,
    DataUpdateDetails,
    LatestDataDetails,
)


AS_OF = datetime(2026, 10, 21, tzinfo=UTC)


class DataDetailsTests(unittest.TestCase):
    def test_nullable_fields(self):
        self.assertIsNone(DataRevisionDetails(None, None, as_of=AS_OF).dependencies)
        self.assertIsNone(DataRevisionDetails(None, None, as_of=AS_OF).weak_dependencies)
        self.assertNotIn("previous_revision", vars(DataRevisionDetails(None, None, as_of=AS_OF)))

    def test_update_combines_revision_and_dependency_details(self):
        as_of = datetime(2026, 10, 21, tzinfo=UTC)
        details = DataUpdateDetails(
            id="prices",
            revision=(2, 3),
            dependencies=(("source", 4),),
            weak_dependencies=(("reference", 5),),
            as_of=as_of,
        )
        self.assertIsInstance(details, LatestDataDetails)
        self.assertIsInstance(details, DataRevisionDetails)
        self.assertEqual(details.as_of, as_of)
        self.assertEqual(details.id, "prices")
        self.assertEqual(details.revision, (2, 3))
        self.assertNotIn("previous_revision", vars(details))
        self.assertEqual(details.dependencies, (("source", 4),))
        self.assertEqual(details.weak_dependencies, (("reference", 5),))

    def test_details_are_frozen(self):
        instances = (
            DataRevisionDetails(None, None, as_of=AS_OF),
            LatestDataDetails((1, 1)),
            DataUpdateDetails(
                id="prices",
                revision=(1, 1),
                dependencies=None,
                weak_dependencies=None, as_of=AS_OF,
            ),
        )
        for instance in instances:
            for field in fields(instance):
                with self.subTest(type=type(instance).__name__, field=field.name):
                    with self.assertRaises(FrozenInstanceError):
                        setattr(instance, field.name, None)
