import unittest
from dataclasses import FrozenInstanceError, fields

from hgraph_fabric.data_structures import (
    DataRevisionDetails,
    DataUpdateDetails,
    LatestDataDetails,
)


class DataDetailsTests(unittest.TestCase):
    def test_nullable_fields(self):
        self.assertIsNone(DataRevisionDetails(None, None, None).dependencies)
        self.assertIsNone(DataRevisionDetails(None, None, None).weak_dependencies)
        self.assertIsNone(DataRevisionDetails(None, None, None).previous_revision_id)
        self.assertIsNone(LatestDataDetails(("prices", 1, 1), None).previous_revision_id)

    def test_update_combines_revision_and_dependency_details(self):
        details = DataUpdateDetails(
            revision_id=("prices", 2, 3),
            previous_revision_id=("prices", 1, 2),
            dependencies=(("source", 4),),
            weak_dependencies=(("reference", 5),),
        )
        self.assertIsInstance(details, LatestDataDetails)
        self.assertIsInstance(details, DataRevisionDetails)
        self.assertEqual(details.revision_id, ("prices", 2, 3))
        self.assertEqual(details.previous_revision_id, ("prices", 1, 2))
        self.assertEqual(details.dependencies, (("source", 4),))
        self.assertEqual(details.weak_dependencies, (("reference", 5),))

    def test_details_are_frozen(self):
        instances = (
            DataRevisionDetails(None, None, None),
            LatestDataDetails(("prices", 1, 1), None),
            DataUpdateDetails(
                revision_id=("prices", 1, 1),
                previous_revision_id=None,
                dependencies=None,
                weak_dependencies=None,
            ),
        )
        for instance in instances:
            for field in fields(instance):
                with self.subTest(type=type(instance).__name__, field=field.name):
                    with self.assertRaises(FrozenInstanceError):
                        setattr(instance, field.name, None)
