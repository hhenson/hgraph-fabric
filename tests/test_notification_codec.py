from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
import unittest

from hgraph_fabric.data_structures import DataUpdateDetails
from hgraph_fabric.transports.codec import decode_update, encode_update


UPDATE = DataUpdateDetails(
    id="prices/東京", revision=(123456789012345678, 3), previous_revision=(42, 8),
    dependencies=(("source", 2),), weak_dependencies=(("reference", 99),),
    as_of=datetime(2026, 10, 22, 12, 34, 56, 123456, timezone(timedelta(hours=4))),
)


class NotificationCodecTests(unittest.TestCase):
    def test_round_trip_preserves_metadata_nulls_and_empty_dependencies(self):
        for dependencies in (None, (), UPDATE.dependencies):
            for weak in (None, (), UPDATE.weak_dependencies):
                for previous in (None, UPDATE.previous_revision):
                    value = replace(UPDATE, dependencies=dependencies,
                                    weak_dependencies=weak, previous_revision=previous)
                    self.assertEqual(decode_update(encode_update(value)), value)

    def test_malformed_or_unknown_messages_fail(self):
        payload = json.loads(encode_update(UPDATE))
        for value in (b"not-json", b"null", b'{"schema": 2}', b'{"schema": true}'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                decode_update(value)
        for key, value in (
            ("id", 1), ("revision", [1, True]), ("revision", [1, -1]),
            ("revision", ["1", 0]), ("dependencies", "source"),
            ("dependencies", [["source", 1.5]]), ("as_of", "not-a-date"),
        ):
            malformed = payload | {"update": payload["update"] | {key: value}}
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                decode_update(json.dumps(malformed).encode())
