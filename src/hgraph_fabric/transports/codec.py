"""Shared JSON wire format for fabric update metadata."""

from dataclasses import asdict
from datetime import datetime
import json

from ..data_structures import DataUpdateDetails


def encode_update(update: DataUpdateDetails) -> bytes:
    record = asdict(update)
    record["as_of"] = update.as_of.isoformat()
    return json.dumps({"schema": 1, "update": record}, separators=(",", ":")).encode("utf-8")


def decode_update(payload: bytes) -> DataUpdateDetails:
    record = json.loads(payload)
    if not isinstance(record, dict) or type(record.get("schema")) is not int or record["schema"] != 1:
        raise ValueError("Unsupported fabric notification schema")
    fields = record["update"]
    if not isinstance(fields, dict) or not isinstance(fields.get("id"), str):
        raise ValueError("Notification requires a string data ID")

    def pair(value, first_type):
        if (not isinstance(value, list) or len(value) != 2
                or type(value[0]) is not first_type or type(value[1]) is not int):
            raise ValueError("Invalid notification version or revision")
        if first_type is int and value[1] < 0:
            raise ValueError("Revision count must be nonnegative")
        return tuple(value)

    def dependencies(value):
        if value is None:
            return None
        if not isinstance(value, list):
            raise ValueError("Dependencies must be an array or null")
        return tuple(pair(item, str) for item in value)

    return DataUpdateDetails(
        id=fields["id"],
        revision=pair(fields["revision"], int),
        dependencies=dependencies(fields["dependencies"]),
        weak_dependencies=dependencies(fields["weak_dependencies"]),
        as_of=datetime.fromisoformat(fields["as_of"]),
    )
