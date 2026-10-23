"""DataCache persistence through Rust-backed obstore stores."""

from dataclasses import asdict
from datetime import datetime
import json

import obstore
from obstore.store import ObjectStore
import pyarrow as pa

from .data_cache import DataCache
from .data_structures import (
    DataId, DataNamespace, DataRevisionDetails, DataRevisionId,
    DataVersionId, LatestDataDetails,
)


class ObjectStoreDataCache(DataCache):
    """Store Arrow versions and JSON metadata in a configured object store.

    Use one coordinated writer per data ID, as required by DataCache's version
    allocation. Readers use separate caches and receive publication updates.
    Version/revision objects are create-only; the latest pointer is written last
    by DataCache.write. Storage failures propagate to the caller.
    """

    def __init__(self, store: ObjectStore, namespace: DataNamespace = "fabric") -> None:
        super().__init__()
        self._store = store
        self._prefix = f"namespace-{namespace.encode('utf-8').hex()}"

    def _path(self, data_id: DataId, name: str) -> str:
        # Hex preserves IDs exactly, including slashes, dot segments, empty IDs
        # and case distinctions on case-insensitive local filesystems.
        return f"{self._prefix}/id-{data_id.encode('utf-8').hex()}/{name}"

    def _read(self, path: str, key):
        try:
            return obstore.get(self._store, path).bytes()
        except FileNotFoundError as error:
            raise KeyError(key) from error

    def load_latest(self, data_id: DataId) -> DataRevisionId:
        value = json.loads(bytes(self._read(self._path(data_id, "latest.json"), data_id)))
        return (data_id, *_pair(value, int))

    def load_version(self, version_id: DataVersionId) -> pa.Table:
        data_id, version = version_id
        data = self._read(self._path(data_id, f"versions/{version}.arrow"), version_id)
        with pa.ipc.open_file(pa.BufferReader(data)) as reader:
            return reader.read_all()

    def load_revision(self, revision_id: DataRevisionId) -> DataRevisionDetails:
        data_id, version, count = revision_id
        data = self._read(self._path(data_id, f"revisions/{version}/{count}.json"), revision_id)
        value = json.loads(bytes(data))
        previous = value["previous_revision"]
        return DataRevisionDetails(
            dependencies=_dependencies(value["dependencies"]),
            weak_dependencies=_dependencies(value["weak_dependencies"]),
            previous_revision=None if previous is None else _pair(previous, int),
            as_of=datetime.fromisoformat(value["as_of"]),
        )

    def write_version(self, version_id: DataVersionId, data: pa.Table) -> None:
        data_id, version = version_id
        sink = pa.BufferOutputStream()
        with pa.ipc.new_file(sink, data.schema) as writer:
            writer.write_table(data)
        # obstore imports unsigned-byte buffers; Arrow exposes signed bytes.
        payload = obstore.Bytes(memoryview(sink.getvalue()).cast("B"))
        obstore.put(self._store, self._path(data_id, f"versions/{version}.arrow"),
                    payload, mode="create")

    def write_revision(self, revision_id: DataRevisionId, details: DataRevisionDetails) -> None:
        data_id, version, count = revision_id
        value = asdict(details)
        value["as_of"] = details.as_of.isoformat()
        obstore.put(self._store, self._path(data_id, f"revisions/{version}/{count}.json"),
                    json.dumps(value, separators=(",", ":")).encode("utf-8"), mode="create")

    def write_latest(self, data_id: DataId, details: LatestDataDetails) -> None:
        obstore.put(self._store, self._path(data_id, "latest.json"),
                    json.dumps(details.revision, separators=(",", ":")).encode("utf-8"))


def _pair(value, first_type):
    if (not isinstance(value, list) or len(value) != 2
            or type(value[0]) is not first_type or type(value[1]) is not int
            or (first_type is int and value[1] < 0)):
        raise ValueError("Invalid stored version or revision")
    return tuple(value)


def _dependencies(value):
    if value is None:
        return None
    if not isinstance(value, list):
        raise ValueError("Stored dependencies must be an array or null")
    return tuple(_pair(item, str) for item in value)
