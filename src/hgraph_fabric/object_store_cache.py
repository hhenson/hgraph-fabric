"""DataCache persistence through Rust-backed obstore stores."""

from dataclasses import asdict
from datetime import datetime
import json
import re

import obstore
from obstore.store import ObjectStore
import pyarrow as pa

from .data_cache import DataCache
from .data_structures import (
    DataId, DataNamespace, DataRevisionDetails, DataRevisionId,
    DataUpdateDetails, DataVersionId, LatestDataDetails,
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
        self._prefix = "/".join(_name(segment) for segment in namespace.split("/"))

    def _path(self, data_id: DataId, name: str) -> str:
        return f"{self._prefix}/{_name(data_id)}/{name}"

    def _read(self, path: str, key):
        try:
            return obstore.get(self._store, path).bytes()
        except FileNotFoundError as error:
            raise KeyError(key) from error

    def write(
        self,
        data: pa.Table | None = None,
        id: DataId | None = None,
        dependencies: tuple[DataVersionId, ...] = (),
        weak_dependencies: tuple[DataVersionId, ...] = (),
        as_of: datetime | None = None,
    ) -> tuple[pa.Table, DataUpdateDetails]:
        # Reject invalid references before the create-only Arrow object is stored.
        _validate_dependency_names(dependencies, weak_dependencies)
        return super().write(data, id, dependencies, weak_dependencies, as_of)

    def load_latest(self, data_id: DataId) -> DataRevisionId:
        value = json.loads(bytes(self._read(self._path(data_id, "latest.json"), data_id)))
        return (data_id, *_pair(value, int))

    def write_dependencies(self, id, dependencies=(), weak_dependencies=(), as_of=None):
        _validate_dependency_names(dependencies, weak_dependencies)
        return super().write_dependencies(id, dependencies, weak_dependencies, as_of)

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

    def reserve_write(self, data_id: DataId, previous: DataRevisionId | None) -> None:
        # One immutable claim per predecessor serializes both kinds of write.
        # Unlike an expiring lease, a suspended writer cannot outlive its claim.
        head = "initial" if previous is None else f"{previous[1]}-{previous[2]}"
        path = self._path(data_id, f"publications/{head}.json")
        obstore.put(self._store, path, json.dumps(previous).encode("utf-8"), mode="create")

    def write_version(self, version_id: DataVersionId, data: pa.Table) -> None:
        data_id, version = version_id
        path = self._path(data_id, f"versions/{version}.arrow")
        sink = pa.BufferOutputStream()
        with pa.ipc.new_file(sink, data.schema) as writer:
            writer.write_table(data)
        # obstore imports unsigned-byte buffers; Arrow exposes signed bytes.
        payload = obstore.Bytes(memoryview(sink.getvalue()).cast("B"))
        obstore.put(self._store, path, payload, mode="create")

    def write_revision(self, revision_id: DataRevisionId, details: DataRevisionDetails) -> None:
        data_id, version, count = revision_id
        _validate_dependency_names(details.dependencies, details.weak_dependencies)
        value = asdict(details)
        value["as_of"] = details.as_of.isoformat()
        obstore.put(self._store, self._path(data_id, f"revisions/{version}/{count}.json"),
                    json.dumps(value, separators=(",", ":")).encode("utf-8"), mode="create")

    def write_latest(
        self, data_id: DataId, details: LatestDataDetails, previous: DataRevisionId | None,
    ) -> None:
        # The create-only predecessor claim excludes other conforming writers
        # through this check and the head write, including on LocalStore where
        # conditional object replacement is unavailable. Never silently rebase
        # a new version: its header must seal the actual predecessor's final count.
        if self._publication_head(data_id) != previous:
            raise RuntimeError(f"Publication head changed for {data_id!r}")
        obstore.put(self._store, self._path(data_id, "latest.json"),
                    json.dumps(details.revision, separators=(",", ":")).encode("utf-8"))


def _validate_dependency_names(*groups):
    for dependencies in groups:
        for dependency_id, _ in dependencies or ():
            _name(dependency_id)


def _name(value: str) -> str:
    # Lowercase avoids aliases on case-insensitive filesystems. Windows device
    # names remain reserved even with an extension, e.g. nul.data.
    if (not isinstance(value, str)
            or re.fullmatch(r"[a-z0-9](?:[a-z0-9._-]{0,126}[a-z0-9])?", value) is None
            or value.split(".", 1)[0] in {
                "con", "prn", "aux", "nul",
                *(f"com{i}" for i in range(1, 10)),
                *(f"lpt{i}" for i in range(1, 10)),
            }):
        raise ValueError(
            "Namespace segments and data IDs must be 1-128 lowercase ASCII letters, digits, "
            "dots, underscores or hyphens, start and end with a letter or digit, "
            "and not be a Windows device name"
        )
    return value


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
