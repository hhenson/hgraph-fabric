"""DataCache persistence through Rust-backed obstore stores."""

from dataclasses import asdict
from datetime import datetime
import json
import re
from uuid import uuid4

import obstore
from obstore.store import ObjectStore
import pyarrow as pa

from .data_cache import DataCache
from .data_structures import (
    DataId, DataNamespace, DataRevisionDetails, DataRevisionId,
    DataUpdateDetails, DataVersionId,
)


class ObjectStoreDataCache(DataCache):
    """Immutable publications with independently recoverable head pointers.

    Revision zero commits a version by naming its already-stored Arrow payload.
    Later revisions commit independently, including on older versions. Competing
    writers fail if they try to create the same publication. Head pointers are
    hints: reads check for subsequent publications and repair stale pointers.
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

    def write(self, data=None, id=None, dependencies=(), weak_dependencies=(), as_of=None,
              *, version=None, expected_revision=None):
        _validate_dependency_names(dependencies, weak_dependencies)
        return super().write(data, id, dependencies, weak_dependencies, as_of,
                             version=version, expected_revision=expected_revision)

    def write_dependencies(self, id, dependencies=(), weak_dependencies=(), as_of=None,
                           *, version=None, expected_revision=None):
        _validate_dependency_names(dependencies, weak_dependencies)
        return super().write_dependencies(id, dependencies, weak_dependencies, as_of,
                                          version=version, expected_revision=expected_revision)

    def _record(self, revision_id):
        data_id, version, count = revision_id
        raw = self._read(self._path(data_id, f"revisions/{version}/{count}.json"), revision_id)
        try:
            value = json.loads(bytes(raw))
            previous = value["previous_revision"]
            details = DataRevisionDetails(
                dependencies=_dependencies(value["dependencies"]),
                weak_dependencies=_dependencies(value["weak_dependencies"]),
                previous_revision=None if previous is None else _pair(previous, int),
                as_of=datetime.fromisoformat(value["as_of"]),
            )
            if count == 0 and re.fullmatch(r"[0-9a-f]{32}", value["payload"]) is None:
                raise ValueError("Invalid payload reference")
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(f"Invalid stored publication {revision_id!r}") from error
        return value, details

    def _exists(self, revision_id):
        try:
            self._record(revision_id)
            return True
        except KeyError:
            return False

    def _head(self, data_id, name, first, exists, *, rebuild=False):
        path = self._path(data_id, name)
        hint = None
        if not rebuild:
            try:
                hint = json.loads(bytes(self._read(path, data_id)))
            except (KeyError, ValueError):
                pass
        if type(hint) is not int or hint < first or not exists(hint):
            candidate = first
            if not exists(candidate):
                raise KeyError(data_id)
        else:
            candidate = hint
        # Contiguous publication numbers let readers recover without a listing.
        # A concurrent writer can make this hint stale immediately; publication
        # records, rather than the pointer, remain the authority on every read.
        while exists(candidate + 1):
            candidate += 1
        if rebuild or type(hint) is not int or hint != candidate:
            obstore.put(self._store, path, str(candidate).encode())
        return candidate

    def _latest_version(self, data_id, *, rebuild=False):
        return self._head(data_id, "latest_version.json", 1,
                          lambda version: self._exists((data_id, version, 0)), rebuild=rebuild)

    def load_latest(self, data_id: DataId) -> DataRevisionId:
        return self.load_latest_revision((data_id, self._latest_version(data_id)))

    def load_latest_revision(self, version_id: DataVersionId) -> DataRevisionId:
        return self._revision_head(version_id)

    def _revision_head(self, version_id, *, rebuild=False):
        data_id, version = version_id
        try:
            count = self._head(data_id, f"revisions/{version}/latest.json", 0,
                               lambda count: self._exists((data_id, version, count)), rebuild=rebuild)
        except KeyError as error:
            raise KeyError(version_id) from error
        return data_id, version, count

    def load_version(self, version_id: DataVersionId) -> pa.Table:
        data_id, version = version_id
        try:
            value, _ = self._record((*version_id, 0))
        except KeyError as error:
            raise KeyError(version_id) from error
        data = self._read(self._path(data_id, f"versions/{version}/{value['payload']}.arrow"), version_id)
        with pa.ipc.open_file(pa.BufferReader(data)) as reader:
            return reader.read_all()

    def load_revision(self, revision_id: DataRevisionId) -> DataRevisionDetails:
        return self._record(revision_id)[1]

    def write_version(self, version_id: DataVersionId, data: pa.Table,
                      details: DataRevisionDetails) -> None:
        data_id, version = version_id
        _validate_dependency_names(details.dependencies, details.weak_dependencies)
        token = uuid4().hex
        path = self._path(data_id, f"versions/{version}/{token}.arrow")
        sink = pa.BufferOutputStream()
        with pa.ipc.new_file(sink, data.schema) as writer:
            writer.write_table(data)
        payload = obstore.Bytes(memoryview(sink.getvalue()).cast("B"))
        obstore.put(self._store, path, payload, mode="create")
        # This single create is the commit. Racing writers have distinct payloads;
        # a loser or a crash before commit leaves an unreferenced payload only.
        self._put_revision((*version_id, 0), details, payload=token)

    def write_revision(self, revision_id: DataRevisionId, details: DataRevisionDetails) -> None:
        _validate_dependency_names(details.dependencies, details.weak_dependencies)
        data_id, version, count = revision_id
        if count <= 0:
            raise ValueError("Publish revision zero with its table using write_version")
        self._record((data_id, version, count - 1))
        self._put_revision(revision_id, details)

    def _put_revision(self, revision_id, details, **extra):
        data_id, version, count = revision_id
        value = asdict(details) | extra
        value["as_of"] = details.as_of.isoformat()
        obstore.put(self._store, self._path(data_id, f"revisions/{version}/{count}.json"),
                    json.dumps(value, separators=(",", ":")).encode(), mode="create")

    def write_latest_version(self, version_id: DataVersionId) -> None:
        data_id, version = version_id
        obstore.put(self._store, self._path(data_id, "latest_version.json"), str(version).encode())

    def write_latest_revision(self, revision_id: DataRevisionId) -> None:
        data_id, version, count = revision_id
        obstore.put(self._store, self._path(data_id, f"revisions/{version}/latest.json"), str(count).encode())

    def recover(self, data_id: DataId) -> tuple[DataUpdateDetails, ...]:
        """Rebuild hints and return each version's head for notification delivery.

        Scan committed publications from version one, ignoring existing hints.
        Callers apply/send these updates to cover notifications lost after a
        commit. Recovery itself neither changes reader caches nor sends messages.
        Unreferenced Arrow payloads are ignored and do not block future writes.
        """
        latest_version = self._latest_version(data_id, rebuild=True)
        updates = []
        for version in range(1, latest_version + 1):
            revision_id = self._revision_head((data_id, version), rebuild=True)
            details = self.load_revision(revision_id)
            updates.append(DataUpdateDetails(id=data_id, revision=revision_id[1:], **asdict(details)))
        return tuple(updates)

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
