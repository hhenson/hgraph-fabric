"""DataCache persistence through Rust-backed obstore stores."""

from dataclasses import asdict
from datetime import datetime
import base64
import io
import json
import re

import obstore
from obstore.store import ObjectStore
import pyarrow as pa
import pyarrow.parquet as pq

from .data_cache import DataCache
from .data_structures import (
    DataId, DataNamespace, DataRevisionDetails, DataRevisionId,
    DataUpdateDetails, DataVersionId,
)


class ObjectStoreDataCache(DataCache):
    """Immutable publications with independently recoverable head pointers.

    A Parquet file atomically commits the table and revision-zero metadata.
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

    def _open_version(self, version_id, key):
        data_id, version = version_id
        path = self._path(data_id, f"versions/{_number(version)}.parquet")
        try:
            # Seekable range reads let revision-zero lookups read the footer
            # without downloading or decoding the entire table.
            return _ParquetReader(obstore.open_reader(self._store, path, buffer_size=0))
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
        path = self._path(data_id, f"revisions/{_number(version)}/{_number(count)}.json")
        try:
            raw = self._read(path, revision_id)
        except KeyError:
            if count != 0:
                raise
            return self._initial_record((data_id, version))
        return _decode_revision(raw, revision_id)

    def _initial_record(self, version_id):
        revision_id = (*version_id, 0)
        with self._open_version(version_id, revision_id) as source:
            try:
                metadata = pq.read_metadata(source).metadata
                raw = metadata[b"hgraph_fabric.revision_zero"]
            except (KeyError, TypeError, pa.ArrowInvalid) as error:
                raise ValueError(f"Invalid stored publication {revision_id!r}") from error
        return _decode_revision(raw, revision_id)

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
        if type(hint) is not int or not first <= hint < 10**20 or not exists(hint):
            candidate = first
            if not exists(candidate):
                raise KeyError(data_id)
        else:
            candidate = hint
        # Contiguous publication numbers let readers recover without a listing.
        # A concurrent writer can make this hint stale immediately; publication
        # records, rather than the pointer, remain the authority on every read.
        while candidate < 10**20 - 1 and exists(candidate + 1):
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
            count = self._head(data_id, f"revisions/{_number(version)}/latest.json", 0,
                               lambda count: self._exists((data_id, version, count)), rebuild=rebuild)
        except KeyError as error:
            raise KeyError(version_id) from error
        return data_id, version, count

    def load_version(self, version_id: DataVersionId) -> pa.Table:
        with self._open_version(version_id, version_id) as source:
            reader = pq.ParquetFile(source)
            _decode_revision(reader.metadata.metadata.get(b"hgraph_fabric.revision_zero"), (*version_id, 0))
            # Parquet exposes the publication footer key as schema metadata.
            # Restore the original schema, including temporal types normalized
            # by Parquet and any user metadata key with the same name.
            schema = pa.ipc.read_schema(pa.BufferReader(
                base64.b64decode(reader.metadata.metadata[b"ARROW:schema"])))
            return reader.read().cast(schema)

    def load_revision(self, revision_id: DataRevisionId) -> DataRevisionDetails:
        return self._record(revision_id)[1]

    def write_version(self, version_id: DataVersionId, data: pa.Table,
                      details: DataRevisionDetails) -> None:
        data_id, version = version_id
        _validate_dependency_names(details.dependencies, details.weak_dependencies)
        path = self._path(data_id, f"versions/{_number(version)}.parquet")
        sink = pa.BufferOutputStream()
        with pq.ParquetWriter(sink, data.schema, compression="zstd",
                              use_compliant_nested_type=False) as writer:
            writer.add_key_value_metadata({"hgraph_fabric.revision_zero": _encode_revision(details)})
            writer.write_table(data)
        payload = obstore.Bytes(memoryview(sink.getvalue()).cast("B"))
        # Only the writer that creates the version proceeds to write its JSON
        # record. The footer also retains that record if this writer stops here.
        obstore.put(self._store, path, payload, mode="create")
        self._put_initial_revision(version_id, details)

    def _put_initial_revision(self, version_id, details):
        revision_id = (*version_id, 0)
        try:
            self._put_revision(revision_id, details)
        except obstore.exceptions.AlreadyExistsError:
            # Recovery may have copied the footer while the winning publisher
            # was paused. Accept that identical record without overwriting it.
            if self._record(revision_id)[1] != details:
                raise ValueError(f"Revision-zero copy differs from publication {revision_id!r}")

    def write_revision(self, revision_id: DataRevisionId, details: DataRevisionDetails) -> None:
        _validate_dependency_names(details.dependencies, details.weak_dependencies)
        data_id, version, count = revision_id
        _number(count)
        if count <= 0:
            raise ValueError("Publish revision zero with its table using write_version")
        self._record((data_id, version, count - 1))
        self._put_revision(revision_id, details)

    def _put_revision(self, revision_id, details):
        data_id, version, count = revision_id
        obstore.put(self._store, self._path(data_id, f"revisions/{_number(version)}/{_number(count)}.json"),
                    _encode_revision(details).encode(), mode="create")

    def write_latest_version(self, version_id: DataVersionId) -> None:
        data_id, version = version_id
        obstore.put(self._store, self._path(data_id, "latest_version.json"), str(version).encode())

    def write_latest_revision(self, revision_id: DataRevisionId) -> None:
        data_id, version, count = revision_id
        obstore.put(self._store, self._path(data_id, f"revisions/{_number(version)}/latest.json"), str(count).encode())

    def recover(self, data_id: DataId) -> tuple[DataUpdateDetails, ...]:
        """Rebuild hints and return each version's head for notification delivery.

        Restore missing revision-zero JSON files and rebuild hints from version one.
        Callers apply/send these updates to cover notifications lost after a
        commit. Recovery itself neither changes reader caches nor sends messages.
        """
        latest_version = self._latest_version(data_id, rebuild=True)
        updates = []
        for version in range(1, latest_version + 1):
            path = self._path(data_id, f"revisions/{_number(version)}/{_number(0)}.json")
            try:
                self._read(path, (data_id, version, 0))
            except KeyError:
                _, initial = self._initial_record((data_id, version))
                self._put_initial_revision((data_id, version), initial)
            revision_id = self._revision_head((data_id, version), rebuild=True)
            details = self.load_revision(revision_id)
            updates.append(DataUpdateDetails(id=data_id, revision=revision_id[1:], **asdict(details)))
        return tuple(updates)


class _ParquetReader(io.RawIOBase):
    """Supply Python's file lifecycle API around obstore's seekable reader."""

    def __init__(self, source):
        self._source = source

    def readable(self):
        return True

    def seekable(self):
        return True

    def read(self, size=-1):
        return bytes(self._source.read(size))

    def seek(self, offset, whence=0):
        return self._source.seek(offset, whence)

    def tell(self):
        return self._source.tell()

    def close(self):
        if not self.closed:
            self._source.close()
        super().close()


def _number(value: int) -> str:
    if type(value) is not int or not 0 <= value < 10**20:
        raise ValueError("Storage numbers must be integers between 0 and 10**20 - 1")
    return f"{value:020d}"


def _encode_revision(details: DataRevisionDetails) -> str:
    value = asdict(details)
    value["as_of"] = details.as_of.isoformat()
    return json.dumps(value, separators=(",", ":"))


def _decode_revision(raw, revision_id):
    try:
        value = json.loads(bytes(raw))
        details = DataRevisionDetails(
            dependencies=_dependencies(value["dependencies"]),
            weak_dependencies=_dependencies(value["weak_dependencies"]),
            as_of=datetime.fromisoformat(value["as_of"]),
        )
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError(f"Invalid stored publication {revision_id!r}") from error
    return value, details


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
