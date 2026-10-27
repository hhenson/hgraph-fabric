from abc import ABC, abstractmethod
from collections.abc import Iterator
from datetime import UTC, datetime

from pyarrow import Table

from .data_structures import (
    DataId,
    DataRevisionDetails,
    DataRevisionId,
    DataUpdateDetails,
    DataVersionId,
)


class DataCache(ABC):
    """Cache data resources and metadata, with storage supplied by subclasses."""

    def __init__(self) -> None:
        self._latest: dict[DataId, DataRevisionId] = {}
        self._data: dict[DataVersionId, Table] = {}
        self._revisions: dict[DataRevisionId, DataRevisionDetails] = {}
        self._latest_revisions: dict[DataVersionId, DataRevisionId] = {}
        self._pending_revision_heads: dict[DataVersionId, DataRevisionId] = {}
        self._pending_latest_heads: dict[DataId, DataRevisionId] = {}

    def cached_ids(self) -> set[DataId]:
        """Return IDs with any cached data or metadata, without loading."""
        return set(self._latest) | {
            key[0] for cache in (self._data, self._revisions, self._latest_revisions)
            for key in cache
        }

    def peek_latest(self, data_id: DataId) -> DataRevisionId | None:
        """Return cached latest for an ID, or None, without loading."""
        return self._latest.get(data_id)

    def peek_version(self, version_id: DataVersionId) -> Table | None:
        """Return a cached table, or None, without loading."""
        return self._data.get(version_id)

    def peek_revision(self, revision_id: DataRevisionId) -> DataRevisionDetails | None:
        """Return cached revision details, or None, without loading."""
        return self._revisions.get(revision_id)

    def peek_latest_revision(self, version_id: DataVersionId) -> DataRevisionId | None:
        """Return cached latest for a version, or None, without walking history."""
        return self._latest_revisions.get(version_id)

    def latest(self, data_id: DataId) -> DataRevisionId:
        if data_id not in self._latest:
            stored = self.load_latest(data_id)
            latest = max(stored, self._pending_latest_heads.pop(data_id, stored))
            self._latest[data_id] = latest
            self._pending_revision_heads.pop(latest[:2], None)
        return self._latest[data_id]

    def get_version(self, version_id: DataVersionId) -> Table:
        if version_id not in self._data:
            self._data[version_id] = self.load_version(version_id)
        return self._data[version_id]

    def get_revision(self, revision_id: DataRevisionId) -> DataRevisionDetails:
        if revision_id not in self._revisions:
            self._revisions[revision_id] = self.load_revision(revision_id)
        return self._revisions[revision_id]

    def write(
        self,
        data: Table | None = None,
        id: DataId | None = None,
        dependencies: tuple[DataVersionId, ...] = (),
        weak_dependencies: tuple[DataVersionId, ...] = (),
        as_of: datetime | None = None,
        *,
        version: int | None = None,
        expected_revision: int | None = None,
    ) -> tuple[Table, DataUpdateDetails]:
        """Write a version, or revise dependencies when no table is supplied.

        Explicit id/as_of override schema metadata. Missing as_of uses UTC now;
        versions start at 1 and advance by one per dataset, independent of as_of.
        Input version metadata is replaced by the allocated version.
        Other metadata and the input table are preserved. Callers deliver the
        returned update to their associated subscription managers.

        Without a table, id is required. version selects an older data version;
        omitting it selects the stored latest version when the attempt begins.
        Unchanged dependencies return the existing table
        and update details, preserving as_of and performing no writes.
        """
        if data is None:
            if id is None:
                raise ValueError("Data ID is required when updating dependencies")
            return self.write_dependencies(id, dependencies, weak_dependencies, as_of,
                                           version=version, expected_revision=expected_revision)
        if version is not None or expected_revision is not None:
            raise ValueError("A table publication allocates its own version")
        metadata = dict(data.schema.metadata or {})
        if id is None:
            if b"id" not in metadata:
                raise ValueError("Data ID is required as an argument or in table metadata")
            id = metadata[b"id"].decode("utf-8")
        if as_of is None:
            as_of = (datetime.fromisoformat(metadata[b"as_of"].decode("utf-8"))
                     if b"as_of" in metadata else datetime.now(UTC))
        previous = self._publication_head(id)
        version = 1 if previous is None else previous[1] + 1

        metadata.update({
            b"id": id.encode("utf-8"),
            b"version": str(version).encode("utf-8"),
            b"as_of": as_of.isoformat().encode("utf-8"),
        })
        data = data.replace_schema_metadata(metadata)
        revision_id = (id, version, 0)
        details = DataRevisionDetails(
            dependencies=dependencies,
            weak_dependencies=weak_dependencies,
            previous_revision=previous[1:] if previous is not None else None,
            as_of=as_of,
        )
        self.write_version((id, version), data, details)
        self.write_latest_revision(revision_id)
        self.write_latest_version((id, version))

        update = DataUpdateDetails(
            id=id, revision=(version, 0), previous_revision=details.previous_revision,
            dependencies=dependencies, weak_dependencies=weak_dependencies,
            as_of=as_of,
        )
        self._data[(id, version)] = data
        self._latest[id] = max(self._latest.get(id, revision_id), revision_id)
        self.apply_update(update)
        return data, update

    def write_dependencies(
        self,
        id: DataId,
        dependencies: tuple[DataVersionId, ...] = (),
        weak_dependencies: tuple[DataVersionId, ...] = (),
        as_of: datetime | None = None,
        *,
        version: int | None = None,
        expected_revision: int | None = None,
    ) -> tuple[Table, DataUpdateDetails]:
        """Revise the selected version's dependencies without rewriting its table.

        version defaults to latest at the start of the attempt. An optional
        expected_revision count rejects a computation based on stale metadata.

        An as_of-only change is a no-op. The returned table keeps the metadata
        from its version publication; the revision timestamp is in the update.
        """
        previous = (self._publication_head(id) if version is None else
                    self.load_latest_revision((id, version)))
        if previous is None:
            raise KeyError(id)
        if expected_revision is not None and previous[2] != expected_revision:
            raise RuntimeError("The target version's revision has changed")
        # Pin this version for the whole attempt. A concurrent new table does
        # not redirect a revision to data that this computation did not consume.
        details = self.get_revision(previous)
        data = self.get_version(previous[:2])
        changed = (dependencies != details.dependencies
                   or weak_dependencies != details.weak_dependencies)
        revision_id = previous
        if changed:
            revision_id = (id, previous[1], previous[2] + 1)
            details = DataRevisionDetails(
                dependencies=dependencies, weak_dependencies=weak_dependencies,
                previous_revision=None, as_of=as_of if as_of is not None else datetime.now(UTC),
            )
            self.write_revision(revision_id, details)
            self.write_latest_revision(revision_id)
        update = DataUpdateDetails(
            id=id, revision=revision_id[1:], previous_revision=details.previous_revision,
            dependencies=details.dependencies, weak_dependencies=details.weak_dependencies,
            as_of=details.as_of,
        )
        self.apply_update(update)
        return data, update

    def _publication_head(self, data_id: DataId) -> DataRevisionId | None:
        # Allocation must use storage, not a potentially stale reader cache.
        try:
            return self.load_latest(data_id)
        except KeyError:
            return None

    def get_latest_revision(self, version_id: DataVersionId) -> DataRevisionId:
        """Resolve this version's own head, including revisions after a successor."""
        if version_id not in self._latest_revisions:
            latest = self._latest.get(version_id[0])
            stored = latest if latest is not None and latest[:2] == version_id else self.load_latest_revision(version_id)
            self._latest_revisions[version_id] = max(
                stored, self._pending_revision_heads.pop(version_id, stored))
        return self._latest_revisions[version_id]

    def version_history(self, data_id: DataId) -> Iterator[DataRevisionId]:
        """Yield each version's latest known revision newest first, loading as needed."""
        return self._version_history(data_id, peek=False)

    def peek_version_history(self, data_id: DataId) -> Iterator[DataRevisionId]:
        """Yield the cached history prefix; stop at an uncached version head or header."""
        return self._version_history(data_id, peek=True)

    def _version_history(self, data_id: DataId, *, peek: bool) -> Iterator[DataRevisionId]:
        revision_id = self.peek_latest(data_id) if peek else self.latest(data_id)
        while revision_id is not None:
            if revision_id[0] != data_id:
                raise ValueError("Version history points to a different data ID")
            # A stored link captures a count at publication time. Include later
            # notifications, whether still pending or already resolved by lookup.
            version_id = revision_id[:2]
            revision_id = max(
                revision_id,
                self._pending_revision_heads.get(version_id, revision_id),
                self._latest_revisions.get(version_id, revision_id),
            )
            yield revision_id
            # The predecessor count is a publication-time snapshot. Resolve
            # the predecessor's own head: it can have received later revisions.
            header_id = (data_id, revision_id[1], 0)
            header = self.peek_revision(header_id) if peek else self.get_revision(header_id)
            if header is None:
                return
            previous = header.previous_revision
            # Strictly decreasing versions also rule out cycles without keeping
            # a set of every visited version for a long history.
            if previous is not None and previous[0] >= revision_id[1]:
                raise ValueError("Revision-zero history must point to an older version")
            if previous is None:
                return
            previous_id = (data_id, previous[0])
            revision_id = (self.peek_latest_revision(previous_id) if peek
                           else self.get_latest_revision(previous_id))

    def apply_update(self, update: DataUpdateDetails) -> None:
        """Cache revision metadata and advance known heads without regressing."""
        revision_id = (update.id, *update.revision)
        # Local writes may already be ahead of their notifications. An older
        # echo must not lower the head used to allocate the next version.
        latest = self._latest.get(update.id)
        if latest is not None:
            latest = self._latest[update.id] = max(latest, revision_id)
        else:
            # Reception starts without replay: a first notification can concern
            # an older version than the head already present in shared storage.
            self._pending_latest_heads[update.id] = max(
                self._pending_latest_heads.get(update.id, revision_id), revision_id,
            )
        self._revisions[revision_id] = DataRevisionDetails(
            dependencies=update.dependencies,
            weak_dependencies=update.weak_dependencies,
            previous_revision=update.previous_revision,
            as_of=update.as_of,
        )
        version_id = revision_id[:2]
        if version_id not in self._latest_revisions and (latest is None or latest[:2] != version_id):
            # A notification is a lower bound until history is loaded: it may
            # lag the stored count or advance beyond the count in the link.
            self._pending_revision_heads[version_id] = max(
                self._pending_revision_heads.get(version_id, revision_id), revision_id,
            )
            return
        version_latest = latest if latest is not None and latest[:2] == version_id else revision_id
        self._latest_revisions[version_id] = max(
            self._latest_revisions.get(version_id, revision_id), version_latest,
        )

    @abstractmethod
    def load_latest(self, data_id: DataId) -> DataRevisionId:
        ...

    @abstractmethod
    def load_version(self, version_id: DataVersionId) -> Table:
        ...

    @abstractmethod
    def load_revision(self, revision_id: DataRevisionId) -> DataRevisionDetails:
        ...

    @abstractmethod
    def load_latest_revision(self, version_id: DataVersionId) -> DataRevisionId:
        """Load the independently advancing head for a published data version."""
        ...

    @abstractmethod
    def write_version(self, version_id: DataVersionId, data: Table, details: DataRevisionDetails) -> None:
        """Atomically publish a table and revision zero; fail if already published."""
        ...

    @abstractmethod
    def write_revision(self, revision_id: DataRevisionId, details: DataRevisionDetails) -> None:
        """Atomically create a revision of an existing version; never overwrite."""
        ...

    @abstractmethod
    def write_latest_version(self, version_id: DataVersionId) -> None:
        """Update the recoverable dataset-version hint after publication."""
        ...

    @abstractmethod
    def write_latest_revision(self, revision_id: DataRevisionId) -> None:
        """Update the recoverable revision hint for this exact data version."""
        ...
