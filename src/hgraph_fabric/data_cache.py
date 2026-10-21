from abc import ABC, abstractmethod
from datetime import UTC, datetime, timedelta

from pyarrow import Table

from .data_structures import (
    DataId,
    LatestDataDetails,
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

    def latest(self, data_id: DataId) -> DataRevisionId:
        if data_id not in self._latest:
            self._latest[data_id] = self.load_latest(data_id)
        return self._latest[data_id]

    def get_data(self, version_id: DataVersionId) -> Table:
        if version_id not in self._data:
            self._data[version_id] = self.load_version(version_id)
        return self._data[version_id]

    def get_revision(self, revision_id: DataRevisionId) -> DataRevisionDetails:
        if revision_id not in self._revisions:
            self._revisions[revision_id] = self.load_revision(revision_id)
        return self._revisions[revision_id]

    def write(
        self,
        data: Table,
        id: DataId | None = None,
        dependencies: tuple[DataVersionId, ...] = (),
        weak_dependencies: tuple[DataVersionId, ...] = (),
        as_of: datetime | None = None,
    ) -> tuple[Table, DataUpdateDetails]:
        """Write a new data version and return its table and update notification.

        Explicit id/as_of override schema metadata. Missing as_of uses UTC now;
        missing version uses as_of in epoch milliseconds (naive times mean UTC).
        Advance any version not past latest to latest + 1, keeping as_of intact.
        Other metadata and the input table are preserved. Callers deliver the
        returned update to their associated subscription managers.
        """
        metadata = dict(data.schema.metadata or {})
        if id is None:
            if b"id" not in metadata:
                raise ValueError("Data ID is required as an argument or in table metadata")
            id = metadata[b"id"].decode("utf-8")
        if as_of is None:
            as_of = (datetime.fromisoformat(metadata[b"as_of"].decode("utf-8"))
                     if b"as_of" in metadata else datetime.now(UTC))
        if b"version" in metadata:
            version = int(metadata[b"version"])
        else:
            timestamp = as_of if as_of.tzinfo is not None else as_of.replace(tzinfo=UTC)
            version = (timestamp - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(milliseconds=1)

        try:
            previous = self.latest(id)
        except KeyError:
            previous = None  # Storage signals an unknown data ID with KeyError.
        if previous is not None:
            version = max(version, previous[1] + 1)

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
        )
        self.write_version((id, version), data)
        self.write_revision(revision_id, details)
        self.write_latest(id, LatestDataDetails((version, 0)))

        update = DataUpdateDetails(
            id=id, revision=(version, 0), previous_revision=details.previous_revision,
            dependencies=dependencies, weak_dependencies=weak_dependencies,
        )
        self._data[(id, version)] = data
        self.apply_update(update)
        return data, update

    def get_latest_revision(self, version_id: DataVersionId) -> DataRevisionId:
        """Walk revision-0 previous-version links from the latest revision."""
        if version_id not in self._latest_revisions:
            data_id, _ = version_id
            revision_id = self.latest(data_id)
            visited = set()
            while revision_id is not None:
                if revision_id[0] != data_id:
                    raise ValueError("Version history points to a different data ID")
                if revision_id[:2] == version_id:
                    self._latest_revisions[version_id] = revision_id
                    break
                if revision_id[:2] in visited:
                    raise ValueError("Cyclic version history")
                visited.add(revision_id[:2])
                header = self.get_revision((data_id, revision_id[1], 0))
                revision_id = (
                    (data_id, *header.previous_revision)
                    if header.previous_revision is not None
                    else None
                )
            else:
                raise KeyError(version_id)
        return self._latest_revisions[version_id]

    def apply_update(self, update: DataUpdateDetails) -> None:
        """Accept the supplied update as latest and cache its revision metadata."""
        revision_id = (update.id, *update.revision)
        self._latest[update.id] = revision_id
        self._revisions[revision_id] = DataRevisionDetails(
            dependencies=update.dependencies,
            weak_dependencies=update.weak_dependencies,
            previous_revision=update.previous_revision,
        )
        self._latest_revisions[revision_id[:2]] = revision_id

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
    def write_version(self, version_id: DataVersionId, data: Table) -> None:
        ...

    @abstractmethod
    def write_revision(self, revision_id: DataRevisionId, details: DataRevisionDetails) -> None:
        ...

    @abstractmethod
    def write_latest(self, data_id: DataId, details: LatestDataDetails) -> None:
        ...
