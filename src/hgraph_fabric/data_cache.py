from abc import ABC, abstractmethod

from pyarrow import Table

from .data_structures import (
    DataId,
    DataRevisionDetails,
    DataRevisionId,
    DataUpdateDetails,
    DataVersionId,
)


class DataCache(ABC):
    """Cache data resources and metadata, loading each missing key on demand."""

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
            self._data[version_id] = self.load_data(version_id)
        return self._data[version_id]

    def get_revision(self, revision_id: DataRevisionId) -> DataRevisionDetails:
        if revision_id not in self._revisions:
            self._revisions[revision_id] = self.load_revision(revision_id)
        return self._revisions[revision_id]

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
    def load_data(self, version_id: DataVersionId) -> Table:
        ...

    @abstractmethod
    def load_revision(self, revision_id: DataRevisionId) -> DataRevisionDetails:
        ...
