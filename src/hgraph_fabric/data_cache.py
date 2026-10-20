from abc import ABC, abstractmethod

from pyarrow import Table

from .data_structures import (
    DataId,
    DataRevisionDetails,
    DataRevisionId,
    DataUpdateDetails,
    DataVersionId,
    LatestDataDetails,
)


class DataCache(ABC):
    """Cache data resources and metadata, loading each missing key on demand."""

    def __init__(self) -> None:
        self._latest: dict[DataId, LatestDataDetails] = {}
        self._data: dict[DataVersionId, Table] = {}
        self._revisions: dict[DataRevisionId, DataRevisionDetails] = {}

    def latest(self, data_id: DataId) -> LatestDataDetails:
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

    def apply_update(self, update: DataUpdateDetails) -> None:
        """Accept the supplied update as latest and cache its revision metadata."""
        self._latest[update.revision_id[0]] = LatestDataDetails(
            revision_id=update.revision_id,
            previous_revision_id=update.previous_revision_id,
        )
        self._revisions[update.revision_id] = DataRevisionDetails(
            dependencies=update.dependencies,
            weak_dependencies=update.weak_dependencies,
        )

    @abstractmethod
    def load_latest(self, data_id: DataId) -> LatestDataDetails:
        ...

    @abstractmethod
    def load_data(self, version_id: DataVersionId) -> Table:
        ...

    @abstractmethod
    def load_revision(self, revision_id: DataRevisionId) -> DataRevisionDetails:
        ...
