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
    """Access to data resources and their revision metadata."""

    @abstractmethod
    def latest(self, data_id: DataId) -> LatestDataDetails:
        ...

    @abstractmethod
    def get_data(self, version_id: DataVersionId) -> Table:
        ...

    @abstractmethod
    def get_revision(self, revision_id: DataRevisionId) -> DataRevisionDetails:
        ...

    @abstractmethod
    def apply_update(self, update: DataUpdateDetails) -> None:
        ...
