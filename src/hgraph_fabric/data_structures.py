from dataclasses import dataclass
from datetime import datetime


type DataNamespace = str
type DataId = str
type DataVersion = int  # A per-dataset sequence starting at 1
type DataRevisionCount = int  # A sequence number of revisions for a given version
type DataRevision = tuple[DataVersion, DataRevisionCount]

type DataVersionId = tuple[DataId, DataVersion]
type DataRevisionId = tuple[DataId, DataVersion, DataRevisionCount]
type DependencyEdge = tuple[DataVersionId, DataVersionId]

type DataVersionKey = tuple[DataNamespace, DataVersionId]  # Required by the load/store the access data from S3
type DataRevisionKey = tuple[DataNamespace, DataRevisionId] # Required by the load/store the access data from S3
type DataIdKey = tuple[DataNamespace, DataId] # Required by the load/store the access data from S3 (for latest)


def no_data_revision(data_id: DataId) -> DataRevisionId:
    """In-memory selection before version 1; never a stored publication.

    None continues to mean uninitialised. The zero marker sorts below every
    published revision and retains the root ID for strong-dependency checks.
    """
    return data_id, 0, 0


def is_no_data(revision: DataRevisionId) -> bool:
    return revision[1] == 0


@dataclass(frozen=True)
class DataRevisionDetails:
    """
    The previous_revision is set when the details are stored at revision 0.
    It records the preceding version and its revision at publication time.
    That count is a snapshot, not a final count: older versions remain revisable.
    """
    dependencies: tuple[DataVersionId, ...] | None
    weak_dependencies: tuple[DataVersionId, ...] | None
    previous_revision: DataRevision | None
    as_of: datetime


@dataclass(frozen=True)
class LatestDataDetails:
    revision: DataRevision


@dataclass(frozen=True)
class DataUpdateDetails(LatestDataDetails, DataRevisionDetails):
    id: DataId
