from dataclasses import dataclass
from datetime import datetime


type DataNamespace = str
type DataId = str
type DataVersion = int  # A per-dataset sequence starting at 1
type DataRevisionCount = int  # A sequence number of revisions for a given version
type DataRevision = tuple[DataVersion, DataRevisionCount]

MAX_DATA_VERSION = 9_999_999_999_999
MAX_DATA_REVISION_COUNT = 99_999

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
    """Immediate accepted inputs and timestamp for one immutable revision."""
    dependencies: tuple[DataVersionId, ...] | None
    weak_dependencies: tuple[DataVersionId, ...] | None
    as_of: datetime


@dataclass(frozen=True)
class LatestDataDetails:
    revision: DataRevision


@dataclass(frozen=True)
class DataUpdateDetails(LatestDataDetails, DataRevisionDetails):
    id: DataId
