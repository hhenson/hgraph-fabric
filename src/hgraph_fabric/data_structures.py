from dataclasses import dataclass
from datetime import datetime


type DataNamespace = str
type DataId = str
type DataVersion = int  # The int encoded datetime as milliseconds
type DataRevisionCount = int  # A sequence number of revisions for a given version
type DataRevision = tuple[DataVersion, DataRevisionCount]

type DataVersionId = tuple[DataId, DataVersion]
type DataRevisionId = tuple[DataId, DataVersion, DataRevisionCount]
type DependencyEdge = tuple[DataVersionId, DataVersionId]

type DataVersionKey = tuple[DataNamespace, DataVersionId]  # Required by the load/store the access data from S3
type DataRevisionKey = tuple[DataNamespace, DataRevisionId] # Required by the load/store the access data from S3
type DataIdKey = tuple[DataNamespace, DataId] # Required by the load/store the access data from S3 (for latest)


@dataclass(frozen=True)
class DataRevisionDetails:
    """
    The previous_revision is set when the details are stored at revision 0.
    This allows us to chain version.
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
