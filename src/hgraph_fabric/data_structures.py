from dataclasses import dataclass


type DataNamespace = str
type DataId = str
type DataVersion = int
type DataRevisionCount = int
type DataRevision = tuple[DataVersion, DataRevisionCount]

type DataVersionId = tuple[DataId, DataVersion]
type DataRevisionId = tuple[DataId, DataVersion, DataRevisionCount]
type DependencyEdge = tuple[DataVersionId, DataVersionId]

type DataVersionKey = tuple[DataNamespace, DataVersionId]
type DataRevisionKey = tuple[DataNamespace, DataRevisionId]
type DataIdKey = tuple[DataNamespace, DataId]


@dataclass(frozen=True)
class DataRevisionDetails:
    dependencies: tuple[DataVersionId, ...] | None
    weak_dependencies: tuple[DataVersionId, ...] | None
    previous_revision: DataRevision | None


@dataclass(frozen=True)
class LatestDataDetails:
    revision: DataRevision


@dataclass(frozen=True)
class DataUpdateDetails(LatestDataDetails, DataRevisionDetails):
    id: DataId
