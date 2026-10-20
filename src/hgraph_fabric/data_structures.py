from dataclasses import dataclass


type DataNamespace = str
type DataId = str
type DataVersion = int
type DataRevision = int

type DataVersionId = tuple[DataId, DataVersion]
type DataRevisionId = tuple[DataId, DataVersion, DataRevision]
type DependencyEdge = tuple[DataVersionId, DataVersionId]

type DataVersionKey = tuple[DataNamespace, DataVersionId]
type DataRevisionKey = tuple[DataNamespace, DataRevisionId]
type DataIdKey = tuple[DataNamespace, DataId]


@dataclass(frozen=True)
class DataRevisionDetails:
    dependencies: tuple[DataVersionId, ...] | None
    weak_dependencies: tuple[DataVersionId, ...] | None


@dataclass(frozen=True)
class LatestDataDetails:
    revision_id: DataRevisionId
    previous_revision_id: DataRevisionId | None


@dataclass(frozen=True)
class DataUpdateDetails(LatestDataDetails, DataRevisionDetails):
    pass
