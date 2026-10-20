type DataId = str
type DataVersion = int
type DataRevision = int

type DataVersionId = tuple[DataId, DataVersion]
type DataRevisionId = tuple[DataId, DataVersion, DataRevision]
