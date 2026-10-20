from .data_cache import DataCache
from .data_graph import DataEdgeGraph
from .data_structures import DataId, DataRevisionId


class DataSubscriptionGraph:
    """Retain full strong-dependency trees for one subscribed data ID.

    Weak dependencies are excluded. Tree construction is deferred until the
    cache provides a version-to-revision lookup.
    """

    data_id: DataId
    latest_revision: DataRevisionId | None
    current_revision: DataRevisionId | None
    _revision_graphs: dict[DataRevisionId, DataEdgeGraph]

    def __init__(self, data_id: DataId) -> None:
        self.data_id = data_id
        self.latest_revision = None
        self.current_revision = None
        self._revision_graphs = {}

    def latest(self) -> DataEdgeGraph:
        if self.latest_revision is None:
            raise LookupError("No latest revision is selected")
        return self._revision_graphs[self.latest_revision]

    def current(self) -> DataEdgeGraph:
        if self.current_revision is None:
            raise LookupError("No current revision is selected")
        return self._revision_graphs[self.current_revision]

    def revision(
        self, revision_id: DataRevisionId, cache: DataCache
    ) -> DataEdgeGraph:
        if revision_id[0] != self.data_id:
            raise ValueError("Revision belongs to a different data ID")
        if revision_id in self._revision_graphs:
            return self._revision_graphs[revision_id]
        raise NotImplementedError(
            "Building dependency trees requires a version-to-revision lookup"
        )
