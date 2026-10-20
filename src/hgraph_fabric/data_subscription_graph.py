from collections import deque

from .data_cache import DataCache
from .data_graph import DataEdgeGraph
from .data_structures import DataId, DataRevisionId


class DataSubscriptionGraph:
    """Retain full strong-dependency trees for one subscribed data ID."""

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
        graph = DataEdgeGraph()
        pending = deque([revision_id])
        queued = {revision_id[:2]}
        while pending:
            current = pending.popleft()
            details = cache.get_revision(current)
            for dependency in details.dependencies or ():
                graph.add((current[:2], dependency))
                if dependency not in queued:
                    pending.append(cache.get_latest_revision(dependency))
                    queued.add(dependency)

        self._revision_graphs[revision_id] = graph
        return graph
