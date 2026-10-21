from collections import deque

from .data_cache import DataCache
from .data_graph import DataEdgeGraph
from .data_structures import DataId, DataRevisionId, DataVersionId


class DataSubscriptionGraph:
    """Retain full strong-dependency trees for one subscribed data ID."""

    data_id: DataId
    latest_revision: DataRevisionId | None
    proposed_revision: DataRevisionId | None
    _current_revision: DataRevisionId | None
    _revision_graphs: dict[DataRevisionId, DataEdgeGraph]

    def __init__(self, data_id: DataId) -> None:
        self.data_id = data_id
        self.latest_revision = None
        self.proposed_revision = None
        self._current_revision = None
        self._revision_graphs = {}
        self._dependency_revisions: dict[DataRevisionId, dict[DataVersionId, DataRevisionId]] = {}

    @property
    def current_revision(self) -> DataRevisionId | None:
        return self._current_revision

    @current_revision.setter
    def current_revision(self, revision_id: DataRevisionId | None) -> None:
        """Select the current revision and discard older retained graphs."""
        if revision_id is not None:
            if revision_id[0] != self.data_id:
                raise ValueError("Revision belongs to a different data ID")
            for retained in self._revision_graphs.keys() | self._dependency_revisions.keys():
                if retained[1:] < revision_id[1:]:
                    self._revision_graphs.pop(retained, None)
                    self._dependency_revisions.pop(retained, None)
        self._current_revision = revision_id

    def latest(self, cache: DataCache) -> DataEdgeGraph:
        if self.latest_revision is None:
            raise LookupError("No latest revision is selected")
        return self.revision(self.latest_revision, cache)

    def current(self, cache: DataCache) -> DataEdgeGraph:
        if self.current_revision is None:
            raise LookupError("No current revision is selected")
        return self.revision(self.current_revision, cache)

    def proposed_graph(self, cache: DataCache) -> DataEdgeGraph:
        """Load the proposed tree without selecting current or pruning history."""
        if self.proposed_revision is None:
            raise LookupError("No proposed revision is selected")
        return self.revision(self.proposed_revision, cache)

    def invalidate_dependency(self, revision_id: DataRevisionId) -> set[DataRevisionId]:
        """Forget trees expanded through an older count of this exact version.

        Explicit root revisions stay pinned. Duplicate notifications and other
        data versions do not invalidate trees. Return the affected root revisions.
        """
        version_id = revision_id[:2]
        invalidated = {
            root for root, dependencies in self._dependency_revisions.items()
            if version_id in dependencies and dependencies[version_id] != revision_id
        }
        for root in invalidated:
            self._revision_graphs.pop(root, None)
            # Keep the old dependency stamps until rebuilding succeeds, so a
            # repeated notification can retry a failed refresh.
        return invalidated

    def revision(
        self, revision_id: DataRevisionId, cache: DataCache
    ) -> DataEdgeGraph:
        if revision_id[0] != self.data_id:
            raise ValueError("Revision belongs to a different data ID")
        if revision_id in self._revision_graphs:
            dependencies = self._dependency_revisions.get(revision_id, {})
            if all(cache.get_latest_revision(key) == value for key, value in dependencies.items()):
                return self._revision_graphs[revision_id]
            # Also cover historical search trees and cache updates received
            # outside the manager. A root key alone is not a freshness stamp.
        graph = DataEdgeGraph()
        dependencies = {}
        pending = deque([revision_id])
        queued = {revision_id[:2]}
        while pending:
            current = pending.popleft()
            details = cache.get_revision(current)
            for dependency in details.dependencies or ():
                graph.add((current[:2], dependency))
                if dependency not in queued:
                    dependencies[dependency] = cache.get_latest_revision(dependency)
                    pending.append(dependencies[dependency])
                    queued.add(dependency)

        self._revision_graphs[revision_id] = graph
        self._dependency_revisions[revision_id] = dependencies
        return graph
