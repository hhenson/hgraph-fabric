from collections import deque

from .data_cache import DataCache
from .data_graph import DataEdgeGraph
from .dependency_resolution import DependencyResolution, RevisionSearch
from .data_structures import DataId, DataRevisionId, DataVersionId


class InconsistentDependencyError(ValueError):
    """Well-formed records currently require different versions of one ID."""

    def __init__(self, revision: DataRevisionId, dependencies: set[DataVersionId]) -> None:
        super().__init__(f"Dependency paths disagree for revision {revision!r}")
        self.dependencies = dependencies


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
        self._proposed_resolution = None
        self._current_resolution = None
        self._proposed_ceilings = {}
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
        self._current_resolution = None

    def latest(self, cache: DataCache) -> DataEdgeGraph:
        if self.latest_revision is None:
            raise LookupError("No latest revision is selected")
        return self.revision(self.latest_revision, cache)

    def current(self, cache: DataCache) -> DataEdgeGraph:
        if self.current_revision is None:
            raise LookupError("No current revision is selected")
        if self._current_resolution is not None:
            return self._current_resolution.graph
        return self.revision(self.current_revision, cache)

    def proposed_graph(self, cache: DataCache) -> DataEdgeGraph:
        """Load the proposed tree without selecting current or pruning history."""
        if self.proposed_revision is None:
            raise LookupError("No proposed revision is selected")
        return self.proposal(cache).graph

    def proposal(self, cache: DataCache) -> DependencyResolution:
        if self.proposed_revision is None:
            raise LookupError("No proposed revision is selected")
        if self._proposed_resolution is None or self._proposed_resolution.root != self.proposed_revision:
            search = RevisionSearch(cache)
            try:
                graph = self.revision(self.proposed_revision, cache)
                revisions = self._dependency_revisions.get(self.proposed_revision, {})
                if any(revision[2] > self._proposed_ceilings.get(version, revision[2])
                       for version, revision in revisions.items()):
                    resolved = next(search.solutions(
                        self.proposed_revision, ceilings=self._proposed_ceilings), None)
                    if resolved is None:
                        raise InconsistentDependencyError(self.proposed_revision, set(revisions))
                    self._proposed_resolution = resolved
                else:
                    self._proposed_resolution = DependencyResolution(
                        self.proposed_revision, graph,
                        revisions | {self.proposed_revision[:2]: self.proposed_revision})
            except InconsistentDependencyError as error:
                resolved = next(search.solutions(
                    self.proposed_revision, ceilings=self._proposed_ceilings), None)
                if resolved is None:
                    error.dependencies.update(search.dependencies)
                    raise
                self._proposed_resolution = resolved
        return self._proposed_resolution

    def stage_resolution(self, resolution: DependencyResolution) -> None:
        self._proposed_resolution = resolution
        for version, revision in resolution.revisions.items():
            if version != resolution.root[:2]:
                self._proposed_ceilings[version] = min(
                    self._proposed_ceilings.get(version, revision[2]), revision[2])

    def reset_proposal(self) -> None:
        self.proposed_revision = self.latest_revision
        self._proposed_resolution = None
        self._proposed_ceilings = {}

    def commit_proposal(self, cache: DataCache) -> None:
        resolution = self.proposal(cache)
        if self.current_revision != self.proposed_revision:
            self.current_revision = self.proposed_revision
        self._current_resolution = resolution

    @property
    def current_dependency_revisions(self) -> dict[DataVersionId, DataRevisionId]:
        """Selected intermediate records in the committed cut, excluding its root."""
        if self._current_resolution is None:
            return {}
        return {key: value for key, value in self._current_resolution.revisions.items()
                if key != self.current_revision[:2]}

    def invalidate_dependency(self, revision_id: DataRevisionId) -> set[DataRevisionId]:
        """Forget trees expanded through an older count of this exact version.

        Explicit root revisions stay pinned. Duplicate notifications and other
        data versions do not invalidate trees. Return the affected root revisions.
        """
        version_id = revision_id[:2]
        invalidated = {
            root for root, dependencies in self._dependency_revisions.items()
            if version_id in dependencies and dependencies[version_id] < revision_id
        }
        if self._current_resolution is not None:
            selected = self._current_resolution.revisions.get(version_id)
            if selected is not None and version_id != self.current_revision[:2] and selected < revision_id:
                invalidated.add(self.current_revision)
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
        edges = []
        dependencies = {}
        pending = deque([revision_id])
        queued = {revision_id[:2]}
        while pending:
            current = pending.popleft()
            details = cache.get_revision(current)
            required_versions = {current[0]: current[1]}
            for dependency in details.dependencies or ():
                if required_versions.setdefault(dependency[0], dependency[1]) != dependency[1]:
                    raise ValueError(f"Conflicting version for data ID {dependency[0]!r}")
                edges.append((current[:2], dependency))
                if dependency not in queued:
                    dependencies[dependency] = cache.get_latest_revision(dependency)
                    pending.append(dependencies[dependency])
                    queued.add(dependency)

        # Load and validate every reachable record before classifying a
        # cross-record mismatch as temporary. Missing/malformed storage must
        # still fail, even on a path beyond the first mismatching edge.
        try:
            graph = DataEdgeGraph(edges)
        except ValueError as error:
            raise InconsistentDependencyError(revision_id, set(dependencies)) from error
        self._revision_graphs[revision_id] = graph
        self._dependency_revisions[revision_id] = dependencies
        return graph
