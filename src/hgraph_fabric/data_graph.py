from collections.abc import Iterable, Iterator, MutableSet

from .data_structures import DataId, DataVersion, DataVersionId, DependencyEdge


class DataEdgeGraph(MutableSet[DataVersionId]):
    """A set of versioned vertices with directed edges and one version per data ID."""

    _from_index: dict[DataId, set[DataId]]
    _to_index: dict[DataId, set[DataId]]
    _edges: dict[DataId, DataVersion]

    def __init__(self, values: Iterable[DataVersionId] = ()) -> None:
        self._from_index = {}
        self._to_index = {}
        self._edges = {}
        for value in values:
            self.add(value)

    def __contains__(self, value: object) -> bool:
        if not isinstance(value, tuple) or len(value) != 2:
            return False
        data_id, version = value
        return (
            isinstance(data_id, str)
            and data_id in self._edges
            and self._edges[data_id] == version
        )

    def __iter__(self) -> Iterator[DataVersionId]:
        return iter(self._edges.items())

    def __len__(self) -> int:
        return len(self._edges)

    def _check_version(self, value: DataVersionId) -> None:
        data_id, version = value
        if data_id in self._edges and self._edges[data_id] != version:
            raise ValueError(f"Conflicting version for data ID {data_id!r}")

    def add(self, value: DataVersionId) -> None:
        self._check_version(value)
        data_id, version = value
        self._edges[data_id] = version
        self._from_index.setdefault(data_id, set())
        self._to_index.setdefault(data_id, set())

    def discard(self, value: DataVersionId) -> None:
        if value not in self:
            return
        data_id, _ = value
        outgoing = self._from_index.pop(data_id)
        incoming = self._to_index.pop(data_id)
        for target in outgoing - {data_id}:
            self._to_index[target].discard(data_id)
        for source in incoming - {data_id}:
            self._from_index[source].discard(data_id)
        del self._edges[data_id]

    def remove(self, value: DataVersionId) -> None:
        if value not in self:
            raise KeyError(value)
        self.discard(value)

    def add_edge(self, edge: DependencyEdge) -> None:
        """Add a source-to-target edge, adding missing endpoints."""
        source, target = edge
        if source[0] == target[0] and source[1] != target[1]:
            raise ValueError(f"Conflicting versions for data ID {source[0]!r}")
        self._check_version(source)
        self._check_version(target)
        self.add(source)
        self.add(target)
        self._from_index[source[0]].add(target[0])
        self._to_index[target[0]].add(source[0])

    def remove_edge(self, edge: DependencyEdge) -> None:
        """Remove an existing directed edge, retaining its endpoints."""
        source, target = edge
        if (
            source not in self
            or target not in self
            or target[0] not in self._from_index[source[0]]
        ):
            raise KeyError(edge)
        self._from_index[source[0]].remove(target[0])
        self._to_index[target[0]].remove(source[0])
