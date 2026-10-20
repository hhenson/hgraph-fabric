from collections.abc import Iterable, Iterator, MutableSet

from .data_structures import DataId, DataVersion, DataVersionId, DependencyEdge


class DataEdgeGraph(MutableSet[DependencyEdge]):
    """A set of directed dependency edges with one version per data ID."""

    _from_index: dict[DataId, set[DataId]]
    _to_index: dict[DataId, set[DataId]]
    _edges: dict[DataId, DataVersion]

    def __init__(self, values: Iterable[DependencyEdge] = ()) -> None:
        self._from_index = {}
        self._to_index = {}
        self._edges = {}
        for value in values:
            self.add(value)

    def __contains__(self, value: object) -> bool:
        if not isinstance(value, tuple) or len(value) != 2:
            return False
        source, target = value
        return (
            self._has_version(source)
            and self._has_version(target)
            and target[0] in self._from_index[source[0]]
        )

    def _has_version(self, value: object) -> bool:
        if not isinstance(value, tuple) or len(value) != 2:
            return False
        data_id, version = value
        return (
            isinstance(data_id, str)
            and data_id in self._edges
            and self._edges[data_id] == version
        )

    def __iter__(self) -> Iterator[DependencyEdge]:
        for source, targets in self._from_index.items():
            for target in targets:
                yield ((source, self._edges[source]), (target, self._edges[target]))

    def __len__(self) -> int:
        return sum(len(targets) for targets in self._from_index.values())

    def _check_version(self, value: DataVersionId) -> None:
        data_id, version = value
        if data_id in self._edges and self._edges[data_id] != version:
            raise ValueError(f"Conflicting version for data ID {data_id!r}")

    def add(self, edge: DependencyEdge) -> None:
        source, target = edge
        if source[0] == target[0] and source[1] != target[1]:
            raise ValueError(f"Conflicting versions for data ID {source[0]!r}")
        self._check_version(source)
        self._check_version(target)
        for data_id, version in edge:
            self._edges[data_id] = version
            self._from_index.setdefault(data_id, set())
            self._to_index.setdefault(data_id, set())
        self._from_index[source[0]].add(target[0])
        self._to_index[target[0]].add(source[0])

    def discard(self, edge: DependencyEdge) -> None:
        if edge not in self:
            return
        source, target = edge
        self._from_index[source[0]].remove(target[0])
        self._to_index[target[0]].remove(source[0])
        for data_id in {source[0], target[0]}:
            if not self._from_index[data_id] and not self._to_index[data_id]:
                del self._from_index[data_id]
                del self._to_index[data_id]
                del self._edges[data_id]

    def remove(self, edge: DependencyEdge) -> None:
        if edge not in self:
            raise KeyError(edge)
        self.discard(edge)

    def add_edge(self, edge: DependencyEdge) -> None:
        self.add(edge)

    def remove_edge(self, edge: DependencyEdge) -> None:
        self.remove(edge)
