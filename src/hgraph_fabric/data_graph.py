from collections.abc import Iterable, Iterator, MutableSet

from .data_structures import DataId, DataVersion, DataVersionId, DependencyEdge


class DataEdgeGraph(MutableSet[DependencyEdge]):
    """A set of directed dependency edges with one version per data ID."""

    _from_index: dict[DataId, set[DataId]]
    _to_index: dict[DataId, set[DataId]]
    _edges: set[DependencyEdge]
    _nodes: dict[DataId, DataVersion]

    def __init__(self, values: Iterable[DependencyEdge] = ()) -> None:
        self._from_index = {}
        self._to_index = {}
        self._edges = set()
        self._nodes = {}
        for value in values:
            self.add(value)

    def __contains__(self, value: object) -> bool:
        try:
            return value in self._edges
        except TypeError:
            return False

    def __iter__(self) -> Iterator[DependencyEdge]:
        return iter(self._edges)

    def __len__(self) -> int:
        return len(self._edges)

    def has_node(self, node: DataVersionId) -> bool:
        data_id, version = node
        return data_id in self._nodes and self._nodes[data_id] == version

    def nodes(self) -> Iterator[DataVersionId]:
        return iter(self._nodes.items())

    def has_data_id(self, data_id: DataId) -> bool:
        return data_id in self._nodes

    def data_ids(self) -> Iterator[DataId]:
        return iter(self._nodes)

    def get_data_version_id(self, data_id: DataId) -> DataVersionId:
        return data_id, self._nodes[data_id]

    def successors(self, data_id: DataId) -> Iterator[DataId]:
        """Return targets of edges from data_id; raise KeyError if it is unknown."""
        return iter(self._from_index[data_id])

    def predecessors(self, data_id: DataId) -> Iterator[DataId]:
        """Return sources of edges to data_id; raise KeyError if it is unknown."""
        return iter(self._to_index[data_id])

    def _check_version(self, value: DataVersionId) -> None:
        data_id, version = value
        if data_id in self._nodes and self._nodes[data_id] != version:
            raise ValueError(f"Conflicting version for data ID {data_id!r}")

    def add(self, edge: DependencyEdge) -> None:
        source, target = edge
        if source[0] == target[0] and source[1] != target[1]:
            raise ValueError(f"Conflicting versions for data ID {source[0]!r}")
        self._check_version(source)
        self._check_version(target)
        self._edges.add(edge)
        for data_id, version in edge:
            self._nodes[data_id] = version
            self._from_index.setdefault(data_id, set())
            self._to_index.setdefault(data_id, set())
        self._from_index[source[0]].add(target[0])
        self._to_index[target[0]].add(source[0])

    def discard(self, edge: DependencyEdge) -> None:
        if edge not in self:
            return
        source, target = edge
        self._edges.remove(edge)
        self._from_index[source[0]].remove(target[0])
        self._to_index[target[0]].remove(source[0])
        for data_id in {source[0], target[0]}:
            if not self._from_index[data_id] and not self._to_index[data_id]:
                del self._from_index[data_id]
                del self._to_index[data_id]
                del self._nodes[data_id]

    def remove(self, edge: DependencyEdge) -> None:
        if edge not in self:
            raise KeyError(edge)
        self.discard(edge)
