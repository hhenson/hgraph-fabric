"""Choose revision records without changing referenced data versions."""
from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from collections.abc import Iterator

from .data_cache import DataCache
from .data_graph import DataEdgeGraph
from .data_structures import DataRevisionId, DataVersionId


@dataclass(frozen=True)
class DependencyResolution:
    root: DataRevisionId
    graph: DataEdgeGraph
    revisions: dict[DataVersionId, DataRevisionId]

    @property
    def nodes(self):
        return dict(self.graph.nodes()) | {self.root[0]: self.root[1]}

    @property
    def signature(self):
        return self.root, tuple(sorted(self.revisions.items()))


class RevisionSearch:
    """A search-local index of immutable immediate-dependency records.

    Binary bounds are used only on a version whose immediate membership and
    ordering have been checked. Descendant expansions are never used as an
    ordering key: their revision choices are resolved recursively instead.
    """
    def __init__(self, cache: DataCache):
        self.cache = cache
        self.indexes = {}

    def counts(self, version, maximum, constraints):
        # The latest record is the usual case; do not load its history unless
        # that candidate fails or a caller asks for another solution.
        yield maximum
        if maximum == 0:
            return
        if not constraints:
            yield from range(maximum - 1, -1, -1)
            return
        key = version, maximum
        if key not in self.indexes:
            records = [self.record((*version, count)) for count in range(maximum + 1)]
            names = records[0].keys()
            ordered = all(record.keys() == names for record in records)
            columns = {name: [record[name] for record in records] for name in names} if ordered else {}
            ordered = ordered and all(all(a <= b for a, b in zip(values, values[1:]))
                                      for values in columns.values())
            self.indexes[key] = columns if ordered else {}
        low, high = 0, maximum + 1
        for name, values in self.indexes[key].items():
            if name in constraints:
                low = max(low, bisect_left(values, constraints[name]))
                high = min(high, bisect_right(values, constraints[name]))
        yield from range(min(high - 1, maximum - 1), low - 1, -1)

    def record(self, revision):
        dependencies = self.cache.get_revision(revision).dependencies or ()
        versions = {revision[0]: revision[1]}
        for name, version in dependencies:
            if versions.setdefault(name, version) != version:
                raise ValueError(f"Conflicting version for data ID {name!r}")
        return dict(dependencies)

    def solutions(self, root, *, constraints=None, ceilings=None) -> Iterator[DependencyResolution]:
        constraints = constraints or {}
        ceilings = ceilings or {}

        def next_states(pending, nodes, revisions, edges):
            version, fixed = pending[0]
            pending = pending[1:]
            if version in revisions:
                yield pending, nodes, revisions, edges
                return
            head = fixed or self.cache.get_latest_revision(version)
            maximum = min(head[2], ceilings.get(version, head[2])) if fixed is None else head[2]
            counts = (head[2],) if fixed is not None else self.counts(version, maximum, constraints | nodes)
            for count in counts:
                revision = (*version, count)
                inputs = self.record(revision)
                chosen = nodes.copy()
                additions = []
                valid = True
                for name, value in ((version[0], version[1]), *inputs.items()):
                    if chosen.setdefault(name, value) != value or constraints.get(name, value) != value:
                        valid = False
                    if name in inputs:
                        additions.append(((name, value), None))
                if not valid:
                    continue
                yield (pending + additions, chosen, revisions | {version: revision},
                       edges + [(version, (name, value)) for name, value in inputs.items()])

        # Explicit backtracking frames keep deep dependency chains independent
        # of Python's call-stack limit. Each iterator probes alternatives lazily.
        stack = [iter([([(root[:2], root)], {}, {}, [])])]
        while stack:
            try:
                pending, nodes, revisions, edges = next(stack[-1])
            except StopIteration:
                stack.pop()
                continue
            if pending:
                stack.append(next_states(pending, nodes, revisions, edges))
            else:
                yield DependencyResolution(root, DataEdgeGraph(edges), revisions)
