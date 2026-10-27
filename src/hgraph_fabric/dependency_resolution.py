"""Choose revision records without changing referenced data versions."""
from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from collections import deque
from collections.abc import Iterator

from .data_cache import DataCache
from .data_graph import DataEdgeGraph
from .data_structures import DataRevisionId, DataVersionId, is_no_data


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
    """Resolve conflict paths using immutable immediate-dependency records.

    Binary bounds are used only on a version whose immediate membership and
    ordering have been checked. Descendant expansions are never used as an
    ordering key: their revision choices are resolved recursively instead.
    """
    def __init__(self, cache: DataCache, *, conflict_paths=None):
        self.cache = cache
        self.conflict_paths = conflict_paths
        self.indexes = {}
        self.records = {}
        self.dependencies: set[DataVersionId] = set()

    def watch_conflict_paths(self, root, conflicts, edges):
        """Retain consumers on rejected paths, including historical choices."""
        if self.conflict_paths is None or not conflicts:
            return
        parents = {}
        for consumer, dependency in edges:
            parents.setdefault(dependency, set()).add(consumer)
        pending = [version for version in parents if version[0] in conflicts]
        visited, consumers = set(pending), set()
        while pending:
            for parent in parents.get(pending.pop(), ()):
                if parent != root[:2]:
                    consumers.add(parent)
                if parent not in visited:
                    visited.add(parent)
                    pending.append(parent)
        if consumers:
            self.conflict_paths.setdefault(root, set()).update(consumers)

    def counts(self, version, maximum, constraints, minimum=0):
        # The latest record is the usual case; do not load its history unless
        # that candidate fails or a caller asks for another solution.
        yield maximum
        if maximum == minimum:
            return
        # Probe revision zero before trying intervening counts. Updates use
        # their committed floor when it is higher than zero.
        self.record((*version, minimum))
        if not constraints:
            yield from range(maximum - 1, minimum - 1, -1)
            return
        key = version, minimum, maximum
        if key not in self.indexes:
            records = [self.record((*version, count)) for count in range(minimum, maximum + 1)]
            names = records[0].keys()
            ordered = all(record.keys() == names for record in records)
            columns = {name: [record[name] for record in records] for name in names} if ordered else {}
            ordered = ordered and all(all(a <= b for a, b in zip(values, values[1:]))
                                      for values in columns.values())
            self.indexes[key] = columns if ordered else {}
        low, high = minimum, maximum + 1
        for name, values in self.indexes[key].items():
            if name in constraints:
                low = max(low, minimum + bisect_left(values, constraints[name]))
                high = min(high, minimum + bisect_right(values, constraints[name]))
        yield from range(min(high - 1, maximum - 1), low - 1, -1)

    def record(self, revision):
        if is_no_data(revision):
            return {}  # A selection boundary, never a metadata lookup.
        if revision in self.records:
            return self.records[revision]
        dependencies = self.cache.get_revision(revision).dependencies or ()
        versions = {revision[0]: revision[1]}
        for name, version in dependencies:
            if versions.setdefault(name, version) != version:
                raise ValueError(f"Conflicting version for data ID {name!r}")
        # A failed candidate can expose a path absent from the latest tree.
        # Keep it available to callers that register notification retries.
        self.dependencies.update(dependencies)
        self.records[revision] = dict(dependencies)
        return self.records[revision]

    def solutions(self, root, *, constraints=None, ceilings=None) -> Iterator[DependencyResolution]:
        """Follow conflict paths first; retain exhaustive alternatives as fallback.

        A path match avoids enumerating histories on unrelated branches. The
        fallback still covers changing membership, joint choices on both trees,
        and exact alignments after a path search finds an omission.
        """
        if is_no_data(root):
            if (constraints or {}).get(root[0], 0) == 0:
                yield DependencyResolution(root, DataEdgeGraph(), {})
            return
        first = self._path_solution(root, constraints or {}, ceilings or {})
        if first is not None:
            yield first
        for candidate in self._enumerate_solutions(root, constraints=constraints, ceilings=ceilings):
            if first is None or candidate.signature != first.signature:
                yield candidate

    def _expand(self, root, choices, ceilings):
        """Keep all parent links, including paths that currently disagree."""
        pending = deque([root[:2]])
        revisions, versions, parents, edges = {}, {}, {}, []
        while pending:
            version = pending.popleft()
            if version in revisions:
                continue
            if version == root[:2]:
                revision = root
            else:
                head = choices.get(version) or self.cache.get_latest_revision(version)
                revision = (*version, min(head[2], ceilings.get(version, head[2])))
            revisions[version] = revision
            versions.setdefault(version[0], set()).add(version[1])
            for dependency in self.record(revision).items():
                parents.setdefault(dependency, set()).add(version)
                edges.append((version, dependency))
                pending.append(dependency)
        return revisions, versions, parents, edges

    def _path_solution(self, root, constraints, ceilings):
        # Each frame holds choices to try. Re-expansion after a parent change
        # follows its newly selected path forward before checking another bound.
        stack = [iter([{}])]
        seen = set()
        while stack:
            try:
                choices = next(stack[-1])
            except StopIteration:
                stack.pop()
                continue
            revisions, versions, parents, edges = self._expand(root, choices, ceilings)
            signature = tuple(sorted(revisions.items()))
            if signature in seen:
                continue
            seen.add(signature)
            conflict = next((name for name in sorted(versions)
                             if len(versions[name]) > 1
                             or (name in constraints and versions[name] != {constraints[name]})), None)
            if conflict is None:
                return DependencyResolution(root, DataEdgeGraph(edges), revisions)
            self.watch_conflict_paths(root, {conflict}, edges)
            target = constraints.get(conflict, min(versions[conflict]))
            wrong = [(conflict, value) for value in sorted(versions[conflict], reverse=True)
                     if value != target]

            # Walk backwards from the conflict to every immediate consumer,
            # then to their parents. A DAG may have several paths to repair.
            pending = deque(wrong)
            visited, consumers = set(wrong), []
            while pending:
                version = pending.popleft()
                for parent in sorted(parents.get(version, ())):
                    if parent in visited:
                        continue
                    visited.add(parent)
                    pending.append(parent)
                    if parent != root[:2]:  # The caller owns root-history search.
                        consumers.append(parent)
            stack.append(self._path_alternatives(consumers, revisions, constraints | {conflict: target}))
        return None

    def _path_alternatives(self, consumers, revisions, constraints):
        for parent in consumers:
            maximum = revisions[parent][2]
            # counts probes zero and bounds matching immediate inputs. If even
            # zero is too high, its verified interval is empty: step back to
            # the next parent rather than searching an unrelated branch.
            for count in self.counts(parent, maximum, constraints):
                if count != maximum:
                    yield revisions | {parent: (*parent, count)}

    def _enumerate_solutions(self, root, *, constraints=None, ceilings=None) -> Iterator[DependencyResolution]:
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
                conflicts = set()
                for name, value in ((version[0], version[1]), *inputs.items()):
                    if chosen.setdefault(name, value) != value or constraints.get(name, value) != value:
                        valid = False
                        conflicts.add(name)
                    if name in inputs:
                        additions.append(((name, value), None))
                if not valid:
                    self.watch_conflict_paths(
                        root, conflicts, edges + [(version, item) for item in inputs.items()])
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
