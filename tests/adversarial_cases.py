"""Small deterministic fixtures and an exhaustive oracle independent of production graphs.

The oracle expands raw records and scans candidate cuts; it never calls the
production graph builder, pair resolver, or stabilization helpers. Selected intermediate revisions are checked against immutable raw records.
"""
from datetime import UTC, datetime
from itertools import combinations, product

from hgraph_fabric.data_cache import DataCache
from hgraph_fabric.data_structures import DataUpdateDetails


AS_OF = datetime(2026, 10, 21, tzinfo=UTC)


class History:
    def __init__(self):
        self.records = {}
        self.heads = {}

    def add(self, data_id, version, count=0, dependencies=(), weak=()):
        previous = self.heads.get(data_id)
        update = DataUpdateDetails(
            id=data_id, revision=(version, count), dependencies=tuple(dependencies) or None,
            weak_dependencies=tuple(weak) or None,
            previous_revision=previous[1:] if previous and count == 0 else None, as_of=AS_OF,
        )
        key = (data_id, version, count)
        self.records[key] = update
        self.heads[data_id] = key
        return update

    def revisions(self, data_id, ceiling=None):
        return sorted(r for r in self.records if r[0] == data_id and (ceiling is None or r <= ceiling))

    def nodes(self, revision, choices=None):
        """Expand raw records with version-scoped latest counts, including leaves."""
        nodes = {}
        seen = set()
        stack = [revision]
        while stack:
            item = stack.pop()
            data_id, version, _ = item
            if data_id in nodes and nodes[data_id] != version:
                raise ValueError("Internally conflicting tree")
            nodes[data_id] = version
            if item[:2] in seen:
                continue
            seen.add(item[:2])
            for dependency in self.records[item].dependencies or ():
                dep_id, dep_version = dependency
                if dep_id in nodes and nodes[dep_id] != dep_version:
                    raise ValueError("Internally conflicting tree")
                nodes[dep_id] = dep_version
                if dependency not in seen:
                    selected = (choices or {}).get(dependency)
                    stack.append(selected or max(r for r in self.records if r[:2] == dependency))
        return nodes

    def resolutions(self, revision):
        """Exhaustively choose intermediate records from raw stored history."""
        def visit(todo, selected, nodes):
            if not todo:
                yield nodes
                return
            version, root = todo[0]
            if version in selected:
                yield from visit(todo[1:], selected, nodes)
                return
            revisions = [root] if root is not None else sorted(
                (r for r in self.records if r[:2] == version), reverse=True)
            for candidate in revisions:
                expanded = nodes.copy()
                dependencies = self.records[candidate].dependencies or ()
                for name, value in (version, *dependencies):
                    if name in expanded and expanded[name] != value:
                        break
                    expanded[name] = value
                else:
                    yield from visit(todo[1:] + [(dep, None) for dep in dependencies],
                                     selected | {version}, expanded)
        yield from visit([(revision[:2], revision)], set(), {})

    @staticmethod
    def compatible(left, a, right, b):
        return all(a[node] == b[node] for node in a.keys() & b.keys())

    def consistent(self, cut, choices=None):
        if choices is not None:
            expanded = {key: self.nodes(revision, choices[key]) for key, revision in cut.items()}
            return all(self.compatible(left, expanded[left], right, expanded[right])
                       for left, right in combinations(cut, 2))
        # A root-only cut can admit several expansions. Check existence without
        # using the production resolver or assuming latest intermediate counts.
        keys = tuple(cut)
        def extend(index, selected):
            if index == len(keys):
                return True
            key = keys[index]
            for nodes in self.resolutions(cut[key]):
                if all(self.compatible(other, graph, key, nodes) for other, graph in selected.items()):
                    if extend(index + 1, selected | {key: nodes}):
                        return True
            return False
        return extend(0, {})

    def cuts(self, ceilings):
        keys = tuple(ceilings)
        for revisions in product(*(self.revisions(key, ceilings[key]) for key in keys)):
            cut = dict(zip(keys, revisions))
            if self.consistent(cut):
                yield cut


class HistoryCache(DataCache):
    def __init__(self, history):
        super().__init__()
        self.history = history
        self.loads = []

    def load_latest(self, data_id):
        self.loads.append(("latest", data_id))
        return self.history.heads[data_id]

    def load_revision(self, revision):
        self.loads.append(("revision", revision))
        return self.history.records[revision]

    def load_version(self, version):
        raise AssertionError("Metadata search must not load dataframe payloads")

    def write_version(self, version_id, data):
        raise AssertionError("Metadata search must not write data")

    def write_revision(self, revision_id, details):
        raise AssertionError("Metadata search must not write revisions")

    def write_latest(self, data_id, details):
        raise AssertionError("Metadata search must not write latest")


def current_cut(manager):
    return {key: sub.current_revision for key, sub in manager._subscriptions.items()}


def dag_edges(mask):
    """Every labeled DAG on four nodes in a fixed topological ordering (64)."""
    possible = tuple(combinations(range(4), 2))
    return tuple(edge for bit, edge in enumerate(possible) if mask & (1 << bit))


def dag_dependencies(edges, node, version):
    return tuple((f"n{target}", version) for source, target in edges if source == node)


def dag_history(mask):
    history = History()
    edges = dag_edges(mask)
    for version in (100, 9000):
        for node in reversed(range(4)):
            history.add(f"n{node}", version, dependencies=dag_dependencies(edges, node, version))
    return history


def current_choices(manager):
    return {key: sub.current_dependency_revisions for key, sub in manager._subscriptions.items()}
