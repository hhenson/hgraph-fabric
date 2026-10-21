"""Small deterministic fixtures and an exhaustive oracle independent of production graphs.

The oracle expands raw records and scans candidate cuts; it never calls the
production graph builder, pair resolver, or stabilization helpers. Root
containment follows the agreed rule: only the contained root version matters.
"""
from itertools import combinations, product

from hgraph_fabric.data_cache import DataCache
from hgraph_fabric.data_structures import DataUpdateDetails


class History:
    def __init__(self):
        self.records = {}
        self.heads = {}

    def add(self, data_id, version, count=0, dependencies=(), weak=()):
        previous = self.heads.get(data_id)
        update = DataUpdateDetails(
            id=data_id, revision=(version, count), dependencies=tuple(dependencies) or None,
            weak_dependencies=tuple(weak) or None,
            previous_revision=previous[1:] if previous and count == 0 else None,
        )
        key = (data_id, version, count)
        self.records[key] = update
        self.heads[data_id] = key
        return update

    def revisions(self, data_id, ceiling=None):
        return sorted(r for r in self.records if r[0] == data_id and (ceiling is None or r <= ceiling))

    def nodes(self, revision):
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
                    stack.append(max(r for r in self.records if r[:2] == dependency))
        return nodes

    def consistent(self, cut):
        expanded = {key: self.nodes(revision) for key, revision in cut.items()}
        for left, right in combinations(cut, 2):
            a, b = expanded[left], expanded[right]
            if left in b:
                if cut[left][1] != b[left]:
                    return False
            elif right in a:
                if cut[right][1] != a[right]:
                    return False
            elif any(a[node] != b[node] for node in a.keys() & b.keys()):
                return False
        return True

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

    def load_data(self, version):
        raise AssertionError("Metadata search must not load dataframe payloads")


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
