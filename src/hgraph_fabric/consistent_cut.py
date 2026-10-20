from collections.abc import Iterator

from .data_cache import DataCache
from .data_graph import DataEdgeGraph
from .data_structures import DataId, DataRevisionId, DataVersion
from .data_subscription_graph import DataSubscriptionGraph


def find_consistent_cut(
    left: DataSubscriptionGraph,
    right: DataSubscriptionGraph,
    cache: DataCache,
) -> tuple[DataRevisionId, DataRevisionId]:
    """Return consistent (left revision, right revision), in argument order.

    The first mismatching common node chooses the leading subscription.
    The other subscription stays fixed throughout the search. While a node
    remains a dependency, its changes are recorded by both subscriptions;
    present dependency versions advance monotonically.

    Prefer a revision matching all the original common nodes. Only if none
    exists may a revision omitting some of those nodes be used. Every common
    node at the proposed revision must still agree, including newly shared
    nodes. Raise LookupError when neither form of alignment is available.

    Trees loaded during the search are cached normally. Neither subscription's
    latest_revision nor current_revision is changed; the caller can apply the
    returned revisions after reviewing or combining the result.
    """
    if left.latest_revision is None or right.latest_revision is None:
        raise LookupError("Both subscriptions need a latest revision")

    left_revision, right_revision = left.latest_revision, right.latest_revision
    left_graph, right_graph = left.latest(cache), right.latest(cache)
    left_nodes = _nodes(left_graph, left_revision)
    right_nodes = _nodes(right_graph, right_revision)

    # 1. Only shared IDs impose agreement. A stable order makes the choice of
    #    the first mismatch reproducible; unrelated nodes need no alignment.
    common = left_nodes.keys() & right_nodes.keys()
    mismatch = _first_mismatch(left_nodes, right_nodes)
    if mismatch is None:
        return left_revision, right_revision

    # 2. Fix the lagging graph as the target. Crossing below its version during
    #    a probe is a lower bound, not a reason to move that fixed subscription.
    if left_nodes[mismatch] > right_nodes[mismatch]:
        leading, start = left, left_revision
        fixed_nodes = right_nodes
        leading_is_left = True
    else:
        leading, start = right, right_revision
        fixed_nodes = left_nodes
        leading_is_left = False
    target = fixed_nodes[mismatch]

    def result(revision: DataRevisionId) -> tuple[DataRevisionId, DataRevisionId]:
        return (revision, right_revision) if leading_is_left else (left_revision, revision)

    def nodes_at(version: DataVersion, count: int) -> dict[DataId, DataVersion]:
        revision = (leading.data_id, version, count)
        return _nodes(leading.revision(revision, cache), revision)

    def exact_revision(
        version: DataVersion, low: int, high: int
    ) -> DataRevisionId | None:
        """Bisect revision COUNTS in this one version, with inclusive bounds."""
        if low > high:
            return None
        middle = (low + high) // 2
        nodes = nodes_at(version, middle)

        # Check ALL shared nodes at every probe, not just the original mismatch.
        # If another required node lags, move forward; if it leads, move back.
        conflict = _first_mismatch(nodes, fixed_nodes)
        if conflict in common:
            if nodes[conflict] > fixed_nodes[conflict]:
                return exact_revision(version, low, middle - 1)
            return exact_revision(version, middle + 1, high)

        # Absence cannot tell us which half contains an exact match: a node
        # may disappear and reappear. A newly shared conflicting node may also
        # disappear, so it cannot safely exclude either half. Search newer first.
        newer = exact_revision(version, middle + 1, high)
        if newer is not None:
            return newer
        if conflict is None and common <= nodes.keys():
            return leading.data_id, version, middle
        return exact_revision(version, low, middle - 1)

    # 3. Establish bounds using each version's maximum count and revision zero.
    #    If zero still leads, walk its previous_revision pointer. That pointer
    #    supplies BOTH the actual previous version and its maximum count.
    #    Never decrement a version number or bisect across different versions.
    for version, maximum_count in _version_bounds(start, cache):
        at_maximum = nodes_at(version, maximum_count).get(mismatch)
        at_zero = nodes_at(version, 0).get(mismatch)
        if at_maximum is not None and at_maximum < target:
            break  # All earlier present values also lag: no exact match there.
        if at_zero is not None and at_zero > target:
            continue  # All present values in this version still lead.

        # Zero lags, matches, or omits the node. The count range [0, maximum]
        # is now bounded within ONE version; midpoint probes may move either way.
        revision = exact_revision(version, 0, maximum_count)
        if revision is not None:
            return result(revision)
        if at_zero is not None and at_zero < target:
            break

    # 4. No exact alignment exists. Try missing-dependency candidates, newest
    #    first, still checking every node that remains shared. Disappearance is
    #    not monotonic, so this uncommon fallback cannot safely use binary search.
    for version, maximum_count in _version_bounds(start, cache):
        for count in range(maximum_count, -1, -1):
            nodes = nodes_at(version, count)
            if common <= nodes.keys():
                continue
            if _first_mismatch(nodes, fixed_nodes) is None:
                revision = (leading.data_id, version, count)
                return result(revision)
    raise LookupError("No revision aligns with the fixed subscription")


def _nodes(graph: DataEdgeGraph, revision: DataRevisionId) -> dict[DataId, DataVersion]:
    # A leaf root has no edges, but its version must still agree if the other
    # tree consumes it.
    return {**dict(graph.nodes()), revision[0]: revision[1]}


def _first_mismatch(
    left: dict[DataId, DataVersion], right: dict[DataId, DataVersion]
) -> DataId | None:
    return next(
        (data_id for data_id in sorted(left.keys() & right.keys())
         if left[data_id] != right[data_id]),
        None,
    )


def _version_bounds(
    revision: DataRevisionId, cache: DataCache
) -> Iterator[tuple[DataVersion, int]]:
    data_id, version, maximum_count = revision
    while True:
        yield version, maximum_count
        previous = cache.get_revision((data_id, version, 0)).previous_revision
        if previous is None:
            return
        if previous[0] >= version:
            raise ValueError("Revision-zero history must point to an older version")
        version, maximum_count = previous
