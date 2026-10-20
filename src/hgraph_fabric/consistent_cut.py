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

    Start from each subscription's current_revision. The caller initializes
    current to latest before a tournament and applies returned pairs between
    comparisons; this function never resets a selection to latest.

    If one subscription's root occurs in the other tree, only that root's
    version needs to match: its descendants are covered by that dependency.
    Resolve a newer root directly to the consumed version. If it was downgraded
    in an earlier comparison, keep it fixed and search the consuming tree.

    Otherwise, the first mismatching common node chooses the leading
    subscription, and the other stays fixed. Prefer matching all original
    common nodes to omitting any; also check newly shared nodes. While a node
    remains a dependency, both subscriptions record its changes and its
    versions advance monotonically. Raise LookupError if no alignment exists.

    Trees loaded during the search are cached normally. Neither subscription's
    latest_revision nor current_revision is changed; the caller can apply the
    returned revisions after reviewing or combining the result.
    """
    if left.current_revision is None or right.current_revision is None:
        raise LookupError("Both subscriptions need a current revision")

    left_revision, right_revision = left.current_revision, right.current_revision
    left_graph, right_graph = left.current(cache), right.current(cache)

    # Containment is stronger than ordinary overlap. Matching the contained
    # root's version covers its entire subtree, so do not compare descendants.
    # This includes leaves, whose roots are absent from their own edge graphs.
    if left_revision[0] == right_revision[0]:
        contained_is_left = left_revision[1] >= right_revision[1]
    elif right_graph.has_data_id(left_revision[0]):
        contained_is_left = True
    elif left_graph.has_data_id(right_revision[0]):
        contained_is_left = False
    else:
        contained_is_left = None

    if contained_is_left is not None:
        contained = left_revision if contained_is_left else right_revision
        other = right_revision if contained_is_left else left_revision
        other_graph = right_graph if contained_is_left else left_graph
        target = (
            other[1] if contained[0] == other[0]
            else other_graph.get_data_version_id(contained[0])[1]
        )
        if contained[1] == target:
            return left_revision, right_revision
        if contained[1] > target:
            # The other tree already identifies the required root version.
            # Resolve its revision directly; there are no count bounds to bisect.
            adjusted = cache.get_latest_revision((contained[0], target))
            return (adjusted, right_revision) if contained_is_left else (left_revision, adjusted)
        # An earlier tournament comparison may have downgraded this root.
        # Hold it fixed and search only for its version in the consuming tree.
        common = {contained[0]}
        mismatch = contained[0]
        fixed_nodes = {contained[0]: contained[1]}
        leading_is_left = not contained_is_left
        leading = left if leading_is_left else right
        start = other
    else:
        if not left_graph or not right_graph:
            return left_revision, right_revision  # Unrelated leaf: nothing to align.
        left_nodes = _nodes(left_graph, left_revision)
        right_nodes = _nodes(right_graph, right_revision)

        # 1. Only shared IDs impose agreement. A stable order makes the choice
        #    of the first mismatch reproducible; unrelated nodes need no alignment.
        common = left_nodes.keys() & right_nodes.keys()
        mismatch = _first_mismatch(left_nodes, right_nodes)
        if mismatch is None:
            return left_revision, right_revision

        # 2. Fix the lagging graph as the target. Crossing below its version
        #    is a lower bound, not a reason to move that fixed subscription.
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

        # Check the target set at every probe: the contained root alone, or all
        # shared nodes for ordinary overlap. A lag moves forward; a lead moves back.
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

    # 3. The selected current count is the initial upper bound, even if newer
    #    revisions exist. Establish bounds using that count and revision zero.
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
    #    first, still checking the remaining nodes in the target set. Disappearance is
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
