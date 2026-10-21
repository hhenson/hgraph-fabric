from collections.abc import Iterator

from .data_cache import DataCache
from .data_graph import DataEdgeGraph
from .data_structures import DataId, DataRevisionId, DataVersion
from .data_subscription_graph import DataSubscriptionGraph


class NoConsistentCutError(LookupError):
    """Available histories contain no compatible pair within the given bounds."""


def find_consistent_cut(
    left: DataSubscriptionGraph,
    right: DataSubscriptionGraph,
    cache: DataCache,
    *,
    lower_bounds: tuple[DataRevisionId | None, DataRevisionId | None] = (None, None),
) -> tuple[DataRevisionId, DataRevisionId]:
    """Return consistent (left revision, right revision), in argument order.

    Start from each subscription's proposed_revision. The caller initializes
    proposed to latest before a search and applies returned pairs between
    comparisons; this function never resets a selection to latest.

    If one subscription's root occurs in the other tree, only that root's
    version needs to match: its descendants are covered by that dependency.
    Resolve a newer root directly to the consumed version. If it was downgraded
    in an earlier comparison, keep it fixed and search the consuming tree.

    Otherwise, the first mismatching common node chooses the leading
    subscription, and the other stays fixed. Prefer matching all original
    common nodes to omitting any; also check newly shared nodes. While a node
    remains a dependency, both subscriptions record its changes and its
    versions advance monotonically. If holding one side fixed cannot find an
    alignment, search both bounded histories as an exceptional fallback. Raise
    LookupError if neither search finds a compatible pair.

    Trees loaded during the search are cached normally. No latest, proposed or
    current selection is changed; the caller can apply the
    returned revisions after reviewing or combining the result.

    Updates supply committed currents as inclusive lower bounds. An exact
    match below current is not a candidate: prefer an omission at current to
    rolling back already delivered data. Initialization leaves these unbounded.
    """
    if left.proposed_revision is None or right.proposed_revision is None:
        raise LookupError("Both subscriptions need a proposed revision")

    left_revision, right_revision = left.proposed_revision, right.proposed_revision
    for revision, floor in zip((left_revision, right_revision), lower_bounds):
        if floor is not None:
            if floor[0] != revision[0]:
                raise ValueError("Lower bound belongs to a different data ID")
            if revision < floor:
                raise NoConsistentCutError("Proposed revision is below its lower bound")
    left_graph, right_graph = left.proposed_graph(cache), right.proposed_graph(cache)

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
            floor = lower_bounds[0 if contained_is_left else 1]
            if floor is not None and target < floor[1]:
                # The consuming side may have an earlier compatible revision
                # (including an omission). Do not cross the contained current.
                return _search_both_histories(
                    left, right, left_revision, right_revision, {contained[0]}, cache,
                    *lower_bounds,
                )
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
    leading_floor = lower_bounds[0 if leading_is_left else 1]
    fixed_floor = lower_bounds[1 if leading_is_left else 0]

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

    # 3. The selected proposed count is the initial upper bound, even if newer
    #    revisions exist. The lower count is zero, or the committed count when
    #    this is the floor version. Walk revision zero to earlier versions only
    #    while still above that floor. Its previous_revision pointer
    #    supplies BOTH the actual previous version and its maximum count.
    #    Never decrement a version number or bisect across different versions.
    for version, maximum_count in _version_bounds(start, cache, leading_floor):
        minimum_count = (
            leading_floor[2] if leading_floor is not None and version == leading_floor[1] else 0
        )
        at_maximum = nodes_at(version, maximum_count).get(mismatch)
        at_minimum = nodes_at(version, minimum_count).get(mismatch)
        if at_maximum is not None and at_maximum < target:
            break  # All earlier present values also lag: no exact match there.
        if at_minimum is not None and at_minimum > target:
            continue  # All present values in this version still lead.

        # The oldest admissible count lags, matches, or omits the node. Bisect
        # within ONE version, stopping at current's count in the floor version.
        revision = exact_revision(version, minimum_count, maximum_count)
        if revision is not None:
            return result(revision)
        if at_minimum is not None and at_minimum < target:
            break

    # 4. No exact alignment exists. Try missing-dependency candidates, newest
    #    first, still checking the remaining nodes in the target set. Disappearance is
    #    not monotonic, so this uncommon fallback cannot safely use binary search.
    for revision in _revisions_backwards(start, cache, leading_floor):
        nodes = nodes_at(revision[1], revision[2])
        if common <= nodes.keys():
            continue
        if _first_mismatch(nodes, fixed_nodes) is None:
            return result(revision)
    # Independent inputs may have been consumed in different orders, leaving no
    # revision that matches the other side's complete fixed dependency vector.
    # Only after the normal search fails do we allow both sides to move. Retain
    # argument order and the original proposed upper bounds throughout.
    fixed = right if leading_is_left else left
    fixed_revision = right_revision if leading_is_left else left_revision
    joint = _search_both_histories(
        leading, fixed, start, fixed_revision, common, cache, leading_floor, fixed_floor
    )
    return joint if leading_is_left else joint[::-1]


def _search_both_histories(
    leading: DataSubscriptionGraph,
    fixed: DataSubscriptionGraph,
    leading_start: DataRevisionId,
    fixed_start: DataRevisionId,
    common: set[DataId],
    cache: DataCache,
    leading_floor: DataRevisionId | None = None,
    fixed_floor: DataRevisionId | None = None,
) -> tuple[DataRevisionId, DataRevisionId]:
    """Rare exhaustive fallback; prefer retaining the formerly fixed side.

    Visit its history newest first, searching the leading history newest first
    at each step. Prefer preserving the original common dependencies to omitting
    them. This costs O(L * R) tree comparisons in the worst case, but does not
    affect the normal bounded/binary search. Metadata failures propagate.
    """
    absent = None
    for fixed_revision in _revisions_backwards(fixed_start, cache, fixed_floor):
        fixed_nodes = _nodes(fixed.revision(fixed_revision, cache), fixed_revision)
        for leading_revision in _revisions_backwards(leading_start, cache, leading_floor):
            leading_nodes = _nodes(leading.revision(leading_revision, cache), leading_revision)
            # Historical membership can differ from the initial pair. Apply
            # the same root-containment rule to each candidate pair afresh.
            if leading.data_id in fixed_nodes:
                matches = leading_revision[1] == fixed_nodes[leading.data_id]
            elif fixed.data_id in leading_nodes:
                matches = fixed_revision[1] == leading_nodes[fixed.data_id]
            else:
                matches = _first_mismatch(leading_nodes, fixed_nodes) is None
            if not matches:
                continue
            pair = leading_revision, fixed_revision
            if common <= leading_nodes.keys() and common <= fixed_nodes.keys():
                return pair
            if absent is None:
                absent = pair
    if absent is not None:
        return absent
    raise NoConsistentCutError("No compatible pair exists within the proposed histories")


def _revisions_backwards(
    revision: DataRevisionId, cache: DataCache, floor: DataRevisionId | None = None
) -> Iterator[DataRevisionId]:
    # Counts are consecutive within a version. Version IDs are sparse and must
    # be reached through revision-zero links, including the prior maximum count.
    for version, maximum_count in _version_bounds(revision, cache, floor):
        minimum_count = floor[2] if floor is not None and version == floor[1] else 0
        for count in range(maximum_count, minimum_count - 1, -1):
            yield revision[0], version, count


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
    revision: DataRevisionId, cache: DataCache, floor: DataRevisionId | None = None
) -> Iterator[tuple[DataVersion, int]]:
    data_id, version, maximum_count = revision
    while True:
        if floor is not None and version < floor[1]:
            return
        yield version, maximum_count
        if floor is not None and version == floor[1]:
            return  # No need to load history preceding the committed version.
        previous = cache.get_revision((data_id, version, 0)).previous_revision
        if previous is None:
            return
        if previous[0] >= version:
            raise ValueError("Revision-zero history must point to an older version")
        version, maximum_count = previous
