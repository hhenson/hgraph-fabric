from collections.abc import Iterator
from enum import Enum, auto

from .data_cache import DataCache
from .data_graph import DataEdgeGraph
from .data_structures import DataId, DataRevisionId, DataVersion
from .data_subscription_graph import DataSubscriptionGraph


class NoConsistentCutError(LookupError):
    """Available histories contain no compatible pair within the given bounds."""


class _Comparison(Enum):
    CONFLICT = auto()
    EXACT = auto()
    OMISSION = auto()


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
    common nodes to omitting any; also check newly shared nodes. Search newest
    first, returning the first exact match. Retain the newest consistent omission
    only as a fallback after every admissible exact candidate is ruled out.
    Refreshed descendants can make expanded histories non-monotonic even when
    published dependency versions advance monotonically. If holding one side fixed
    cannot find an alignment, search both bounded histories as an exceptional fallback. Raise
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
        # Hold it fixed and search for its version or a consistent omission.
        # Retain its full tree: historical candidates may no longer contain it.
        common = {contained[0]}
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

        # 2. Fix the lagging graph as the target while searching the leading
        #    subscription's bounded history.
        if left_nodes[mismatch] > right_nodes[mismatch]:
            leading, start = left, left_revision
            leading_is_left = True
        else:
            leading, start = right, right_revision
            leading_is_left = False
    leading_floor = lower_bounds[0 if leading_is_left else 1]
    fixed_floor = lower_bounds[1 if leading_is_left else 0]
    fixed = right if leading_is_left else left
    fixed_revision = right_revision if leading_is_left else left_revision
    fixed_nodes = _nodes(right_graph if leading_is_left else left_graph, fixed_revision)

    def result(revision: DataRevisionId) -> tuple[DataRevisionId, DataRevisionId]:
        return (revision, right_revision) if leading_is_left else (left_revision, revision)

    # 3. Walk actual revisions newest first, respecting proposed/current bounds.
    # Revision zero links to the preceding sparse version and its maximum count;
    # only counts within a version are consecutive.
    #
    # A refreshed intermediate can change an expanded history from [1, 3, 5]
    # to [7, 3, 5]. Neither endpoints nor middle probes can then rule out a range.
    # Visiting newest first makes the first exact match the largest admissible
    # revision, so it can return immediately without searching older records.
    absent = None
    for revision in _revisions_backwards(start, cache, leading_floor):
        nodes = _nodes(leading.revision(revision, cache), revision)
        comparison = _compare_candidates(revision, nodes, fixed_revision, fixed_nodes, common)
        if comparison is _Comparison.CONFLICT:
            continue
        if comparison is _Comparison.EXACT:
            return result(revision)
        if absent is None:
            absent = revision  # Remember the newest consistent omission only.

    # 4. An omission has lower precedence than every admissible exact match.
    # Use it only after the entire bounded history has been searched.
    if absent is not None:
        return result(absent)
    # Independent inputs may have been consumed in different orders, leaving no
    # revision that matches the other side's complete fixed dependency vector.
    # Only after the normal search fails do we allow both sides to move. Retain
    # argument order and the original proposed upper bounds throughout.
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
    affect the normal one-sided search. Metadata failures propagate.
    """
    absent = None
    for fixed_revision in _revisions_backwards(fixed_start, cache, fixed_floor):
        fixed_nodes = _nodes(fixed.revision(fixed_revision, cache), fixed_revision)
        for leading_revision in _revisions_backwards(leading_start, cache, leading_floor):
            leading_nodes = _nodes(leading.revision(leading_revision, cache), leading_revision)
            comparison = _compare_candidates(
                leading_revision, leading_nodes, fixed_revision, fixed_nodes, common
            )
            if comparison is _Comparison.CONFLICT:
                continue
            pair = leading_revision, fixed_revision
            if comparison is _Comparison.EXACT:
                return pair
            if absent is None:
                absent = pair
    if absent is not None:
        return absent
    raise NoConsistentCutError("No compatible pair exists within the proposed histories")


def _compare_candidates(
    left_revision: DataRevisionId,
    left_nodes: dict[DataId, DataVersion],
    right_revision: DataRevisionId,
    right_nodes: dict[DataId, DataVersion],
    common: set[DataId],
) -> _Comparison:
    # Containment belongs to this candidate pair, not the original proposals.
    # A matching root is exact even if its latest dependency revision has
    # changed or removed descendants from the original common-node set.
    if left_revision[0] in right_nodes:
        matches = left_revision[1] == right_nodes[left_revision[0]]
        return _Comparison.EXACT if matches else _Comparison.CONFLICT
    if right_revision[0] in left_nodes:
        matches = right_revision[1] == left_nodes[right_revision[0]]
        return _Comparison.EXACT if matches else _Comparison.CONFLICT

    # Without containment every shared node must agree, including newly shared
    # nodes. Losing an original dependency cannot excuse a different conflict.
    if _first_mismatch(left_nodes, right_nodes) is not None:
        return _Comparison.CONFLICT
    if common <= left_nodes.keys() and common <= right_nodes.keys():
        return _Comparison.EXACT
    return _Comparison.OMISSION


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
