from collections.abc import Iterator
from enum import Enum, auto

from .data_cache import DataCache
from .dependency_resolution import RevisionSearch
from .data_graph import DataEdgeGraph
from .data_structures import DataId, DataRevisionId, DataVersion, DataVersionId
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
    conflict_paths: dict[DataRevisionId, set[DataVersionId]] | None = None,
) -> tuple[DataRevisionId, DataRevisionId]:
    """Return consistent (left revision, right revision), in argument order.

    Start from each subscription's proposed_revision. The caller initializes
    proposed to latest before a search and applies returned pairs between
    comparisons; this function never resets a selection to latest.

    If one subscription's root occurs in the other tree, resolve a newer root
    directly to the consumed version and align the intermediate records too.
    Matching a root value does not discard its dependency constraints.

    Otherwise, the first mismatching common node chooses the leading
    subscription, and the other stays fixed. Prefer matching all original
    common nodes to omitting any; also check newly shared nodes. Search newest
    first, returning the first exact match. Retain the newest consistent omission
    only as a fallback after every admissible exact candidate is ruled out.
    Intermediate revision choices are searched within their referenced data
    version before changing a root revision. If holding one side fixed
    cannot find an alignment, search both bounded histories as an exceptional fallback. Raise
    LookupError if neither search finds a compatible pair.

    Successful intermediate choices are staged on the subscriptions. Root
    revision selections and committed graphs remain unchanged until the caller
    accepts the returned revisions and commits the complete proposed cut.

    Updates supply committed currents as inclusive lower bounds. An exact
    match below current is not a candidate: prefer an omission at current to
    rolling back already delivered data. Initialization leaves these unbounded.
    If supplied, conflict_paths collects consumer versions from rejected paths,
    keyed by the root candidate that reached them, for notification watches.
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
    search = RevisionSearch(cache, conflict_paths=conflict_paths)
    common = _nodes(left_graph, left_revision).keys() & _nodes(right_graph, right_revision).keys()
    resolved, aligned = _resolve_dependencies(left, right, left_revision, right_revision, common, search)
    if aligned is _Comparison.EXACT:
        _stage(left, right, resolved)
        return left_revision, right_revision
    omission = resolved

    # A contained root with a newer data version can jump directly to the
    # referenced version. Its dependency records still need to agree.
    for contained_is_left in (True, False):
        contained = left_revision if contained_is_left else right_revision
        other_nodes = _nodes(right_graph, right_revision) if contained_is_left else _nodes(left_graph, left_revision)
        target = other_nodes.get(contained[0])
        floor = lower_bounds[0 if contained_is_left else 1]
        if target is not None and contained[1] > target and (floor is None or target >= floor[1]):
            adjusted = cache.get_latest_revision((contained[0], target))
            pair = (adjusted, right_revision) if contained_is_left else (left_revision, adjusted)
            resolved, aligned = _resolve_dependencies(left, right, *pair, common, search)
            if aligned is _Comparison.EXACT:
                _stage(left, right, resolved)
                return pair

    left_nodes = _nodes(left_graph, left_revision)
    right_nodes = _nodes(right_graph, right_revision)
    mismatch = _first_mismatch(left_nodes, right_nodes)
    if mismatch is None:
        # The starting graphs already agree; dependency search has staged them.
        _stage(left, right, omission)
        return left_revision, right_revision
    leading_is_left = left_nodes[mismatch] > right_nodes[mismatch]
    leading = left if leading_is_left else right
    start = left_revision if leading_is_left else right_revision
    leading_floor = lower_bounds[0 if leading_is_left else 1]
    fixed = right if leading_is_left else left
    fixed_revision = right_revision if leading_is_left else left_revision

    def result(revision: DataRevisionId) -> tuple[DataRevisionId, DataRevisionId]:
        return (revision, right_revision) if leading_is_left else (left_revision, revision)

    # 3. Walk actual revisions newest first, respecting proposed/current bounds.
    # Revision zero links to the preceding sparse version and its maximum count;
    # only counts within a version are consecutive.
    #
    # Each root candidate searches intermediate revisions within fixed data
    # versions. Expanded descendants are not scalar ordering keys for roots
    # with changing paths. Immutable immediate records provide binary bounds
    # inside RevisionSearch when their membership and ordering permit it.
    absent = omission if leading_is_left else omission[::-1] if omission else None
    fixed_inputs = search.record(fixed_revision) | {fixed_revision[0]: fixed_revision[1]}
    for revision in _revisions_backwards(start, cache, leading_floor, search, fixed_inputs):
        resolved, comparison = _resolve_dependencies(
            leading, fixed, revision, fixed_revision, common, search)
        if comparison is _Comparison.EXACT:
            _stage(leading, fixed, resolved)
            return result(revision)
        if resolved is not None and absent is None:
            absent = resolved

    # 4. An omission has lower precedence than every admissible exact match.
    # Use it only after the entire bounded history has been searched.
    if absent is not None:
        _stage(leading, fixed, absent)
        return result(absent[0].root)
    # Independent inputs may have been consumed in different orders, leaving no
    # revision that matches the other side's complete fixed dependency vector.
    # Only after the normal search fails do we allow both sides to move. Retain
    # argument order and the original proposed upper bounds throughout.
    joint = _search_both_histories(
        leading, fixed, start, fixed_revision, common, cache, leading_floor,
        lower_bounds[1 if leading_is_left else 0], conflict_paths=conflict_paths,
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
    *, conflict_paths=None,
) -> tuple[DataRevisionId, DataRevisionId]:
    """Rare exhaustive fallback; prefer retaining the formerly fixed side.

    Visit its history newest first, searching the leading history newest first
    at each step. Prefer preserving the original common dependencies to omitting
    them. This costs O(L * R) tree comparisons in the worst case, but does not
    affect the normal one-sided search. Metadata failures propagate.
    """
    absent = None
    search = RevisionSearch(cache, conflict_paths=conflict_paths)
    for fixed_revision in _revisions_backwards(fixed_start, cache, fixed_floor, search):
        fixed_inputs = search.record(fixed_revision) | {fixed_revision[0]: fixed_revision[1]}
        for leading_revision in _revisions_backwards(leading_start, cache, leading_floor, search, fixed_inputs):
            resolved, comparison = _resolve_dependencies(
                leading, fixed, leading_revision, fixed_revision, common, search)
            if comparison is _Comparison.EXACT:
                _stage(leading, fixed, resolved)
                return leading_revision, fixed_revision
            if resolved is not None and absent is None:
                absent = resolved
    if absent is not None:
        _stage(leading, fixed, absent)
        return absent[0].root, absent[1].root
    raise NoConsistentCutError("No compatible pair exists within the proposed histories")


def _stage(left, right, resolved):
    left.stage_resolution(resolved[0])
    right.stage_resolution(resolved[1])


def _resolve_dependencies(left, right, left_revision, right_revision, common, search):
    """Hold root revisions fixed and resolve intermediate revision choices first.

    Start with the proposals already selected by earlier comparisons. A failed
    candidate never changes either proposal. Try changing one tree before the
    joint search; retain omissions only after ruling out an exact alignment.
    """
    if left_revision == left.proposed_revision and right_revision == right.proposed_revision:
        a, b = left.proposal(search.cache), right.proposal(search.cache)
        comparison = _compare_candidates(left_revision, a.nodes, right_revision, b.nodes, common)
        if comparison is _Comparison.CONFLICT:
            conflicts = {key for key in a.nodes.keys() & b.nodes.keys() if a.nodes[key] != b.nodes[key]}
            search.watch_conflict_paths(left_revision, conflicts, a.graph)
            search.watch_conflict_paths(right_revision, conflicts, b.graph)
        if comparison is _Comparison.EXACT:
            return (a, b), comparison

    # A root's immediate version references cannot be revised while that root
    # revision is fixed. Avoid exploring unrelated intermediate histories.
    if (search.record(right_revision).get(left_revision[0], left_revision[1]) != left_revision[1]
            or search.record(left_revision).get(right_revision[0], right_revision[1]) != right_revision[1]):
        return None, _Comparison.CONFLICT

    def candidates(sub, revision, constraints=None):
        yield from search.solutions(revision, constraints=constraints,
                                    ceilings=sub._proposed_ceilings)

    def initial(sub, revision):
        if sub.proposed_revision == revision:
            return sub.proposal(search.cache)
        return next(candidates(sub, revision), None)

    first_left, first_right = initial(left, left_revision), initial(right, right_revision)
    absent = None
    seen = set()

    def pairs():
        if first_left is not None and first_right is not None:
            yield first_left, first_right
            conflict = _first_mismatch(first_left.nodes, first_right.nodes)
            # Start at the leading conflict path, independent of argument order.
            left_leads = conflict is None or first_left.nodes[conflict] > first_right.nodes[conflict]
            order = (True, False) if left_leads else (False, True)
            for change_left in order:
                if change_left:
                    for candidate in candidates(left, left_revision, first_right.nodes):
                        yield candidate, first_right
                else:
                    for candidate in candidates(right, right_revision, first_left.nodes):
                        yield first_left, candidate
        for candidate in candidates(left, left_revision):
            for other in candidates(right, right_revision, candidate.nodes):
                yield candidate, other

    for a, b in pairs():
        key = a.signature, b.signature
        if key in seen:
            continue
        seen.add(key)
        comparison = _compare_candidates(left_revision, a.nodes, right_revision, b.nodes, common)
        if comparison is _Comparison.EXACT:
            return (a, b), comparison
        if comparison is _Comparison.OMISSION and absent is None:
            absent = a, b
    return absent, _Comparison.OMISSION if absent else _Comparison.CONFLICT


def _compare_candidates(
    left_revision: DataRevisionId,
    left_nodes: dict[DataId, DataVersion],
    right_revision: DataRevisionId,
    right_nodes: dict[DataId, DataVersion],
    common: set[DataId],
) -> _Comparison:
    # Root versions and every other shared node belong to the same cut.
    # If a root also occurs below its peer, select compatible intermediate
    # records there rather than ignoring the root's upstream dependencies.
    if _first_mismatch(left_nodes, right_nodes) is not None:
        return _Comparison.CONFLICT
    if left_revision[0] in right_nodes or right_revision[0] in left_nodes:
        return _Comparison.EXACT
    if common <= left_nodes.keys() and common <= right_nodes.keys():
        return _Comparison.EXACT
    return _Comparison.OMISSION


def _revisions_backwards(
    revision: DataRevisionId, cache: DataCache, floor: DataRevisionId | None,
    search: RevisionSearch, constraints=None,
) -> Iterator[DataRevisionId]:
    # Counts are consecutive within a version. Version IDs are sparse and must
    # be reached through revision-zero links, including the prior maximum count.
    for version, maximum_count in _version_bounds(revision, cache, floor):
        minimum_count = floor[2] if floor is not None and version == floor[1] else 0
        # Reaching the subscribed parent uses the same zero-count probe and
        # immediate-record bounds as intermediate parents. Only immutable inputs
        # of the fixed root constrain this search; its descendants may resolve
        # through different intermediate revisions during a later comparison.
        for count in search.counts((revision[0], version), maximum_count, constraints, minimum_count):
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
