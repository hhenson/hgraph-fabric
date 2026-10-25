from collections import deque
from collections.abc import Iterable

from .consistent_cut import NoConsistentCutError, find_consistent_cut
from .data_cache import DataCache
from .data_structures import DataId, DataRevisionId, DataUpdateDetails, DataVersionId
from .data_subscription_graph import DataSubscriptionGraph, InconsistentDependencyError


class DataSubscriptionManager:
    """Manage subscription trees using a shared data cache."""

    def __init__(self, cache: DataCache) -> None:
        self._cache = cache
        self._subscriptions: dict[DataId, DataSubscriptionGraph] = {}
        self._consistent_revisions: dict[DataId, DataRevisionId] | None = None
        self._dependents: dict[DataVersionId, set[DataId]] = {}
        self._subscription_dependencies: dict[DataId, set[DataVersionId]] = {}
        self._conflicts: dict[DataVersionId, set[DataId]] = {}
        self._dirty_subscriptions: set[DataId] = set()
        self._pending_cuts: dict[DataId, set[DataVersionId]] = {}

    def add_subscription(self, data_id: DataId) -> DataSubscriptionGraph:
        if data_id not in self._subscriptions:
            subscription = DataSubscriptionGraph(data_id)
            subscription.latest_revision = self._cache.latest(data_id)
            try:
                subscription.latest(self._cache)
            except InconsistentDependencyError:
                pass  # Register both paths; initialization waits for a cut.
            self._index_dependencies(subscription)
            self._subscriptions[data_id] = subscription
        return self._subscriptions[data_id]

    def add_subscriptions(self, data_ids: Iterable[DataId]) -> None:
        for data_id in data_ids:
            self.add_subscription(data_id)

    @property
    def is_initialized(self) -> bool:
        """Whether every registered subscription has a current revision."""
        return all(sub.current_revision is not None for sub in self._subscriptions.values())

    def apply_update(self, update: DataUpdateDetails) -> set[DataVersionId]:
        """Cache an update and return newly available subscription data versions."""
        advanced = self._apply_update(update)
        self.cleanup_conflicts()
        return advanced

    def _apply_update(self, update: DataUpdateDetails) -> set[DataVersionId]:
        self._cache.apply_update(update)
        revision = (update.id, *update.revision)
        # Only trees using this exact data version can have changed. Current
        # and latest are indexed: a waiting latest tree still needs refreshes.
        affected = set()
        dependents = set(self._dependents.get(revision[:2], ()))
        for data_id in dependents:
            sub = self._subscriptions[data_id]
            invalidated = sub.invalidate_dependency(revision)
            if invalidated & {sub.current_revision, sub.latest_revision}:
                affected.add(data_id)
            if sub.current_revision in invalidated:
                self._dirty_subscriptions.add(data_id)
        for data_id in affected:
            self._index_dependencies(self._subscriptions[data_id])

        subscription = self._subscriptions.get(update.id)
        if subscription is not None:
            # Startup can load an image newer than queued notifications.
            subscription.latest_revision = max(subscription.latest_revision or revision, revision)
            self._index_dependencies(subscription)
            before = self._consistent_revisions
            advanced = self._try_advance(subscription)
            if self._consistent_revisions != before:
                return advanced

        # A mismatch inside one tree can block even a single subscription, or
        # an update to a different root. Retry that original driver when an
        # exact dependency version involved in the failed expansion changes.
        for data_id, dependencies in list(self._pending_cuts.items()):
            if revision[:2] in dependencies:
                before = self._consistent_revisions
                advanced = self._try_advance(self._subscriptions[data_id])
                if self._consistent_revisions != before:
                    return advanced
        if subscription is None:
            # Refreshing trees does not itself request a tournament. Wake only
            # recorded conflicts shared by at least two dependent subscriptions,
            # with an affected subscription still waiting behind latest.
            if not affected or len(dependents) < 2 or revision[:2] not in self._conflicts:
                return set()
            # A later comparison can downgrade another dependent onto this
            # path. The original pair identifies the conflict, not every root
            # that can now drive progress when the path changes.
            waiting = affected
            for data_id, sub in self._subscriptions.items():
                if data_id in waiting and sub.current_revision != sub.latest_revision:
                    before = sub.current_revision
                    advanced = self._try_advance(sub)
                    if sub.current_revision != before:
                        return advanced  # Publish one complete proposed cut.
                    # One waiting root may still be blocked by another input.
                    # Allow the next affected root to drive an attempt instead.
            return set()
        return set()

    def _try_advance(self, subscription: DataSubscriptionGraph) -> set[DataVersionId]:
        current = {key: sub.current_revision for key, sub in self._subscriptions.items()}
        subscriptions = tuple(self._subscriptions.values())
        established = current == self._consistent_revisions
        self._pending_cuts.pop(subscription.data_id, None)
        if established and subscription.current_revision == subscription.latest_revision:
            return set()  # This root cannot drive an advance.
        self._prepare_proposals(subscriptions, updating=established)
        updated = subscriptions.index(subscription)
        try:
            if not established:
                # Without a validated existing cut there is no stable set to reuse.
                accepted = self._stabilize(subscriptions, [], list(range(len(subscriptions))), updated)
            else:
                accepted = self._advance_cut(subscriptions, updated)
            if not accepted:
                return set()
            self._commit_proposals(subscriptions)
        except InconsistentDependencyError as error:
            self._pending_cuts[subscription.data_id] = error.dependencies
            return set()
        except NoConsistentCutError:
            # No forward cut is available. Missing/malformed metadata remains
            # an error and must not be mistaken for a blocked update.
            return set()
        return {
            sub.current_revision[:2]
            for key, sub in self._subscriptions.items()
            if current[key] is None or sub.current_revision[1] > current[key][1]
        }

    def _dependency_versions(self, sub, current_revision, choices):
        dependencies = set(choices)
        pending = list({current_revision, sub.latest_revision} - {None})
        visited = set()
        while pending:
            revision = pending.pop()
            if revision in visited:
                continue
            visited.add(revision)
            # Sibling dependency revisions arrive separately. The index must
            # watch every reachable version, even during a temporary mismatch;
            # constructing a selectable tree belongs to cut resolution.
            required_versions = {revision[0]: revision[1]}
            for version in self._cache.get_revision(revision).dependencies or ():
                # One record itself must still be well formed. Only different
                # records may temporarily require different versions.
                if required_versions.setdefault(version[0], version[1]) != version[1]:
                    raise ValueError(f"Conflicting version for data ID {version[0]!r}")
                if version[0] != sub.data_id:
                    dependencies.add(version)
                pending.append(self._cache.get_latest_revision(version))
        return dependencies

    def _index_dependencies(self, sub: DataSubscriptionGraph) -> None:
        self._set_dependency_index(sub, self._dependency_versions(
            sub, sub.current_revision, sub.current_dependency_revisions))

    def _set_dependency_index(self, sub, dependencies):
        previous = self._subscription_dependencies.get(sub.data_id, set())
        for version in previous - dependencies:
            self._dependents[version].remove(sub.data_id)
            if not self._dependents[version]:
                del self._dependents[version]
        for version in dependencies - previous:
            self._dependents.setdefault(version, set()).add(sub.data_id)
        self._subscription_dependencies[sub.data_id] = dependencies

    def _track_conflicts(self, left: DataSubscriptionGraph, right: DataSubscriptionGraph, resolutions=None) -> None:
        """Watch revisions that can change the paths to a version mismatch."""
        resolutions = resolutions or (left.proposal(self._cache), right.proposal(self._cache))
        graphs = tuple(resolution.graph for resolution in resolutions)
        nodes = [resolution.nodes for resolution in resolutions]
        common = nodes[0].keys() & nodes[1].keys()
        conflicts = {key for key in common if nodes[0][key] != nodes[1][key]}
        for graph, sub in zip(graphs, (left, right)):
            # Changing the mismatched node's revision cannot change its data
            # version. Watch its consumers instead: A -> Y versus B -> M -> Y
            # requires A, B and M notifications. A/B are already subscriptions,
            # so only M needs a soft revision watch.
            pending = [parent for key in conflicts if key != sub.data_id
                       for parent in graph.predecessors(key)]
            visited = set()
            while pending:
                key = pending.pop()
                if key in visited or key == sub.data_id:
                    continue
                visited.add(key)
                if key not in self._subscriptions:
                    version = graph.get_data_version_id(key)
                    self._conflicts.setdefault(version, set()).update((left.data_id, right.data_id))
                pending.extend(graph.predecessors(key))

    def get_consistent_cut(self) -> set[DataVersionId]:
        """Downgrade proposals from latest, then commit the complete cut."""
        subscriptions = tuple(self._subscriptions.values())
        self._prepare_proposals(subscriptions)
        try:
            self._stabilize(subscriptions, [], list(range(len(subscriptions))))
            self._commit_proposals(subscriptions)
        except InconsistentDependencyError as error:
            for sub in subscriptions:
                if sub.current_revision != sub.latest_revision:
                    self._pending_cuts[sub.data_id] = error.dependencies
            if not self.is_initialized:
                raise NoConsistentCutError("Dependency revisions have not converged") from error
            # Keep the already delivered selections until a complete cut can
            # be accepted. No partial proposal has been committed.
        self.cleanup_conflicts()
        return {sub.current_revision[:2] for sub in subscriptions}

    def cleanup_conflicts(self) -> None:
        """Retire resolved roots and versions absent from current/latest trees.

        Run after a completed update or initial cut, including blocked updates.
        Keep live version markers while any recorded root is still waiting; that
        root may be blocked by a peer's dependency rather than consume it itself.
        Never clean up midway through the proposed-cut search.
        """
        waiting = {
            sub.data_id for sub in self._subscriptions.values()
            if sub.current_revision != sub.latest_revision
        }
        for version in list(self._conflicts):
            self._conflicts[version].intersection_update(waiting)
            if not self._conflicts[version] or version not in self._dependents:
                del self._conflicts[version]

    def _prepare_proposals(
        self, subscriptions: tuple[DataSubscriptionGraph, ...], *, updating: bool = False,
    ) -> None:
        for sub in subscriptions:
            if sub.latest_revision is None:
                raise LookupError(f"No latest revision is selected for {sub.data_id!r}")
        for sub in subscriptions:
            # A previous cut may have selected older intermediate revisions
            # even without a subsequent notification. Those choices must be
            # reconsidered so a stable root cannot hold back a waiting peer.
            reuse_current = (
                updating and sub.current_revision == sub.latest_revision
                and sub.data_id not in self._dirty_subscriptions
                and all(self._cache.peek_latest_revision(version) == revision
                        for version, revision in sub.current_dependency_revisions.items())
            )
            sub.reset_proposal(reuse_current=reuse_current)

    def _commit_proposals(self, subscriptions: tuple[DataSubscriptionGraph, ...]) -> None:
        # Even a single subscription must expand its full proposed tree. Do all
        # loads before selecting/pruning any current so a failure is atomic.
        for sub in subscriptions:
            sub.proposed_graph(self._cache)
        indexes = []
        for sub in subscriptions:
            resolution = sub.proposal(self._cache)
            choices = set(resolution.revisions) - {resolution.root[:2]}
            indexes.append(self._dependency_versions(sub, sub.proposed_revision, choices))
        # Complete all metadata reads before changing any committed selection.
        for sub, dependencies in zip(subscriptions, indexes):
            sub.commit_proposal(self._cache)
            self._set_dependency_index(sub, dependencies)
        self._consistent_revisions = {sub.data_id: sub.current_revision for sub in subscriptions}
        self._dirty_subscriptions.clear()
        self._pending_cuts.clear()

    def _rejected(self, subscriptions: tuple[DataSubscriptionGraph, ...], updated: int | None) -> bool:
        if updated is None:
            return False  # Initialization is allowed to downgrade.
        sub = subscriptions[updated]
        if sub.current_revision is not None and sub.proposed_revision <= sub.current_revision:
            return True  # The item driving this update can no longer advance.
        return any(
            sub.current_revision is not None and sub.proposed_revision < sub.current_revision
            for sub in subscriptions
        )

    def _resolve(
        self, subscriptions: tuple[DataSubscriptionGraph, ...], pair: tuple[int, int],
        updated: int | None = None,
    ) -> list[int]:
        left, right = (subscriptions[i] for i in pair)
        before_resolutions = left.proposal(self._cache), right.proposal(self._cache)
        before_states = tuple(r.signature for r in before_resolutions)
        try:
            if updated is None:
                revisions = find_consistent_cut(left, right, self._cache)
            else:
                revisions = find_consistent_cut(
                    left, right, self._cache,
                    lower_bounds=(left.current_revision, right.current_revision),
                )
        except NoConsistentCutError:
            self._track_conflicts(left, right)
            raise
        after = []
        for index, revision in zip(pair, revisions):
            sub = subscriptions[index]
            sub.proposed_revision = revision
            after.append(sub.proposal(self._cache).signature)
        changed = [index for index, old, new in zip(pair, before_states, after) if old != new]
        if changed:
            # Causes belong to the graphs before resolution, including a
            # revision-only change below an unchanged subscribed root.
            self._track_conflicts(left, right, before_resolutions)
        return changed

    def _advance_cut(self, subscriptions: tuple[DataSubscriptionGraph, ...], updated: int) -> bool:
        # Waiting roots propose latest; at-latest roots reuse committed trees
        # when their intermediate choices are already latest too.
        # Other pairs are deferred until we know the updated root can advance.
        if self._rejected(subscriptions, updated):
            return False
        others = [i for i in range(len(subscriptions)) if i != updated]
        pending = deque(others)
        queued = set(others)
        compared = []
        while pending:
            other = pending.popleft()
            queued.remove(other)
            pair = (min(updated, other), max(updated, other))
            changed = self._resolve(subscriptions, pair, updated)
            if self._rejected(subscriptions, updated):
                return False
            if updated in changed:
                # Lowering the updated proposal invalidates its earlier matches.
                # Changing only the peer leaves the other star comparisons valid.
                for earlier in compared:
                    if earlier != other and earlier not in queued:
                        pending.append(earlier)
                        queued.add(earlier)
            if other not in compared:
                compared.append(other)

        # Unchanged peers with unchanged trees still agree by the old cut.
        # Refreshed trees need insertion too, even at the same root revision:
        # their old pair checks no longer certify the new dependency metadata.
        stable = [updated] + [
            i for i in others
            if subscriptions[i].proposed_revision == subscriptions[i].current_revision
            and subscriptions[i].data_id not in self._dirty_subscriptions
            and subscriptions[i]._current_resolution is not None
            and subscriptions[i].proposal(self._cache).signature == subscriptions[i]._current_resolution.signature
        ]
        improved = [i for i in others if i not in stable]
        return self._stabilize(subscriptions, stable, improved, updated)

    def _stabilize(
        self,
        subscriptions: tuple[DataSubscriptionGraph, ...],
        stable: list[int],
        incoming: list[int],
        updated: int | None = None,
    ) -> bool:
        """Insert proposals into a stable set, rechecking only invalidated pairs."""
        if self._rejected(subscriptions, updated):
            return False
        for item in incoming:
            pending = deque((min(item, other), max(item, other)) for other in stable)
            queued = set(pending)
            stable.append(item)
            while pending:
                pair = pending.popleft()
                queued.remove(pair)
                changed = self._resolve(subscriptions, pair, updated)
                if self._rejected(subscriptions, updated):
                    return False
                for index in changed:
                    for other in stable:
                        if other == index:
                            continue
                        revisit = (min(index, other), max(index, other))
                        # This pair was just resolved. Pending pairs will already
                        # observe the changed proposal when they execute.
                        if revisit != pair and revisit not in queued:
                            pending.append(revisit)
                            queued.add(revisit)
            # The enlarged set agrees before the next item is introduced.
        return True
