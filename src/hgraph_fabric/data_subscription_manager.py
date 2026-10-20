from collections.abc import Iterable, Iterator
from itertools import chain

from .data_cache import DataCache
from .data_structures import DataId, DataRevisionId, DataVersion, DataVersionId
from .data_subscription_graph import DataSubscriptionGraph


class DataSubscriptionManager:
    """Manage subscription trees using a shared data cache."""

    def __init__(self, cache: DataCache) -> None:
        self._cache = cache
        self._subscriptions: dict[DataId, DataSubscriptionGraph] = {}

    def add_subscription(self, data_id: DataId) -> DataSubscriptionGraph:
        if data_id not in self._subscriptions:
            subscription = DataSubscriptionGraph(data_id)
            subscription.latest_revision = self._cache.latest(data_id)
            subscription.latest(self._cache)
            self._subscriptions[data_id] = subscription
        return self._subscriptions[data_id]

    def add_subscriptions(self, data_ids: Iterable[DataId]) -> None:
        for data_id in data_ids:
            self.add_subscription(data_id)

    def get_consistent_cut(self) -> set[DataVersionId]:
        """Select compatible root versions by walking newer histories backwards.

        Prefer exact dependency matches to revisions without the dependency.
        Search the newer conflicting tree first, retaining alternative paths
        for changing dependencies. Commit current selections only after
        agreement; raise LookupError if the histories cannot be aligned.
        """
        candidates: dict[DataId, DataRevisionId] = {}
        for data_id, subscription in self._subscriptions.items():
            if subscription.latest_revision is None:
                raise LookupError(f"No latest revision is selected for {data_id!r}")
            candidates[data_id] = subscription.latest_revision

        pending: list[Iterator[dict[DataId, DataRevisionId]]] = [
            iter([candidates])
        ]
        visited: set[tuple[DataRevisionId, ...]] = set()
        while pending:
            candidates = next(pending[-1], None)
            if candidates is None:
                pending.pop()
                continue
            state = tuple(candidates.values())
            if state in visited:
                continue
            visited.add(state)
            versions: dict[DataId, tuple[DataVersion, DataId]] = {}
            for data_id, revision_id in candidates.items():
                graph = self._subscriptions[data_id].revision(revision_id, self._cache)
                # Leaf roots have no edges, but still participate in the cut.
                for node_id, version in chain((revision_id[:2],), graph.nodes()):
                    if node_id in versions:
                        other_version, other_owner = versions[node_id]
                        if version != other_version:
                            newer, older = (
                                (data_id, other_owner)
                                if version > other_version
                                else (other_owner, data_id)
                            )
                            break
                    versions[node_id] = version, data_id
                else:
                    continue
                break
            else:
                for data_id, revision_id in candidates.items():
                    self._subscriptions[data_id].current_revision = revision_id
                return {revision_id[:2] for revision_id in candidates.values()}

            # Defer both searches until needed; try the newer tree first.
            pending.append(self._replacements(
                candidates, older, (node_id, max(version, other_version))
            ))
            pending.append(self._replacements(
                candidates, newer, (node_id, min(version, other_version))
            ))
        raise LookupError("No consistent cut exists in the subscription histories")

    def _replacements(
        self,
        candidates: dict[DataId, DataRevisionId],
        data_id: DataId,
        dependency: DataVersionId,
    ) -> Iterator[dict[DataId, DataRevisionId]]:
        for revision_id in self._ordered_revisions(candidates[data_id], dependency):
            yield {**candidates, data_id: revision_id}

    def _ordered_revisions(
        self, revision_id: DataRevisionId, dependency: DataVersionId
    ) -> Iterator[DataRevisionId]:
        """Search exact matches first, then absent dependencies, newest first."""
        # Index actual history before bisecting: version numbers may have gaps.
        history = []
        previous = self._previous_revision(revision_id)
        while previous is not None:
            history.append(previous)
            previous = self._previous_revision(previous)

        subscription = self._subscriptions[revision_id[0]]
        node_id, target = dependency
        versions: dict[int, DataVersion | None] = {}

        def version_at(index: int) -> DataVersion | None:
            if index not in versions:
                candidate = history[index]
                if candidate[0] == node_id:
                    versions[index] = candidate[1]
                else:
                    graph = subscription.revision(candidate, self._cache)
                    versions[index] = (
                        graph.get_data_version_id(node_id)[1]
                        if graph.has_data_id(node_id) else None
                    )
            return versions[index]

        def matches(low: int, high: int) -> Iterator[int]:
            if low >= high:
                return
            mid = (low + high) // 2
            version = version_at(mid)
            # Present versions are monotonic. Absence cannot exclude either
            # range: the dependency may have disappeared and reappeared.
            if version is None or version <= target:
                yield from matches(low, mid)
            if version == target:
                yield mid
            if version is None or version >= target:
                yield from matches(mid + 1, high)

        yielded = set()
        for index in matches(0, len(history)):
            yielded.add(index)
            yield history[index]
        # A missing dependency wins only after exact candidates are exhausted.
        for index, candidate in enumerate(history):
            if index not in yielded and version_at(index) is None:
                yielded.add(index)
                yield candidate
        # Retain other bounds for backtracking when both roots must move.
        for index, candidate in enumerate(history):
            if index not in yielded:
                yield candidate

    def _previous_revision(self, revision_id: DataRevisionId) -> DataRevisionId | None:
        data_id, version, count = revision_id
        if count > 0:
            return data_id, version, count - 1
        # Versions need not be consecutive: only the revision-zero link
        # identifies the previous version and its maximum revision count.
        previous = self._cache.get_revision(revision_id).previous_revision
        if previous is None:
            return None
        if previous[0] >= version:
            raise ValueError("Revision-zero history must point to an older version")
        return data_id, *previous
