from collections.abc import Iterable
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

        Prefer stepping the newer conflicting tree, but retain the other path
        because dependencies can appear or disappear. Return the subscribed
        roots, committing current selections only after agreement. Raise
        LookupError if the histories cannot be aligned.
        """
        candidates: dict[DataId, DataRevisionId] = {}
        for data_id, subscription in self._subscriptions.items():
            if subscription.latest_revision is None:
                raise LookupError(f"No latest revision is selected for {data_id!r}")
            candidates[data_id] = subscription.latest_revision

        pending: list[tuple[dict[DataId, DataRevisionId], DataId | None]] = [
            (candidates, None)
        ]
        visited: set[tuple[DataRevisionId, ...]] = set()
        while pending:
            candidates, step = pending.pop()
            if step is not None:
                previous = self._previous_revision(candidates[step])
                if previous is None:
                    continue
                candidates = {**candidates, step: previous}
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

            # Try the merge direction first; defer loading the alternative.
            pending.append((candidates, older))
            pending.append((candidates, newer))
        raise LookupError("No consistent cut exists in the subscription histories")

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
