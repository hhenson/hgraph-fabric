from collections import deque
from collections.abc import Iterable

from .consistent_cut import find_consistent_cut
from .data_cache import DataCache
from .data_structures import DataId, DataRevisionId, DataUpdateDetails, DataVersionId
from .data_subscription_graph import DataSubscriptionGraph


class DataSubscriptionManager:
    """Manage subscription trees using a shared data cache."""

    def __init__(self, cache: DataCache) -> None:
        self._cache = cache
        self._subscriptions: dict[DataId, DataSubscriptionGraph] = {}
        self._consistent_revisions: dict[DataId, DataRevisionId] | None = None

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

    def apply_update(self, update: DataUpdateDetails) -> set[DataVersionId]:
        """Cache an update and return newly available subscription data versions."""
        self._cache.apply_update(update)
        subscription = self._subscriptions.get(update.id)
        if subscription is None:
            return set()
        subscription.latest_revision = (update.id, *update.revision)

        current = {key: sub.current_revision for key, sub in self._subscriptions.items()}
        subscriptions = tuple(self._subscriptions.values())
        self._prepare_proposals(subscriptions)
        updated = subscriptions.index(subscription)
        if current != self._consistent_revisions:
            # Without a validated existing cut there is no stable set to reuse.
            accepted = self._stabilize(subscriptions, [], list(range(len(subscriptions))), updated)
        else:
            accepted = self._advance_cut(subscriptions, updated)
        if not accepted:
            return set()
        self._commit_proposals(subscriptions)
        return {
            sub.current_revision[:2]
            for key, sub in self._subscriptions.items()
            if current[key] is None or sub.current_revision[1] > current[key][1]
        }

    def get_consistent_cut(self) -> set[DataVersionId]:
        """Downgrade proposals from latest, then commit the complete cut."""
        subscriptions = tuple(self._subscriptions.values())
        self._prepare_proposals(subscriptions)
        self._stabilize(subscriptions, [], list(range(len(subscriptions))))
        self._commit_proposals(subscriptions)
        return {sub.current_revision[:2] for sub in subscriptions}

    def _prepare_proposals(self, subscriptions: tuple[DataSubscriptionGraph, ...]) -> None:
        for sub in subscriptions:
            if sub.latest_revision is None:
                raise LookupError(f"No latest revision is selected for {sub.data_id!r}")
        for sub in subscriptions:
            sub.proposed_revision = sub.latest_revision

    def _commit_proposals(self, subscriptions: tuple[DataSubscriptionGraph, ...]) -> None:
        # Search only touches proposed selections. Prune history when the whole
        # cut is accepted, using the normal current setter for changed items.
        for sub in subscriptions:
            if sub.current_revision != sub.proposed_revision:
                sub.current_revision = sub.proposed_revision
        self._consistent_revisions = {sub.data_id: sub.current_revision for sub in subscriptions}

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

    def _resolve(self, subscriptions: tuple[DataSubscriptionGraph, ...], pair: tuple[int, int]) -> list[int]:
        left, right = (subscriptions[i] for i in pair)
        revisions = find_consistent_cut(left, right, self._cache)
        changed = []
        for index, revision in zip(pair, revisions):
            if subscriptions[index].proposed_revision != revision:
                subscriptions[index].proposed_revision = revision
                changed.append(index)
        return changed

    def _advance_cut(self, subscriptions: tuple[DataSubscriptionGraph, ...], updated: int) -> bool:
        # First compare only the updated item against everyone else's latest
        # proposal. Other pairs are deferred until we know it can advance.
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
            changed = self._resolve(subscriptions, pair)
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

        # Unchanged peers still agree by the old cut, and the updated item now
        # agrees with all of them. This is the stable set. Only improved peers
        # need insertion and the usual propagation of invalidated comparisons.
        stable = [updated] + [
            i for i in others if subscriptions[i].proposed_revision == subscriptions[i].current_revision
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
                changed = self._resolve(subscriptions, pair)
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
