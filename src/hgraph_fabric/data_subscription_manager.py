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
        if current != self._consistent_revisions:
            # New subscriptions (or externally changed selections) need an
            # initial complete tournament before we can reuse a validated cut.
            self.get_consistent_cut()
        else:
            # Try every root held behind latest: this update may unlock peers that
            # could not advance previously. Roots already at latest remain fixed.
            candidates = {
                key for key, sub in self._subscriptions.items()
                if sub.latest_revision is not None and sub.current_revision < sub.latest_revision
            }
            if candidates:
                self._run_tournament(candidates)

        # Report the final selections, including peers unlocked by this update.
        # A higher revision count alone does not represent new data.
        return {
            sub.current_revision[:2]
            for key, sub in self._subscriptions.items()
            if current[key] is None or sub.current_revision[1] > current[key][1]
        }

    def get_consistent_cut(self) -> set[DataVersionId]:
        """Initialize all roots to latest and run a complete pairwise tournament."""
        return self._run_tournament()

    def _run_tournament(self, candidates: set[DataId] | None = None) -> set[DataVersionId]:
        subscriptions = tuple(self._subscriptions.values())
        for subscription in subscriptions:
            if subscription.latest_revision is None:
                raise LookupError(f"No latest revision is selected for {subscription.data_id!r}")
        previous = tuple(subscription.current_revision for subscription in subscriptions)
        try:
            # A full search initializes everyone. An update only proposes
            # forward moves for the subscriptions currently behind latest.
            active = {
                i for i, sub in enumerate(subscriptions)
                if candidates is None or sub.data_id in candidates
            }
            for i in active:
                subscriptions[i].current_revision = subscriptions[i].latest_revision

            # Other pairs were validated in the preceding cut and have not
            # changed. Revisit only pairs touching a candidate or a later change.
            queued = {
                (min(i, j), max(i, j))
                for i in active for j in range(len(subscriptions)) if i != j
            }
            pending = deque(sorted(queued))
            while pending:
                pair = pending.popleft()
                queued.remove(pair)
                i, j = pair
                left, right = subscriptions[i], subscriptions[j]
                revisions = find_consistent_cut(left, right, self._cache)
                changed = []
                for index, revision in zip(pair, revisions):
                    if subscriptions[index].current_revision != revision:
                        subscriptions[index].current_revision = revision
                        changed.append(index)
                for index in changed:
                    for other in range(len(subscriptions)):
                        if other == index:
                            continue
                        revisit = (min(index, other), max(index, other))
                        # The pair just returned is already consistent.
                        if revisit != pair and revisit not in queued:
                            pending.append(revisit)
                            queued.add(revisit)

            if candidates is not None and any(
                sub.current_revision < old for sub, old in zip(subscriptions, previous)
            ):
                # An incremental update must advance the cut or do nothing.
                # Keep the known consistent cut if the proposal would regress it.
                for sub, revision in zip(subscriptions, previous):
                    sub.current_revision = revision

            self._consistent_revisions = {
                sub.data_id: sub.current_revision for sub in subscriptions
            }
            return {revision[:2] for revision in self._consistent_revisions.values()}
        except Exception:
            for subscription, revision in zip(subscriptions, previous):
                subscription.current_revision = revision
            raise
