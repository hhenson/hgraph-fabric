from collections.abc import Iterable
from itertools import combinations

from .consistent_cut import find_consistent_cut
from .data_cache import DataCache
from .data_structures import DataId, DataVersionId
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
        """Run pairwise tournament rounds from latest until all pairs agree.

        Apply each returned revision pair to current selections. A later match
        can invalidate an earlier one, so repeat until a full round is unchanged.
        If a comparison fails, restore the selections from before the tournament.
        """
        subscriptions = tuple(self._subscriptions.values())
        for subscription in subscriptions:
            if subscription.latest_revision is None:
                raise LookupError(f"No latest revision is selected for {subscription.data_id!r}")
        previous = tuple(subscription.current_revision for subscription in subscriptions)
        try:
            # Initialize everyone before the first match. Subsequent matches
            # use current, preserving adjustments made earlier in the tournament.
            for subscription in subscriptions:
                subscription.current_revision = subscription.latest_revision

            while True:
                changed = False
                for left, right in combinations(subscriptions, 2):
                    left_revision, right_revision = find_consistent_cut(left, right, self._cache)
                    if left.current_revision != left_revision:
                        left.current_revision = left_revision
                        changed = True
                    if right.current_revision != right_revision:
                        right.current_revision = right_revision
                        changed = True
                if not changed:
                    # Empty and single-subscription tournaments also finish here.
                    return {subscription.current_revision[:2] for subscription in subscriptions}
                # Revisions only move backwards. A stable round verifies every
                # pair against the final selections, including earlier matches.
        except Exception:
            for subscription, revision in zip(subscriptions, previous):
                subscription.current_revision = revision
            raise
