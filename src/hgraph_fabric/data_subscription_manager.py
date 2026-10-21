from collections import deque
from collections.abc import Iterable
from copy import copy

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
            # New subscriptions (or externally changed selections) need a
            # complete proposed cut before we can reuse a validated prefix.
            self._advance_cut()
        else:
            # Try every root held behind latest: this update may unlock peers that
            # could not advance previously. Other roots start at committed current.
            candidates = {
                key for key, sub in self._subscriptions.items()
                if sub.latest_revision is not None and sub.current_revision < sub.latest_revision
            }
            if candidates:
                self._advance_cut(candidates)

        # Report the final selections, including peers unlocked by this update.
        # A higher revision count alone does not represent new data.
        return {
            sub.current_revision[:2]
            for key, sub in self._subscriptions.items()
            if sub.current_revision is not None
            and (current[key] is None or sub.current_revision[1] > current[key][1])
        }

    def get_consistent_cut(self) -> set[DataVersionId]:
        """Initialize current to latest, then downgrade to a consistent cut."""
        subscriptions = tuple(self._subscriptions.values())
        previous = tuple(sub.current_revision for sub in subscriptions)
        try:
            self._stabilize_cut(subscriptions)
        except Exception:
            for sub, revision in zip(subscriptions, previous):
                sub.current_revision = revision
            raise
        self._consistent_revisions = {sub.data_id: sub.current_revision for sub in subscriptions}
        return {revision[:2] for revision in self._consistent_revisions.values()}

    def _advance_cut(self, candidates: set[DataId] | None = None) -> None:
        """Find an upgrade without changing the published current selections."""
        subscriptions = tuple(self._subscriptions.values())
        proposed = []
        for sub in subscriptions:
            proposal = copy(sub)
            # Proposal setters prune retained graphs. Give them a private map
            # so blocked or failed searches preserve the published trees too.
            # The graph objects themselves are reused; the search only reads them.
            proposal._revision_graphs = sub._revision_graphs.copy()
            proposed.append(proposal)

        # Latest is the upper bound of the proposed upgrade. Pair comparisons
        # may lower proposals while finding agreement; committed current remains
        # the lower bound and is untouched throughout these comparisons.
        self._stabilize_cut(tuple(proposed), candidates)
        if any(
            sub.current_revision is not None and proposal.current_revision < sub.current_revision
            for sub, proposal in zip(subscriptions, proposed)
        ):
            return

        # The whole proposal is now consistent and moves nothing backwards.
        # Publish only changed selections, pruning old graphs only at commit.
        for sub, proposal in zip(subscriptions, proposed):
            for revision, graph in proposal._revision_graphs.items():
                sub._revision_graphs.setdefault(revision, graph)
            if sub.current_revision != proposal.current_revision:
                sub.current_revision = proposal.current_revision
        self._consistent_revisions = {sub.data_id: sub.current_revision for sub in subscriptions}

    def _stabilize_cut(
        self,
        subscriptions: tuple[DataSubscriptionGraph, ...],
        candidates: set[DataId] | None = None,
    ) -> None:
        """Stabilize the supplied selections, which may be private proposals."""
        for subscription in subscriptions:
            if subscription.latest_revision is None:
                raise LookupError(f"No latest revision is selected for {subscription.data_id!r}")
        # A full search initializes everyone. An update only proposes
        # forward moves for the subscriptions currently behind latest.
        active = [
            i for i, sub in enumerate(subscriptions)
            if candidates is None or sub.data_id in candidates
        ]
        for i in active:
            subscriptions[i].current_revision = subscriptions[i].latest_revision

        # Unchanged subscriptions already form a consistent prefix on an
        # update. A full search starts with an empty prefix. Preserve the
        # registration order when inserting candidates and comparing pairs.
        prefix = [
            i for i, sub in enumerate(subscriptions)
            if candidates is not None and sub.data_id not in candidates
        ]
        for incoming in active:
            pending = deque(
                (min(incoming, other), max(incoming, other)) for other in prefix
            )
            queued = set(pending)
            prefix.append(incoming)
            while pending:
                pair = pending.popleft()
                queued.remove(pair)
                i, j = pair
                revisions = find_consistent_cut(subscriptions[i], subscriptions[j], self._cache)
                changed = []
                for index, revision in zip(pair, revisions):
                    if subscriptions[index].current_revision != revision:
                        subscriptions[index].current_revision = revision
                        changed.append(index)

                # Equality preserves earlier comparisons. Moving either
                # side backwards invalidates its comparisons throughout
                # this prefix, including those with the incoming item.
                for index in changed:
                    for other in prefix:
                        if other == index:
                            continue
                        revisit = (min(index, other), max(index, other))
                        # This pair was just resolved; pending pairs already
                        # observe the changed selection when they execute.
                        if revisit != pair and revisit not in queued:
                            pending.append(revisit)
                            queued.add(revisit)
            # Every pair in the enlarged prefix now agrees. Only then may
            # the next subscription participate in comparisons.
