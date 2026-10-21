from collections.abc import Iterable
from datetime import datetime
from time import monotonic
from types import TracebackType
from typing import Protocol, Self

from pyarrow import Table

from .consistent_cut import NoConsistentCutError
from .data_cache import DataCache
from .data_structures import DataId, DataUpdateDetails, DataVersionId
from .data_subscription_graph import DataSubscriptionGraph
from .data_subscription_manager import DataSubscriptionManager


class DataFabricSession:
    """Subscription and publication facade driven by an external caller."""

    def __init__(self, cache: DataCache) -> None:
        self._cache = cache
        self._subscriptions = DataSubscriptionManager(cache)

    def add_subscription(self, data_id: DataId) -> DataSubscriptionGraph:
        return self._subscriptions.add_subscription(data_id)

    def add_subscriptions(self, data_ids: Iterable[DataId]) -> None:
        self._subscriptions.add_subscriptions(data_ids)

    def get_consistent_cut(self) -> dict[DataId, Table]:
        """Select a consistent cut and load its subscribed tables."""
        return self._tables(self._subscriptions.get_consistent_cut())

    def apply_update(self, update: DataUpdateDetails) -> dict[DataId, Table]:
        """Apply a notification and load only subscriptions with new data."""
        return self._tables(self._subscriptions.apply_update(update))

    def publish(
        self,
        data: Table,
        id: DataId | None = None,
        dependencies: tuple[DataVersionId, ...] = (),
        weak_dependencies: tuple[DataVersionId, ...] = (),
        as_of: datetime | None = None,
    ) -> tuple[Table, DataUpdateDetails]:
        """Persist data; the driver delivers the returned update to sessions.

        Deliver to every session sharing this cache before the next cut/read,
        including this session when it has subscriptions.
        """
        return self._cache.write(data, id, dependencies, weak_dependencies, as_of)

    def _tables(self, versions: Iterable[DataVersionId]) -> dict[DataId, Table]:
        return {version[0]: self._cache.get_version(version) for version in versions}


class DataFabricTransport(Protocol):
    """Transport of update metadata; Arrow tables remain in shared storage.

    Deliver applicable notifications in publication order, including local
    publications. The driver coordinates cache writes and notification delivery
    before further cut operations. Serialization and routing belong to the
    transport; dependency notifications must reach interested sessions too.
    """

    def start(self) -> None: ...

    def stop(self) -> None: ...

    def poll(self, timeout: float | None = None) -> DataUpdateDetails | None:
        """Return one update or None on timeout; seconds, None waits indefinitely."""
        ...

    def send(self, update: DataUpdateDetails) -> None: ...


class PollingDataFabricSession(DataFabricSession):
    """Drive a session with a caller-owned polling loop."""

    def __init__(self, cache: DataCache, transport: DataFabricTransport) -> None:
        super().__init__(cache)
        self._transport = transport

    def start(self) -> None:
        self._transport.start()

    def stop(self) -> None:
        self._transport.stop()

    def __enter__(self) -> Self:
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.stop()

    def poll(self, timeout: float | None = None) -> dict[DataId, Table]:
        """Initialize pending subscriptions, otherwise receive until data advances.

        Initialization returns the full consistent cut before receiving messages.
        Timeout is in seconds; None waits indefinitely. Zero makes one
        nonblocking attempt. Notifications without a delta consume the same
        timeout budget, rather than starting a fresh wait.
        """
        if timeout is not None and timeout < 0:
            raise ValueError("Timeout must be nonnegative")
        deadline = None if timeout is None else monotonic() + timeout
        remaining = timeout
        if not self._subscriptions.is_initialized:
            try:
                return self.get_consistent_cut()
            except NoConsistentCutError:
                # A later publication can establish the first cut. Missing or
                # malformed metadata still propagates as a processing error.
                if deadline is not None:
                    remaining = max(0.0, deadline - monotonic())
        while True:
            update = self._transport.poll(remaining)
            if update is None:
                return {}
            changed = self.apply_update(update)
            if changed:
                return changed
            if deadline is not None:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    return {}

    def publish(
        self,
        data: Table,
        id: DataId | None = None,
        dependencies: tuple[DataVersionId, ...] = (),
        weak_dependencies: tuple[DataVersionId, ...] = (),
        as_of: datetime | None = None,
    ) -> tuple[Table, DataUpdateDetails]:
        """Persist before sending; local subscriptions advance through poll()."""
        data, update = super().publish(data, id, dependencies, weak_dependencies, as_of)
        self._transport.send(update)
        return data, update
