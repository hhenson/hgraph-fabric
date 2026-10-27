from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import Future
from contextlib import contextmanager
from datetime import datetime
from functools import partial
from math import isfinite
from queue import Empty, Queue
from threading import Lock, Thread, current_thread
from time import monotonic
from types import TracebackType
from typing import Protocol, Self

from pyarrow import Table

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
        data: Table | None = None,
        id: DataId | None = None,
        dependencies: tuple[DataVersionId, ...] = (),
        weak_dependencies: tuple[DataVersionId, ...] = (),
        as_of: datetime | None = None,
        *,
        version: int | None = None,
        expected_revision: int | None = None,
    ) -> tuple[Table, DataUpdateDetails]:
        """Persist data; the driver delivers the returned update to sessions.

        Omit data to revise dependencies; version selects an older version,
        otherwise the stored latest is selected for this attempt. An unchanged
        dependency publication returns existing details without a new revision.

        Deliver to every session sharing this cache before the next cut/read,
        including this session when it has subscriptions.
        """
        return self._cache.write(data, id, dependencies, weak_dependencies, as_of,
                                 version=version, expected_revision=expected_revision)

    def _tables(self, versions: Iterable[DataVersionId]) -> dict[DataId, Table]:
        return {version[0]: self._cache.get_version(version) for version in versions}


class DataFabricTransport(Protocol):
    """Transport of update metadata; Arrow tables remain in shared storage.

    Deliver applicable notifications in publication order, including local
    publications. The driver coordinates cache writes and notification delivery
    before further cut operations. Serialization and routing belong to the
    transport; dependency notifications must reach interested sessions too.
    """

    def start(self) -> None:
        """Establish reception before loading subscriptions; retain incoming updates.

        Start from the live boundary, without replaying earlier publications.
        Storage must expose each publication before its notification is sent.
        """
        ...

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
            initial = self.get_consistent_cut()
            if initial:
                return initial
            # An all-no-data cut is initialised, but supplies no value delta.
            # Continue receiving within the same timeout budget.
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
        data: Table | None = None,
        id: DataId | None = None,
        dependencies: tuple[DataVersionId, ...] = (),
        weak_dependencies: tuple[DataVersionId, ...] = (),
        as_of: datetime | None = None,
        *,
        version: int | None = None,
        expected_revision: int | None = None,
    ) -> tuple[Table, DataUpdateDetails]:
        """Persist before sending; local subscriptions advance through poll()."""
        previous = None
        if data is None and id is not None:
            previous = (self._cache.load_latest(id) if version is None else
                        self._cache.load_latest_revision((id, version)))
            version = previous[1]
            if expected_revision is None:
                expected_revision = previous[2]
        data, update = super().publish(data, id, dependencies, weak_dependencies, as_of,
                                       version=version, expected_revision=expected_revision)
        if previous != (update.id, *update.revision):
            self._transport.send(update)
        return data, update


@contextmanager
def run_data_fabric_session(
    cache: DataCache,
    transport: DataFabricTransport,
    callback: Callable[[dict[DataId, Table], Callable[..., tuple[Table, DataUpdateDetails]]], None],
    *,
    subscriptions: Iterable[DataId] = (),
    poll_interval: float = 0.05,
) -> Iterator[Callable[..., tuple[Table, DataUpdateDetails]]]:
    """Run a session on one thread and yield its synchronous publish callable.

    The worker owns the cache, transport, and callbacks for this context. External
    publishers queue work and wait for their result. Callbacks receive both the
    change set and publish callable, including on initialization; their publishes
    execute directly on the worker. Do not access cache or transport concurrently.

    Polling is bounded by poll_interval (seconds) to service queued publications
    and shutdown. The transport must honor that timeout and callbacks must return
    for shutdown to complete. Exit finishes accepted publications, stops the
    session, and joins the thread. Worker errors fail queued publications and
    propagate on exit; processing does not resume after failure.
    """
    if not isfinite(poll_interval) or poll_interval <= 0:
        raise ValueError("Poll interval must be finite and positive")

    requests: Queue[
        tuple[Future[tuple[Table, DataUpdateDetails]], Callable[[], tuple[Table, DataUpdateDetails]]] | None
    ] = Queue()
    ready: Future[None] = Future()
    finished: Future[None] = Future()
    lock = Lock()
    accepting = True
    session: PollingDataFabricSession

    def publish(
        data: Table | None = None,
        id: DataId | None = None,
        dependencies: tuple[DataVersionId, ...] = (),
        weak_dependencies: tuple[DataVersionId, ...] = (),
        as_of: datetime | None = None,
        *,
        version: int | None = None,
        expected_revision: int | None = None,
    ) -> tuple[Table, DataUpdateDetails]:
        result: Future[tuple[Table, DataUpdateDetails]] = Future()
        on_worker = current_thread() is thread
        with lock:
            if not accepting and not on_worker:
                if finished.done():
                    finished.result()  # Report the worker's failure if it died.
                raise RuntimeError("Data fabric session is closed")
            operation = partial(session.publish, data, id, dependencies, weak_dependencies, as_of,
                                version=version, expected_revision=expected_revision)
            if not on_worker:
                requests.put((result, operation))
        # The callback is already on the owner thread: waiting on the queue
        # here would deadlock that thread against itself.
        if on_worker:
            return operation()
        return result.result()

    def run() -> None:
        nonlocal accepting, session
        error = None
        try:
            with PollingDataFabricSession(cache, transport) as session:
                session.add_subscriptions(subscriptions)
                ready.set_result(None)
                while True:
                    try:
                        request = requests.get_nowait()
                    except Empty:
                        pass
                    else:
                        if request is None:
                            break
                        result, operation = request
                        try:
                            result.set_result(operation())
                        except BaseException as exc:
                            result.set_exception(exc)
                            raise
                    # Service incoming notifications between publications so
                    # publishers cannot starve subscription callbacks.
                    changed = session.poll(poll_interval)
                    if changed:
                        callback(changed, publish)
        except BaseException as exc:
            error = exc
        finally:
            with lock:
                accepting = False
                while not requests.empty():
                    request = requests.get_nowait()
                    if request:
                        request[0].set_exception(error or RuntimeError("Data fabric session is closed"))
                if error is None:
                    finished.set_result(None)
                else:
                    finished.set_exception(error)
                if not ready.done():
                    ready.set_exception(error or RuntimeError("Data fabric session did not start"))

    thread = Thread(target=run, name="data-fabric-session")
    thread.start()
    try:
        ready.result()
        yield publish
    finally:
        with lock:
            accepting = False
            requests.put(None)
        thread.join()
        finished.result()
