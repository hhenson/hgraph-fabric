"""hgraph services backed by one threaded fabric session and a push source."""
from dataclasses import dataclass
from datetime import UTC, datetime
from math import isfinite
from queue import Empty, Queue
from threading import Thread
from typing import TypeVar, get_type_hints

from hgraph import (
    AUTO_RESOLVE, COMPOUND_SCALAR, SIZE, SCHEMA, STATE, TS, TSB, TSD, TSL, TSS,
    REMOVE, EvaluationEngineApi, Frame, Size, TimeSeriesSchema, compute_node,
    const, convert, graph, impl_input, impl_output, last_modified_time, push_queue, request_reply_service,
    service_impl, sink_node, subscription_service,
)
from hgraph._frame import as_arrow_table
from hgraph.adaptors.data_frame import schema_from_frame
from pyarrow import Table

from .data_cache import DataCache
from .data_fabric_session import DataFabricTransport, PollingDataFabricSession

__all__ = ("fabric_service", "subscribe", "version_id", "publish", "VersionId")
VersionId = tuple[str, int]
WEAK_SIZE = TypeVar("WEAK_SIZE", bound=Size)


# Erase row schemas at the service boundary so all typed clients use the same
# implementation, worker and cut, rather than one service per schema/TSL size.
@subscription_service
def _subscribe(id: TS[str], path: str = "fabric") -> TS[Frame]: ...


@subscription_service
def version_id(id: TS[str], path: str = "fabric") -> TS[VersionId]:
    """Subscribe to the version selected by the shared consistent cut."""
    ...


@request_reply_service
def _publish(df: TS[Frame], id: TS[str],
             dependencies: TS[tuple[VersionId, ...]],
             weak_dependencies: TS[tuple[VersionId, ...]],
             data_time: TS[datetime], path: str = "fabric") -> TS[VersionId]: ...


@graph
def subscribe(id: TS[str], schema: type[COMPOUND_SCALAR] = AUTO_RESOLVE,
              path: str = "fabric") -> TS[Frame[COMPOUND_SCALAR]]:
    """Subscribe to a dataset through the registered fabric service."""
    return _typed_frame[COMPOUND_SCALAR: schema](_subscribe(id, path=path))


@compute_node
def _typed_frame(data: TS[Frame], schema: type[COMPOUND_SCALAR] = AUTO_RESOLVE) -> TS[Frame[COMPOUND_SCALAR]]:
    frame = as_arrow_table(data.value)
    if get_type_hints(schema_from_frame(frame)) != get_type_hints(schema):
        raise TypeError(f"Fabric table does not match subscription schema {schema.__name__}")
    return frame


@dataclass(frozen=True, eq=False)
class _Publication:
    data: Table | None
    id: str
    dependencies: tuple[VersionId, ...]
    weak_dependencies: tuple[VersionId, ...]
    as_of: datetime


def publish(df: TS[Frame[COMPOUND_SCALAR]], id: TS[str],
            dependencies: TSL[TS[VersionId], SIZE],
            weak_dependencies: TSL[TS[VersionId], WEAK_SIZE] = (),
            path: str = "fabric") -> TS[VersionId]:
    """Request publication; reply with the persisted version, not subscribed data.

    A table/ID tick creates a version; dependency-only ticks revise its metadata.
    Consumers must subscribe to receive data selected by the consistent cut.
    """
    def inputs(value):
        if isinstance(value, (tuple, list)):
            if not value:
                return const((), tp=TS[tuple[VersionId, ...]])
            value = TSL.from_ts(*value, tp=TS[VersionId])
        return _dependency_ids(value)
    return _publish(df, id, inputs(dependencies), inputs(weak_dependencies),
                    last_modified_time(df), path=path)


@graph
def _dependency_ids(dependencies: TSL[TS[VersionId], SIZE],
                    size: type[SIZE] = AUTO_RESOLVE) -> TS[tuple[VersionId, ...]]:
    if size.SIZE == 0:
        return const((), tp=TS[tuple[VersionId, ...]])
    return convert[TS[tuple[VersionId, ...]]](dependencies)


class _FabricIO(TimeSeriesSchema):
    data: TSD[str, TS[Frame]]
    versions: TSD[str, TS[VersionId]]
    replies: TSD[int, TS[VersionId]]


class _Worker:
    """One owner for storage, transport, subscription changes and publications."""

    def __init__(self, cache, transport, poll_interval):
        self.cache, self.transport, self.poll_interval = cache, transport, poll_interval

    def start(self, sender):
        self.commands = Queue()
        self.error = None
        self.thread = Thread(target=self.run, args=(sender,), name="fabric-adaptor")
        self.thread.start()

    def stop(self):
        self.commands.put(None)
        self.thread.join()
        if self.error is not None:
            raise self.error

    def run(self, sender):
        session = PollingDataFabricSession(self.cache, self.transport)
        keys = frozenset()
        try:
            # Establish reception before the first subscription image loads.
            session.start()
            while True:
                try:
                    command = self.commands.get_nowait()
                except Empty:
                    command = ()
                if command is None:
                    break
                if command:
                    requested, publications, retired = command
                    if retired:
                        sender({"replies": {key: REMOVE for key in retired}})
                    if requested != keys:
                        # The core manager has no removal API. Rebuild just its
                        # selection on membership changes; keep reception live.
                        replacement = PollingDataFabricSession(self.cache, self.transport)
                        replacement.add_subscriptions(sorted(requested))
                        removed = keys - requested
                        session, keys = replacement, requested
                        if removed:
                            sender({"data": {k: REMOVE for k in removed},
                                    "versions": {k: REMOVE for k in removed}})
                    for request_id, request in publications:
                        _, update = session.publish(
                            request.data, request.id, request.dependencies,
                            request.weak_dependencies, request.as_of)
                        sender({"replies": {request_id: (update.id, update.revision[0])}})
                changed = session.poll(self.poll_interval)
                if changed:
                    sender({"data": changed, "versions": {
                        k: (k, int(table.schema.metadata[b"version"]))
                        for k, table in changed.items()}})
        except BaseException as error:
            self.error = error
            sender(error)
        finally:
            try:
                session.stop()
            except BaseException as error:
                if self.error is None:
                    self.error = error
                    sender(error)


@push_queue(TS[object])
def _session_events(sender, worker: _Worker):
    worker.start(sender)


@_session_events.stop
def _stop_session(worker: _Worker):
    worker.stop()


@compute_node
def _responses(event: TS[object]) -> TSB[_FabricIO]:
    if isinstance(event.value, BaseException):
        raise event.value
    return event.value


@sink_node(valid=())
def _requests(data_ids: TSS[str], version_ids: TSS[str],
              publications: TSD[int, TSB[SCHEMA]], worker: _Worker,
              _state: STATE = None, _api: EvaluationEngineApi = None):
    keys = frozenset(data_ids.value if data_ids.valid else ()) | frozenset(
        version_ids.value if version_ids.valid else ())
    previous = getattr(_state, "previous", {})
    requests = []
    if publications.valid:
        for key, value in publications.modified_items():
            if not value.all_valid:
                continue
            # Request snapshots can re-tick the retained frame. Its original
            # tick time identifies new data without comparing table contents.
            identity = value.id.value, value.data_time.value
            requests.append((key, _Publication(
                as_arrow_table(value.df.value) if previous.get(key) != identity else None,
                value.id.value, tuple(value.dependencies.value), tuple(value.weak_dependencies.value),
                _api.evaluation_clock.evaluation_time.replace(tzinfo=UTC))))
            previous[key] = identity
    retired = tuple(publications.removed_keys()) if publications.valid else ()
    for key in retired:
        previous.pop(key, None)
    _state.previous = previous
    worker.commands.put((keys, tuple(requests), retired))


@service_impl(interfaces=(_subscribe, version_id, _publish))
def fabric_service(path: str, cache: DataCache, transport: DataFabricTransport,
                   poll_interval: float = 0.05):
    """Register with register_service(path, fabric_service, cache=..., transport=...).

    All three interfaces share one worker. The graph sends requests without
    waiting on storage or broker IO; results arrive through a push source.
    poll_interval bounds the worker's broker wait (seconds), not graph polling.
    """
    if not isfinite(poll_interval) or poll_interval <= 0:
        raise ValueError("Poll interval must be finite and positive")
    worker = _Worker(cache, transport, poll_interval)
    output = _responses(_session_events(worker))
    _requests(impl_input(_subscribe, path), impl_input(version_id, path),
              impl_input(_publish, path), worker)
    impl_output(_subscribe, output.data, path)
    impl_output(version_id, output.versions, path)
    impl_output(_publish, output.replies, path)
