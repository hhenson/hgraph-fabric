"""Optional hgraph integration: one graph-owned polling session per scope."""
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import UTC, timedelta
from typing import TypeVar, get_type_hints

from hgraph import (
    AUTO_RESOLVE, COMPOUND_SCALAR, SIZE, STATE, TS, TSB, TSD, TSL,
    EvaluationEngineApi, Frame, Size, TimeSeriesSchema, compute_node,
    graph, schedule, sink_node, const,
)
from hgraph._frame import as_arrow_table
from hgraph.adaptors.data_frame import schema_from_frame

from .data_cache import DataCache
from .data_fabric_session import DataFabricTransport, PollingDataFabricSession

__all__ = ("fabric_service", "subscribe", "version_id", "publish", "VersionId")

VersionId = tuple[str, int]

WEAK_SIZE = TypeVar("WEAK_SIZE", bound=Size)


class _FabricIO(TimeSeriesSchema):
    data: TSD[str, TS[Frame]]
    versions: TSD[str, TS[VersionId]]
    session: TS[object]


@dataclass(eq=False)
class _Config:
    cache: DataCache
    transport: DataFabricTransport
    subscriptions: set[str] = field(default_factory=set)


_scope = ContextVar("hgraph_fabric_scope", default=None)


@contextmanager
def fabric_service(
    cache: DataCache,
    transport: DataFabricTransport,
    *,
    poll_interval: timedelta = timedelta(milliseconds=10),
):
    """Wire components inside one fabric session and consistent-cut scope.

    Use inside the host graph. Subscription IDs are fixed during wiring; the
    graph executor owns the session and polls once per interval without blocking.
    The transport starts before the initial image loads and stops with the graph.
    """
    if poll_interval <= timedelta(0):
        raise ValueError("Poll interval must be positive")
    config = _Config(cache, transport)
    token = _scope.set((config, _session_source(schedule(poll_interval, initial_delay=False), config)))
    try:
        yield
    finally:
        _scope.reset(token)


def _service():
    scope = _scope.get()
    if scope is None:
        raise RuntimeError("Fabric clients must be wired inside fabric_service(...)")
    return scope


@compute_node
def _session_source(tick: TS[bool], config: _Config, _state: STATE = None) -> TSB[_FabricIO]:
    changed = _state.session.poll(0)
    delta = {"session": _state.session} if _state.first else {}
    _state.first = False
    if changed:
        delta.update(data=changed, versions={
            key: (key, int(data.schema.metadata[b"version"])) for key, data in changed.items()
        })
    return delta or None


@_session_source.start
def _start_session(config: _Config, _state: STATE = None):
    session = PollingDataFabricSession(config.cache, config.transport)
    try:
        session.start()
        session.add_subscriptions(sorted(config.subscriptions))
    except BaseException:
        session.stop()
        raise
    _state.session = session
    _state.first = True


@_session_source.stop
def _stop_session(_state: STATE = None):
    if hasattr(_state, "session"):
        _state.session.stop()


@graph
def subscribe(id: str, schema: type[COMPOUND_SCALAR] = AUTO_RESOLVE) -> TS[Frame[COMPOUND_SCALAR]]:
    """Subscribe to a fixed dataset ID in the enclosing fabric service."""
    config, io = _service()
    config.subscriptions.add(id)
    return _typed_frame[COMPOUND_SCALAR: schema](io.data[id])


@compute_node
def _typed_frame(data: TS[Frame], schema: type[COMPOUND_SCALAR] = AUTO_RESOLVE) -> TS[Frame[COMPOUND_SCALAR]]:
    frame = as_arrow_table(data.value)
    if get_type_hints(schema_from_frame(frame)) != get_type_hints(schema):
        raise TypeError(f"Fabric table does not match subscription schema {schema.__name__}")
    return frame


@graph
def version_id(id: str) -> TS[VersionId]:
    """Return the subscribed data version, ticking alongside its selected table."""
    config, io = _service()
    config.subscriptions.add(id)
    return io.versions[id]


def publish(
    df: TS[Frame[COMPOUND_SCALAR]],
    id: TS[str],
    dependencies: TSL[TS[VersionId], SIZE],
    weak_dependencies: TSL[TS[VersionId], WEAK_SIZE] = (),
) -> None:
    """Publish a table tick, or revise dependencies when only those inputs tick.

    Explicit dependencies are required; use an empty TSL for a source dataset.
    A changed ID publishes the currently valid table under the new ID. Published
    results reach consumers through subscribe(), after notification and cut
    selection, rather than directly from this sink.
    """
    _, io = _service()
    if isinstance(dependencies, (tuple, list)):
        dependencies = (TSL.from_ts(*dependencies, tp=TS[VersionId]) if dependencies
                        else const((), tp=TSL[TS[VersionId], Size[0]]))
    if isinstance(weak_dependencies, (tuple, list)):
        weak_dependencies = (TSL.from_ts(*weak_dependencies, tp=TS[VersionId]) if weak_dependencies
                             else const((), tp=TSL[TS[VersionId], Size[0]]))
    _publish(df, id, dependencies, weak_dependencies, io.session)


@sink_node(valid=("df", "id", "session"))
def _publish(
    df: TS[Frame[COMPOUND_SCALAR]],
    id: TS[str],
    dependencies: TSL[TS[VersionId], SIZE],
    weak_dependencies: TSL[TS[VersionId], WEAK_SIZE],
    session: TS[object],
    _state: STATE = None,
    _api: EvaluationEngineApi = None,
) -> None:
    if ((len(dependencies) and not dependencies.all_valid)
            or (len(weak_dependencies) and not weak_dependencies.all_valid)):
        return
    # A frame may have ticked while a dependency list was incomplete. Publish
    # that retained frame once all dependencies arrive, even on a later tick.
    new_table = (getattr(_state, "data_time", None) != df.last_modified_time
                 or getattr(_state, "id", None) != id.value)
    session.value.publish(
        as_arrow_table(df.value) if new_table else None,
        id=id.value, dependencies=tuple(dependencies.value) if len(dependencies) else (),
        weak_dependencies=tuple(weak_dependencies.value) if len(weak_dependencies) else (),
        as_of=_api.evaluation_clock.evaluation_time.replace(tzinfo=UTC),
    )
    _state.id = id.value
    _state.data_time = df.last_modified_time
