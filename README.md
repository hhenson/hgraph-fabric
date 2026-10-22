# hgraph-fabric

A standalone, open-source project for developing the hgraph versioned
dataflow fabric.

The concept connects independent computations through complete, versioned
datasets. Lineage records describe which input versions a computation has
accepted, including when its output is unchanged.

## Status

The package currently provides identity aliases, frozen revision metadata,
a lazy data cache with abstract storage readers and writers, and a directed dependency
edge graph with version checks and node queries. The subscription graph
builds and retains full strong-dependency graphs breadth first, using the cache's
latest-revision lookup for each specific dependency version. Each subscription
tracks `latest_revision`, `proposed_revision` and `current_revision` independently.
`proposed_graph(cache)` loads the proposed tree without pruning retained history;
weak dependencies are excluded. Retained trees record the exact dependency
revisions used to build them. Reuse checks those revisions, so the same root
revision can refresh its ancestors when a referenced version gets a newer count.
The explicit root revision and dependency data versions stay pinned.
`DataSubscriptionManager(cache)` registers roots with `add_subscription(data_id)`
or `add_subscriptions(data_ids)`, loading their latest trees.

`get_consistent_cut()` initializes proposals to latest and builds a consistent
prefix one subscription at a time using `find_consistent_cut`. That pairwise
function compares proposed selections. If either proposal moves backwards, its
comparisons within the prefix are queued again. Only after the complete set is
consistent are proposals copied to current. Before committing, every proposed
full tree is loaded, including a single subscription with no pair comparisons.
Failed searches or tree loads leave current intact.

Cache writes and notifications to every associated subscription manager are
coordinated. Deliver an update to all associated managers before starting another
cut operation or reading trees between those notifications.
The cache's known latest revision and latest revision per version only advance.
An older local notification still supplies its revision metadata, but cannot
lower the publication head and cause a subsequent write to reuse a version.
Subscription selections continue to be managed separately by the manager.

`apply_update(update)` always updates the cache. An update for a subscribed
data ID changes that subscription's latest revision and attempts advancement.
For dependency updates, the manager indexes current and latest trees by exact
`DataVersionId`, and refreshes affected trees. A conflict tracker records versions
on paths to mismatched nodes when comparisons downgrade proposals or cannot find
a bounded cut. It watches consumers whose revisions can change the referenced
version, excluding the mismatched endpoint and IDs already subscribed. For
`A -> Y` versus `B -> M -> Y`, only M needs a soft revision watch: A and B
already receive subscription updates, and revising Y cannot change its version.

An unsubscribed dependency update attempts advancement only if that exact version
has a recorded conflict, at least two subscriptions depend on it, and an affected
subscription is behind latest. Unrelated branches, other data versions and
duplicate notifications do not trigger searches. A still-blocked root does not
prevent another affected root from driving a retry. One complete proposed cut is
committed per notification. `cleanup_conflicts()` retires caught-up roots and
versions absent from all current/latest trees. It runs automatically after each
completed update (including a blocked update) and initial cut, and can also be
called explicitly. Live conflicts remain tracked while roots are waiting.
Affected waiting roots come from the dependency index, including those downgraded
onto a tracked path by later comparisons. Unexpected errors propagate to the
caller and are fatal to processing; recovery and replay after errors are not part
of this contract.

Tree refresh and cut advancement are separate. When the gate skips a search,
root selections stay unchanged; refreshed current trees are marked for comparison
at the next resolution, rather than assumed consistent from earlier checks.
With an established cut, it sets proposals to latest and first compares the
updated item against every other item, rechecking earlier matches if its proposal
changes. If that proposal drops to its committed current (or below), it stops
immediately and retains the existing cut. The update must also leave every other
subscription at or above its committed current. These currents are inclusive
search bounds: an exact match below current cannot displace a valid omission
at current. Bounds apply to both version IDs and revision counts.

If the updated item can advance, it and the unchanged peers form the stable set.
Other improved peers and peers whose current trees were refreshed are inserted
one at a time, propagating invalidated pair comparisons. The same early-stop rule applies during this second stage. Only a
complete consistent proposal is committed; current selections and their retained
graphs remain available throughout the search. Without a validated existing cut,
it constructs the full proposed cut first.

The update returns the `set[DataVersionId]` of subscriptions whose current data
version advanced, including newly selected subscriptions and waiting peers.
Revision-count changes can advance the cut but do not indicate new data and are
excluded from the returned set.

The pairwise search keeps the lagging subscription fixed, follows revision-zero
links across sparse versions, and examines admissible revisions newest first.
The first exact match returns immediately. The newest consistent omission is
remembered, but used only after ruling out every exact match within the bounds.
This can require a linear number of tree comparisons: refreshed descendants can
break the ordering that binary pruning would require.
If one subscription's root occurs in the other tree, only that root version
needs to match; otherwise all common nodes are checked. Subscriptions record
dependency changes while those dependencies remain present. Exact matches
take priority over missing-dependency candidates. If the normal search cannot
align either history with the fixed side, an exceptional fallback searches both
histories within their proposed bounds. It preserves those matching preferences
and does not change selections. This fallback can require a quadratic number of
tree comparisons; successful normal comparisons do not enter it.
Concrete storage backends and end-to-end fabric execution are not implemented.

The cache's `latest(data_id)` returns a revision ID. Historical version lookup
starts there and follows the `(version, revision_count)` pair in
`previous_revision` from each version's revision-0 record, retaining the same
data ID. That pair identifies the previous version and its maximum revision;
`None` marks the end of the chain.

Notifications for an ID whose head has not been loaded are retained as hints.
The first `latest` lookup combines those hints with the stored head, so an older
notification cannot hide a newer version that predates the session. Historical
version lookups likewise combine notified counts with their stored history link.
Pending hints are retired as those lookups resolve; notifications alone do not
make an unseen head available through `peek_latest`.

`peek_latest(data_id)`, `peek_version(version_id)`, `peek_revision(revision_id)`,
and `peek_latest_revision(version_id)` return the corresponding cached value or
`None`. They do not load from storage or walk history; `peek_version` returns the
cached Arrow table, including an empty table. The latest-revision peek reads only
the version-specific lookup cache, even if other cached metadata could resolve it.

`cached_ids()` returns a set of IDs represented in any cache, including entries
cached through `get_version(version_id)` or `get_revision(revision_id)` without a
latest record. It does not enumerate storage. `version_history(data_id)` lazily
yields `(id, version, maximum_revision_count)` newest first, following revision-zero
links and loading missing metadata. `peek_version_history(data_id)` yields only
the prefix reachable from the cached latest and headers, stopping at the first
missing link without loading. Missing storage records raise in the loading form;
malformed history links raise in either form.

`cache.write(data=None, id=None, dependencies=(), weak_dependencies=(), as_of=None)`
accepts an Arrow table and returns `(written_table, DataUpdateDetails)`. Explicit
`id` and `as_of` override schema metadata. An ID must be supplied in one of those
places; missing `as_of` defaults to UTC now. The schema stores UTF-8 `id`, decimal
`version`, and ISO-format `as_of`. A metadata version is used if present;
otherwise the version is derived from the resolved `as_of` in epoch milliseconds.
Naive datetimes are interpreted as UTC for this calculation. Any version not
greater than latest is advanced to `latest_version + 1`, without changing `as_of`.
Other metadata and the input table are preserved.

Writes create revision zero, linking to the previous latest version and count.
Storage subclasses implement `write_version(DataVersionId, Table)`,
`write_revision(DataRevisionId, DataRevisionDetails)`, and
`write_latest(DataId, LatestDataDetails)`, called in that order. `load_latest`
raises `KeyError` for a new ID. After successful persistence, the cache retains
the table and metadata; deliver the returned update to every associated manager
as part of the coordinated write/notification operation. Storage errors propagate.

Omit the table to update dependencies on the latest version:
`cache.write(id="prices", dependencies=(("source", 7),))`, or call
`cache.write_dependencies("prices", dependencies=(("source", 7),))` directly.
Supply the complete strong and weak dependency tuples; omitted tuples are empty.
Changed dependencies increment the revision count and persist only revision and
latest metadata. Unchanged tuples return the existing `(Table, DataUpdateDetails)`
without writes, even when `as_of` differs. The table retains its original version
metadata; the new revision's `as_of` is recorded in the update details. An unknown
ID raises `KeyError`. Supplying a table always creates a new version, without a
table equality check.

We will build the components incrementally in Python to establish the structure
and semantics. A later port to Rust or C++ remains an open decision. Integration
with hgraph will follow once the standalone components are established.

The initial implementation milestone is a working Python core with in-memory
storage. Durable storage adapters come later.
Individual components and their APIs will be agreed as we build them.

## Sessions

`DataFabricSession(cache)` combines a subscription manager and the cache's
publication API. Register roots with `add_subscription` or `add_subscriptions`.
`get_consistent_cut()` returns a `dict[DataId, Table]` for the selected cut;
`apply_update(update)` returns that mapping for only the subscriptions whose
data versions advanced. Tables are loaded through the cache after selection.
Blocked updates and revision-count-only changes return an empty mapping.

`publish(data=None, id=None, dependencies=(), weak_dependencies=(), as_of=None)`
returns the cache's `(Table, DataUpdateDetails)` result. The external driver
delivers that update to each associated session before further cut operations,
including the publishing session when it has subscriptions.

`PollingDataFabricSession(cache, transport)` adds `start()`, `stop()`, and
`poll(timeout=None)`. Use `with PollingDataFabricSession(cache, transport) as session:`
to start on entry and stop on exit, including when the block raises an exception.
Its `publish` persists first, then sends the update metadata. Dependency-only
calls with unchanged dependency tuples return existing details without sending.
This also applies to the threaded helper's publish callable.
If any registered subscription has no current revision, `poll` first initializes
and returns the full consistent cut without receiving a message. This also applies
to subscriptions added after polling has begun; the whole cut is returned because
adding a root can change the versions selected for existing roots.
If no initial consistent cut is available, polling continues receiving within
the remaining timeout so a later publication can establish it. Storage and
malformed metadata errors still propagate.
`poll` receives and applies notifications until subscribed data advances or its
timeout expires. It returns the advanced tables, or `{}` on expiry. Timeouts are
seconds and cover the whole call, including notifications that yield no changes;
`None` waits indefinitely and zero makes one nonblocking attempt.

The `DataFabricTransport` protocol defines `start`, `stop`, `poll` and `send`.
Adapters handle serialization and routing and deliver applicable updates in
publication order, including local publications and dependency updates. Local
subscriptions advance when their publication is polled. The caller drives the
loop and coordinates delivery with cache operations; storage and transport
errors propagate. Arrow tables remain in shared storage.

`run_data_fabric_session(cache, transport, callback, subscriptions=ids)` runs the
polling session on a worker thread and yields a thread-safe publish callable:

```python
from hgraph_fabric.data_fabric_session import run_data_fabric_session

with run_data_fabric_session(cache, transport, on_update, subscriptions=["prices"]) as publish:
    written_table, update = publish(output_table, id="result")
```

The worker starts the session, registers subscriptions, and calls
`on_update(changes, publish)` for the initial cut and later nonempty changes,
where `changes` is a `dict[DataId, Table]`. The callback receives the publish
callable directly, so even the initial callback can publish before the context
body has bound its own callable. Callbacks run on the worker. Publications
from other threads are queued and wait for their result; a callback can use the
publish callable directly without deadlocking. Cache and transport operations
all run on the worker; callers must not access those objects concurrently.

Context exit finishes accepted publications, stops the session and joins the
worker. A finite positive `poll_interval` (default 0.05 seconds) bounds transport
waits so the worker can handle publications and shutdown. Transports must honor
timeouts and callbacks must return for shutdown to finish. Worker failures stop
processing, release waiting publishers with the error, and propagate on context
exit. There is no retry or recovery loop.

## Kafka and Core NATS notifications

Install the adapter you need with `pip install 'hgraph-fabric[kafka]'` or
`pip install 'hgraph-fabric[nats]'`. Both use the existing `DataFabricTransport`
interface and send the same versioned JSON encoding of `DataUpdateDetails`.
Arrow tables and revision history remain in shared storage. Use one channel per
fabric namespace; each session receives all notifications on that channel,
including local publications and updates to dependencies it does not directly
subscribe to.

```python
from hgraph_fabric.data_fabric_session import PollingDataFabricSession
from hgraph_fabric.transports import KafkaDataFabricTransport, NatsDataFabricTransport

transport = KafkaDataFabricTransport("localhost:9092", "fabric-updates")
# Alternatively:
# transport = NatsDataFabricTransport("nats://localhost:4222", "fabric.updates")

# cache must be a fresh DataCache backed by the shared storage.
with PollingDataFabricSession(cache, transport) as session:
    session.add_subscriptions(["prices", "positions"])
    initial = session.poll(0)
    while True:
        changes = session.poll(1.0)
        if changes:
            consume(changes)
```

Start reception **before** registering subscriptions, since registration already
reads metadata. `start()` establishes the live boundary and retains subsequent
notifications while the initial image loads. The first cut can overlap those
notifications: duplicates and older revisions do not lower selected heads.
Newer revisions of historical dependencies still refresh their relevant trees.
The threaded session helper already follows this startup order.

Kafka requires an existing topic with exactly one partition. Each adapter
captures its numeric high-watermark offset at startup and assigns that partition
directly, so every session independently receives subsequent updates. It does
not replay earlier records, join a balancing group, or commit offsets. Keep the
topic single-partition, without compaction, and retain records long enough for
active sessions to consume them. `send()` waits for broker delivery confirmation.
The optional `config` mapping supplies common client connection/security options.

Core NATS uses a concrete subject without a queue group. Its private asyncio
thread buffers notifications while the session accesses storage. `start()` waits
for the server to process the subscription, and `send()` flushes the publication
to the server. This is live delivery, not persistence. The optional
`connect_options` mapping supplies authentication/TLS and connection options.

Both adapters expose `timeout=10.0` for startup/send operations. `poll(None)`
waits indefinitely, `poll(0)` does not wait, and positive timeouts are in seconds.
Calls belong to the session's owner thread; use the threaded helper for publishing
from other threads. Stop closes the transport; NATS also joins its client thread.
Transport and decoding errors propagate. Core NATS reconnect is disabled and
disconnect/slow-consumer errors fail the session, since notifications may have
been lost. Restart with a new session and fresh cache to rebuild from storage.

Publishers must make tables and metadata readable before sending notifications.
Storage writes and notification sends are separate operations; a failed send
still fails the process rather than attempting recovery.

Broker contract tests run in CI against Kafka and Core NATS. To run them against
your own test brokers (Kafka tests create and delete temporary topics):

```sh
python -m pip install -e '.[kafka,nats]'
HGRAPH_FABRIC_KAFKA=localhost:9092 HGRAPH_FABRIC_NATS=nats://localhost:4222 \
    python -m unittest discover -s tests -p test_notification_brokers.py -v
```

## Design reference

The existing implementation lives in
[hgraph's fabric extension](https://github.com/hhenson/hgraph/tree/db55ea2c271bb39da7d597da14305155c4e82eca/extensions/fabric).
Its [RFC 0026](https://github.com/hhenson/hgraph/blob/db55ea2c271bb39da7d597da14305155c4e82eca/docs/source/rfc/rfc_0026_versioned_dataflow_fabric.rst)
provides the starting semantic reference. These links pin the source consulted
when creating this project; they do not imply that this package implements
the RFC.

This project has no dependency on hgraph. It uses the same distribution and
import names as the existing extension, so use a separate virtual environment.
It is not currently a compatible replacement for that extension.

## Development

Requires Python 3.12 or newer.

```sh
python -m venv .venv
# Activate .venv using your shell's activation command.
python -m pip install -e .
python -c "import hgraph_fabric"
```

To build the source distribution and wheel:

```sh
python -m pip install build
python -m build
```

Contributions should be small and focused. Agree behaviour before expanding
the implementation, and add behavioural tests with each component.

## Licence

MIT, using hgraph's existing licence and copyright notice:
Copyright (c) 2023 Howard Henson. See [LICENSE](LICENSE).

## hgraph adaptor

Install `hgraph-fabric[hgraph]` and import `hgraph_fabric.hgraph_adaptor`.
The adaptor targets hgraph 0.8.35's native runtime. Its schema-typed Arrow table
is named `Frame[Schema]` (the equivalent of `DataFrame[Schema]`). The fabric core
still imports and runs without hgraph installed.

```python
from dataclasses import dataclass
from hgraph import CompoundScalar, graph, register_service
from hgraph_fabric.data_cache import DataCache
from hgraph_fabric.data_fabric_session import DataFabricTransport
from hgraph_fabric.hgraph_adaptor import (
    fabric_service, subscribe, version_id, publish,
)

@dataclass(frozen=True)
class Prices(CompoundScalar):
    price: int

@graph
def copy_prices():
    prices = subscribe[Prices]("prices")
    publish(prices, "prices_copy")

@graph
def app(cache: DataCache, transport: DataFabricTransport):
    register_service("fabric", fabric_service, cache=cache, transport=transport)
    copy_prices()
```

One `@service_impl` serves two subscription interfaces (tables and versions)
and one request-reply interface (publication). The publication interface takes
`TS[Frame]` with typed ID, dependency and original frame-tick-time inputs.
Typed client wrappers convert
frames and dependency lists at the boundary, so different schemas and TSL sizes
share the same implementation and consistent cut. The service uses a push source
to bring worker results into the graph. Cache access, publication, transport
polling and subscription management all run on one worker thread. Graph requests
are queued without waiting for storage or broker IO. Worker failures propagate
through the push source; shutdown joins the worker and closes the transport.

`subscribe[Schema](id) -> TS[Frame[Schema]]` and
`version_id(id) -> TS[VersionId]` accept `TS[str]` or constant IDs.
`VersionId` is `tuple[str, int]`. Clients can live in inline or mapped components;
subscription membership follows the union of table and version requests.
Changing membership rebuilds the core subscription selection while keeping
transport reception active. IDs must exist in storage when requested.
Row schemas are checked using hgraph's `schema_from_frame` field-type mapping.

`publish(df, id, dependencies=None, weak_dependencies=()) -> TS[VersionId]` sends a
request and replies with the persisted version. That acknowledgement does not
mean the version has entered the consistent cut: consumers must subscribe to its
ID for selected data. Both dependency arguments accept a
`TSL[TS[VersionId], SIZE]` or a sequence of version ports, with independent sizes.
Pass `()` explicitly for a source dataset's strong dependencies. All elements
must be valid before publication; a pending table is retained until they are.
A fresh table tick or changed ID publishes a new version; a dependency-only tick
revises the latest version's dependencies. The cache suppresses revisions that
change only `as_of`. Publications use graph time as their revision timestamp.

All clients accept `path="fabric"`. Register separate paths for independent
sessions. Reception starts before loading the initial image. Each cut change
returns matching tables and versions through one push event. Run the graph in
real-time mode. The optional registration parameter `poll_interval=0.05` bounds
the worker's broker wait in seconds, allowing it to service queued requests and
shutdown; it does not schedule polling on the graph thread. Transport polling
must honor that timeout, and storage operations must complete for shutdown to
finish. The worker drains accepted requests before stopping.

Omit `dependencies` to infer them from fabric subscriptions upstream of `df`.
Discovery stops at each subscription; it does not flatten that dataset's stored
dependency tree. A path containing a declared passive input (including
`@compute_node(active=...)` or `passive(...)`) produces a weak dependency.
If any fully active path reaches the same subscription, it is strong. Repeated
version IDs are deduplicated, and an ID present in the strong set is removed
from the weak set, including when dynamic subscription IDs converge.

Hidden strong version signals can publish dependency-only revisions even when
the frame computation suppresses its output. Hidden weak version signals wait
until initially valid and are then sampled on publication inputs; weak-only
updates do not trigger publication. Explicit `weak_dependencies` can supplement
automatic discovery and retain their existing revision-triggering behavior.
Supplying `dependencies` explicitly disables discovery.

Automatic discovery requires subscriptions and publications to use the same
service path, since separate services can have separate caches. For a transfer
between services, declare the dependencies explicitly. A publication cannot
strongly depend on its own dataset ID; this is checked before queueing storage
work, including when an ID changes at runtime.

Inspection supports inline graphs, native and Python nodes, structural inputs,
and bound delayed inputs. Crossing a compiled graph boundary or an unresolved
input raises a wiring error asking for explicit dependencies. Runtime calls to
`make_active()` or `make_passive()` in user code cannot be inferred at wiring time.

The `hgraph` extra currently pins the upstream wiring-inspection commit from
[hgraph PR #1783](https://github.com/hhenson/hgraph/pull/1783). Installation builds
hgraph from source until this API is included in a released wheel; the pin can
then be replaced with that release requirement.
