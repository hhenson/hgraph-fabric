# hgraph-fabric

A standalone, open-source project for developing the hgraph versioned
dataflow fabric.

The concept connects independent computations through complete, versioned
datasets. Lineage records describe which input versions a computation has
accepted, including when its output is unchanged.

## Status

The package currently provides identity aliases, frozen revision metadata,
a lazy data cache with abstract storage loaders, and a directed dependency
edge graph with version checks and node queries. The subscription graph
builds and retains full strong-dependency graphs breadth first, using the cache's
latest-revision lookup for each specific dependency version. Latest/current
revision selection is explicit; weak dependencies are excluded.
`DataSubscriptionManager(cache)` registers roots with `add_subscription(data_id)`
or `add_subscriptions(data_ids)`, loading their latest trees. Its
`get_consistent_cut()` returns a `set[DataVersionId]` for compatible subscribed
roots. It initializes every current selection to latest, runs round-robin
comparisons using `find_consistent_cut`, and applies each returned revision pair.
When a selection changes, its other comparisons are queued again, so later
adjustments are checked against earlier pairs. A failed comparison restores
the pre-tournament selections.

`apply_update(update)` always updates the cache. Only an update for a subscribed
data ID changes that subscription's latest revision and triggers recalculation.
With an established cut, it tries to advance subscriptions currently behind
latest and compares only affected pairs. Waiting peers can advance together;
if a proposal would regress any selection, the previous cut is retained.
Before an initial cut exists, it runs the full tournament.
It returns the `set[DataVersionId]` of subscriptions whose current data version
advanced, including newly selected subscriptions and waiting peers. Revision
count changes alone do not indicate new data and are excluded.

The pairwise search keeps the lagging subscription fixed, follows revision-zero
links across sparse versions, and binary-searches counts within one version.
If one subscription's root occurs in the other tree, only that root version
needs to match; otherwise all common nodes are checked. Subscriptions record
dependency changes while those dependencies remain present. Exact matches
take priority over missing-dependency candidates.
Concrete storage backends and end-to-end fabric execution are not implemented.

The cache's `latest(data_id)` returns a revision ID. Historical version lookup
starts there and follows the `(version, revision_count)` pair in
`previous_revision` from each version's revision-0 record, retaining the same
data ID. That pair identifies the previous version and its maximum revision;
`None` marks the end of the chain.

We will build the components incrementally in Python to establish the structure
and semantics. A later port to Rust or C++ remains an open decision. Integration
with hgraph will follow once the standalone components are established.

The initial implementation milestone is a working Python core with in-memory
storage. Durable storage adapters, Kafka and hgraph integration come later.
Individual components and their APIs will be agreed as we build them.

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
