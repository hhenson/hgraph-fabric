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
roots and selects their current revisions together. The search compares shared
nodes, including leaf roots, and walks the newer conflicting history backwards
first. It can backtrack to the other history when dependencies appear or disappear.
Version changes follow revision-zero links, so version numbers may have gaps.
If no cut exists, it raises `LookupError` without changing current selections.
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
