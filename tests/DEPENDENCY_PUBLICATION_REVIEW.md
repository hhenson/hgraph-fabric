# Dependency-only publication: adversarial review

Reviewed production baseline: `3be2a55` (merged PR #29). No additional production
defect was reproduced. This change adds behavioral coverage using the real
publication path; production code is unchanged.

## What was checked

The review followed `publish(data=None, ...)` through cache persistence, send
suppression, wire encoding, tree invalidation, proposed-cut selection, current
bounds, delta reporting and conflict cleanup. Table publications continue to
create versions without comparing table contents. Dependency publications compare
strong and weak tuples, persist a new count only when those tuples differ, and
retain the old timestamp when they are unchanged.

Earlier adversarial packs construct metadata records directly. The new pack
publishes Arrow tables and dependency revisions through `PollingDataFabricSession`
and `MemoryDataCache`, then round-trips notifications through the broker codec.
It exercises two coordinated managers, either sharing the writer's cache or using
a separate reader cache backed by the same storage.

`test_adversarial_dependency_publication.py` adds:

| Family | Cases | Main checks |
| --- | ---: | --- |
| Block, release, weak changes and removal | 24 | All root orders; peer revision versus peer table publication; unrelated progress; as_of-only retries; removal/restoration; stale notifications |
| Soft conflict-path release | 96 | All root orders; direct versus diamond paths; waiting revisions versus data versions; advance versus omission; watch retirement |
| Cold history reconstruction | 4 | New version links the last published count; search selects an interior revision; later peer revision releases a newer table |
| Deterministic live streams | 32 | 48 mixed events, 16 timestamp-only retries and three convergence publications per stream; sparse versions and changing membership |
| Subscribed intermediate | 4 | Containment remains version-based; descendant trees refresh without a table delta |
| Polling across empty deltas | 1 | A blocked count-only notification is consumed before the notification releasing a new peer table |
| Fatal metadata | 4 | Missing dependencies and internally conflicting trees propagate errors before selecting a new current |
| Notifications during initialization | 1 | Publications between root-head reads; both revision notifications retained; duplicate and older messages cannot regress the cut |

The reused independent oracle expands raw stored records rather than production
graphs. Every committed cut is checked for consistency and forward-only movement;
returned version IDs must match exactly the data versions that advanced. It also
checks current/latest dependency indexes, conflict retirement and coordinated
duplicate delivery. The initialization and polling cases check the public table
change sets explicitly.

## Results and limits

The new scenarios pass. A root's revision can remain behind latest while waiting
for a peer; once resolved, revision-only movement produces no new table delta.
A soft revision can release newer subscribed table versions, which are then
reported. Timestamp-only retries produce neither persistence nor notifications.

The agreed policy boundaries remain unchanged: explicit root revisions are pinned,
descendant counts are dynamic, containment compares the contained root's version,
and soft updates only request resolution under the existing conflict gate. A
skipped refresh can leave unchanged selections with dirty trees; that is not
reported as a new cut or treated here as a defect. Unexpected metadata failures
are fatal; there is no retry/recovery or concurrent-writer guarantee.

These are finite, deterministic scenarios, not proof for every possible graph.
Broker delivery itself remains covered by the existing Kafka/Core NATS tests.

Run the new pack with:

```sh
python -I -m unittest discover -s tests -p 'test_adversarial_dependency_publication.py'
```
