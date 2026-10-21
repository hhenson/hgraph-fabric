# Mixed subscription/dependency roles: adversarial review

Reviewed production tree: `2810cc6` (PR #21, including candidate-specific
containment). No new defect was reproduced. This change adds tests and review
documentation; production is unchanged.

## Review focus

The preceding containment matrices exercise explicit, small histories. This
review combines those transitions with live updates: a node may be both a
subscription and another subscription's dependency, while paths appear,
disappear, branch, reconnect, or stop containing a subscribed root.

The code review covered candidate classification in normal/joint search,
inclusive current bounds, rechecking incident pairs after proposal downgrades,
dirty-peer insertion during updates, dependency indexes, conflict retirement,
and adding a subscription after a cut has already been committed. No additional
implementation change was justified by that inspection or the executions below.

## Added test pack

`test_adversarial_mixed_roles.py` adds:

| Family | Cases | Checks |
| --- | ---: | --- |
| Live forests with mixed roles | 128 streams / 8,960 updates | Six nodes, two to six subscriptions, changing chains and containment, sparse data versions and revision counts, membership removal/restoration, weak missing references |
| Live branching DAGs with mixed roles | 64 streams / 4,480 updates | Multiple dependency paths, shared descendants, count refreshes, dependency and root updates, differing subscription subsets |
| Exhaustive bounded pair validation | 3,072 comparisons | 24 chosen bound pairs per branching stream in both argument orders; every possible pair inside each selected interval is enumerated from raw records |
| Promotion of a tracked intermediate into a subscription | 24 cases | Six root registration orders, single/bulk registration, repeated IDs, immediate release or a blocked update before release; old committed floors and new-subscription advancement reporting |

Each stream has 64 generated events and six final dependency-before-consumer
events that must converge to all subscribed latest revisions. Every event also
gets an immediate duplicate notification. Half the streams coordinate delivery
to two managers sharing a cache. Node names are shuffled independently of their
topological positions, so lexical mismatch selection is not tied to graph depth.

The shared `Stream` assertions independently expand raw records and verify:

- Every newly committed cut is pairwise consistent and forward-only.
- Returned data-version IDs describe exactly the roots whose data versions
  advanced; count-only moves do not report new dataframe versions.
- Selected current/latest trees and reverse dependency indexes match the raw
  history, and conflict entries reference live paths and waiting roots.
- Cleanup is idempotent; complete final frontiers converge and retire conflicts.
- Coordinated shared-cache delivery and immediate duplicate handling preserve
  the agreed behavior. A partial root advance may advance further on a duplicate;
  a soft duplicate does not initiate another cut search.

The bounded-pair oracle never calls production graph construction or comparison
helpers. It enumerates raw-history cuts below the chosen ceilings and at or above
the floors. A returned pair must belong to that admissible set; rejection requires
the set to be empty. Latest/current/proposed selections must remain unchanged.
The pack asserts it actually visits containment, ordinary overlap and disjoint
pairs, and both successful and unsatisfiable bounded searches. The reverse call
reuses the first call's trees and cache.

## Viable-history restrictions

All graphs have a fixed topological node order, so no generated dependency cycle
is involved. Direct dependencies always consume an available latest data version;
present versions never decrease. Counts advance within a version, and new
versions are sparse and linked through their count-zero metadata.

A branching count update can make different paths inside an old root disagree
on a data version. The generator checks **every retained raw revision** before
delivering a candidate update. Such internally conflicting input is discarded
before any cache or manager sees it. The filter uses only raw graph coherence,
never whether the resolver succeeds or what cut it selects. Failure to find a
coherent publication within the bounded generation attempts fails the test.
Fatal malformed/missing metadata remains covered by the existing error tests.

Soft updates still obey the agreed gate. An unchanged selection can have dirty
trees after resolution is deliberately skipped; the tests require consistency
whenever a new cut commits. No uncoordinated cache updates, recovery after fatal
errors, or globally maximal manager-cut guarantee is assumed.

## Validation and limits

Built the wheel and source distribution, then tested the installed wheel:

- Python 3.12 and 3.14: **164 tests pass, no expected failures**.
- Strict adversarial runner: **57 tests pass**.

```sh
python -I -m unittest discover -s tests
python -I tests/run_adversarial.py --strict
```

Separate local exploration also passed 512 forest streams and 256 branching
streams, each with 64 events, plus 6,144 bounded comparisons. Those exploratory
runs are separate from the retained, shuffled-name matrices above.

The pair oracle is exhaustive within each chosen finite interval. The stream
families are deterministic generated samples, not an enumeration of every DAG or
history. Together with earlier packs they extend coverage of viable interactions;
they do not prove correctness for all possible scenarios, concurrency, storage,
transport, or dataframe contents.
