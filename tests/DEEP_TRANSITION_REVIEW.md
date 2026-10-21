# Deep dependency transitions: adversarial review

Originally reviewed production tree: `0bfbe78` (merged PR #17). The ordering
finding below is now addressed, and both reproductions are passing regressions.

## Finding: refreshed descendants invalidate count-search ordering (addressed)

The reviewed `consistent_cut.py:173-178` used the first/last expanded dependency versions to
exclude an entire data version. Lines 147-151 used a middle expanded version to
exclude half of its revision counts. Both assume the expanded dependency values
remain monotonic across a root's historical revisions.

That assumption need not survive the agreed rule of expanding dependencies
using their latest revision within the referenced data version.

Example, before the intermediate update:

| Root revision | Direct dependencies | Expanded X version |
| --- | --- | ---: |
| A@100, count 0 | none | absent |
| A@200, count 0 | M@100, which consumes X@1 | 1 |
| A@200, count 1 | X@3 | 3 |
| A@200, count 2 (latest) | X@5 | 5 |
| B@100, count 0 (latest) | X@3 | 3 |

The initial cut correctly selects A@200/count 1 and B@100/count 0. M then
publishes count 1 consuming X@7. A no longer consumes M in its current or latest
revision, but expanding A's older count 0 must still use M's latest count.
A@200's expanded X history becomes **[7, 3, 5]**.

Initialize a fresh manager after coordinated delivery of that M update. Seeking
B's X@3, the count-zero check sees 7 > 3 and skips all of A@200. The omission
fallback returns A@100, despite the unchanged exact match at A@200/count 1.
The returned cut is consistent; the defect is unnecessary downgrade and violation
of the specified preference for an available exact match over an omission.

The second reproduction starts with **[1, 2, 3, 4, 5, 6, 9]**, with count 3
consuming M rather than X directly. Updating M from X@4 to X@7 yields
**[1, 2, 3, 7, 5, 6, 9]**. Seeking X@6, the endpoints still bracket the target,
but the middle probe excludes the half containing the exact match at count 5.
Fixing only the count-zero shortcut would therefore leave the same gap.

These examples require no missing records, failure recovery, uncoordinated
notifications, decreasing published dependency versions, or crossed input
frontiers. M advances its X version; A changes from an indirect path to a direct
one. It is the *refreshed expansion of historical records* that loses monotonicity.
The reproductions verify the exact candidate with the independent raw-record
oracle and verify that the selected omission is itself consistent.

The search now visits the leading history newest first within the existing
current/proposed bounds. It returns immediately on an exact match, since every
newer candidate has already been checked. It remembers the first consistent
omission, continues searching, and uses that omission only if no exact match
exists in the bounded history. Every candidate still checks other common nodes.
Sparse versions are reached through revision-zero links as before.

This removes binary pruning of expanded values. Worst-case one-sided search is
linear in the number of admissible revisions; already-compatible proposals and
direct containment resolution retain their fast paths. Tests now verify that
an exact match stops further historical reads, instead of asserting a logarithmic
probe count that requires an ordering guarantee the model does not provide.

Reproductions in `test_adversarial_transitions.py`:

- `test_refreshed_historical_path_does_not_hide_an_exact_count_match`
- `test_refreshed_middle_probe_does_not_prune_the_matching_half`

## Added scenario pack

All stream tests publish through cache/manager APIs, with no manual current,
latest or proposed selection changes. Coordinated shared-cache deliveries finish
before inspecting trees. The pair-search test sets proposals and bounds directly
because those are inputs to that public operation.

| Family | Cases | Transitions and checks |
| --- | ---: | --- |
| Repeated block/release/removal/restoration | 144 histories | Chain and repeated-diamond paths, depths 1/4/12, sparse root versions or count-only updates, six registration orders, one or two managers; 32 publication steps each |
| Seeded multi-source streams | 129 histories | Four roots, three sources, 60 mixed source/intermediate/root events plus four convergence events; membership changes, weak missing references, count and data-version updates, shuffled registration |
| Conflict-path replacement | 12 histories | Introduce M -> N -> X while blocked, acquire N watch, release via N, remove N, verify later N revisions do not trigger search |
| Sparse intermediate versions | 2 histories | Six version rotations through 90000001, 48 steps each; pinned old trees, new conflicts, count updates, retirement, fresh-cache previous-version/max-count walks |
| Different roles across shared managers | 6 delivery orders | Same item is a root in one manager and a dependency in two others; count-only root advancement, soft wakeup, one-dependent gate, later dirty-tree validation |
| Long bounded count search | 1,296 comparisons | 73 counts across three sparse versions, four-level paths, four absence patterns, nine targets, three ceilings, nine floor positions, both argument orders; exact-then-absence result checked by exhaustive enumeration |
| Refreshed non-monotonic histories | 2,916 comparisons | All 243 five-count patterns of match/lead/omission produced by intermediate refreshes, six floors and both argument orders; newest exact wins, omission is deferred, reads stop at the exact match |
| Fatal deep metadata errors | 4 cases | Missing dependency or internally conflicting versions at depth 12; chain/diamond; errors propagate and no current selection is published |

There are **13,032 successful publication steps**, plus immediate duplicate
notifications, 4,212 pair comparisons, four fatal-update cases, and the two
now-passing reproductions. Shared-manager cases deliver each publication to
every associated manager.

Every newly committed cut is checked for pairwise consistency by the independent
raw-record oracle, forward-only revisions, proposal ceilings, and exactly the
reported data-version advances. Current/latest expanded nodes, reverse indexes,
conflict retirement and cleanup idempotence are also checked. A complete final
frontier must converge. Payload loading is forbidden by the test cache.

## Contract distinctions

- The agreed soft-update gate can leave unchanged selections with dirty trees.
  Tests require consistency whenever a new cut commits; they do not redefine a
  skipped search as an error.
- Unexpected exceptions are fatal. The tests do not require recovery/replay after
  an error. Immediate successful-event duplicates are tested separately.
- A dependency duplicate cannot trigger another search. A subscribed-root
  duplicate after partial advancement can search again with tighter current
  bounds and advance further. Exploration seed 2007 exercised this behavior;
  both cuts were valid and forward-only. It is not reported as a defect or used
  to impose a globally maximal first cut. That seed is retained in the pack.
- Containment comparisons honor the agreed root-only rule. No stronger
  all-descendant consistency contract is imposed.

## Validation and limits

An additional local exploration ran the seeded stream family for seeds 0-2047
(131,072 publication steps plus duplicates). Safety and convergence checks passed;
seed 2007's partial-root-duplicate behavior is retained in the regular pack. This
larger exploration is separate from the default CI matrix above.

The full suite has **150 passing tests with no expected failures**. The strict
adversarial runner has 43 passing tests. Run against the installed wheel:

```sh
python -I -m unittest discover -s tests
python -I tests/run_adversarial.py --strict
```

The added matrices cover their stated spaces, not every graph or history. The
seeded streams check safe results and eventual convergence to a common final
frontier; they do not prove globally maximal cuts. No concurrency, transport,
durable-storage or dataframe-content behavior is claimed.
