# Adversarial stabilisation review

Original reviewed implementation: `b50c01d` (the proposed-cut and affected-item
update algorithm). All four reproduced findings are now addressed: joint-history
fallback, commit-time tree validation, dependency refresh with conflict tracking,
and committed-current search bounds.

## Run

After installing the project into the selected Python environment:

```sh
python -I -m unittest discover -s tests
python -I tests/run_adversarial.py
python -I tests/run_adversarial.py --strict
```

The original four review reproductions are ordinary passing regressions. The
[second conflict-tracker review](CONFLICT_TRACKER_REVIEW.md) now covers conflict-path
wakeups and cleanup with passing regressions. Retry-after-error findings were
withdrawn: unexpected failures are fatal. Shared-cache delivery follows the
coordinated contract. Those original findings have no remaining expected failures.

The [deep transition review](DEEP_TRANSITION_REVIEW.md) adds 13,032 publication
steps and bounded pair searches. Its ordering finding is now addressed: the
search visits revisions newest first, returns an exact match immediately, and
retains the newest consistent omission only after ruling out every exact match.
The two reproductions pass, along with 2,916 additional refreshed-history cases.
The [containment transition review](CONTAINMENT_TRANSITION_REVIEW.md) adds changing
topologies and candidate-specific containment cases. Its inconsistent omission
acceptance and missed historical containment matches are now addressed by a
shared candidate comparison in both search paths. The full suite has 161 passing
tests with no expected failures; the strict adversarial runner has 54 passing tests.

## Passing scenario matrix

| Family | Scenarios | What varies |
| --- | ---: | --- |
| Four-node DAGs, subscription subsets and starting heads | 5,120 | All 64 edge subsets in one fixed topological node order; all nonempty root subsets; old/new head per root |
| Four-node DAGs, mixed heads and registration order | 24,576 | 64 edge subsets × 16 head vectors × 24 registration orders |
| Live publication streams | 4,725 | All 315 dependency-before-consumer orders across those DAGs × 15 nonempty subscription subsets |
| Dependency appearance/disappearance histories | 3,072 | 512 reproducible seeds × six registration orders, three roots and three sources, sparse versions and within-version counts |
| Larger graph shapes | 15 | Chain, star, diamond, disconnected and cyclic graphs at sizes 2, 8 and 32 |
| Metadata-only update streams | 12 | Six root delivery orders with and without a subscription to the shared source |
| Monotonic crossed-history paths | 3,600 | Six two-input paths per side, all intermediate proposed ceilings, both argument orders, sparse versions and revision counts |
| **Total** | **41,120** | Scenarios, not 37,520 distinct topology shapes |

The live-stream cases check every update and its duplicate, not just the final
cut. Fatal metadata-error tests check propagation and that no new current
selection is published; they do not require retry or recovery after an error.

`adversarial_cases.py` contains the independent oracle. It walks raw metadata,
scans version-specific revision records and checks pair compatibility without
calling the production graph builder, pair resolver or manager helpers. For
the bounded initial-cut histories it also enumerates all possible cuts and
checks membership in that admissible set. It includes logical leaf roots,
ignores weak dependencies, and honours the agreed root-containment shortcut.
It does not require order-independent answers where several valid cuts exist.

Assertions cover consistency, proposal ceilings, forward-only committed updates,
exact returned DataVersionIds, duplicate delivery, cache-only unrelated-version
updates, unchanged cut when the driving item cannot advance, eventual convergence
in the generated complete histories, and avoiding dataframe loads.

The seeded membership histories advance source versions monotonically whenever
present and record every step of a continuous dependency. Sparse root versions
are 100 and 9000, with revision counts 0 and 1 in each. Weak references deliberately
point at unavailable records to confirm that strong-only expansion is used.

## Review cases and their assumptions

### 1. Crossed input frontiers can reject an existing cut (addressed)

Both roots have an older revision consuming `(x=1, y=1)`. Their latest revisions
consume `a: (x=2, y=1)` and `b: (x=1, y=2)` respectively. A valid cut exists by
selecting both older revisions. The pair search chooses the leading root from
the first conflicting ID and holds the other root fixed; neither one-sided
history search can reach that fixed vector, so initialization raises
`LookupError: No revision aligns with the fixed subscription`.

These are monotonic histories with complete consumed-input changes, but the
application may exclude crossed observations. The fix is defensive: only when
the existing fixed-side search fails, enumerate both actual revision histories
within the proposals, visiting the formerly fixed side newest first. Preserve
original common dependencies before considering omissions. Do not swallow
missing metadata or malformed-history errors. Worst-case work is O(L * R);
normal successful comparisons retain the existing search and cost.

Location: [`consistent_cut.py`](../src/hgraph_fabric/consistent_cut.py#L89).
Reproduction: `test_crossed_input_frontiers_find_the_existing_older_cut`.

### 2. A single-subscription update commits an unloadable tree (addressed)

Start with a valid leaf subscription `a@100`, then update `a@200` to depend on an
unavailable version. With no other subscriptions there are no pair comparisons,
and nothing builds the proposed tree before commit. `apply_update` reports
`a@200` as available; loading its current graph afterwards raises `KeyError`.
The commit path now loads every proposed full tree before changing any current
selection or pruning retained history. This also covers a single subscription
without pair comparisons. Missing metadata and conflicting versions propagate
as errors, preserving the previous current. Tree loadability is required even
when there are no inter-subscription constraints to compare.

Location: [`data_subscription_manager.py`](../src/hgraph_fabric/data_subscription_manager.py#L71).
Reproduction: `test_single_subscription_does_not_commit_unloadable_tree`.

### 3. Retained trees make cut results depend on cache history (addressed)

A retained root revision references `middle@100`; the middle's revision count
then advances from dependencies `x@1` to `x@2` without changing its data version.
A newly expanded copy of that exact root revision follows the middle's latest
count, while the retained tree still contains `x@1`. Adding a peer consuming
`x@2` succeeds with a fresh manager but raises `LookupError` with the warmed one,
despite identical final metadata and identical root revision IDs.

The root remains unchanged while the dependency count advances. Retained graphs
now record the exact dependency revisions used during expansion and refresh when
those version-scoped latest counts change. Current and latest subscription trees
are reverse-indexed by dependency version; only affected trees are refreshed on
a notification. Historical search trees also check freshness on reuse.

When a comparison downgrades a proposal or finds no bounded cut, the manager
tracks versions on the paths to mismatched nodes, respecting root containment.
An unsubscribed dependency update retries resolution only for a tracked conflict
with at least two dependent subscriptions and at least one affected root behind
latest. New data versions, unrelated branches and duplicates do not wake searches.
Entries are removed as waiting roots catch up.

`test_dependency_refresh.py` covers direct and nested conflict paths in every
registration order, skip conditions, index additions/removals, count-only
advancement, blocked-driver fallback, error propagation, historical graph freshness,
and rechecking refreshed current trees at the next resolution. The gate controls
search, not refresh: it can leave root selections unchanged without certifying
that their newly refreshed trees are mutually consistent until resolution runs.

Location: [`data_subscription_graph.py`](../src/hgraph_fabric/data_subscription_graph.py#L60).
Reproduction: `test_same_revision_cut_is_independent_of_retained_tree_cache`.

### 4. An unrelated waiting history can block independent progress (addressed)

Root `a` is an independent leaf. Root `b` has history `100: x@1`, `200: no x`,
`300: x@2`; `c` consumes `x@1`. The committed cut uses `b@200`, while `b@300` is
waiting. Updating `a` from 100 to 200 should permit the witness cut
`{a@200, b@200, c@100}`. Instead the second stage resolves `b/c` to the older exact
match `b@100`, and the lower-bound guard rejects the entire update. `a` never
reverts to its old proposal, yet its independent advancement is blocked.

Update comparisons now supply committed currents as inclusive lower bounds.
The exact-match search, omission fallback, containment handling and joint search
all respect those bounds, including counts within a version. Thus `b@100` is
excluded, `b@200` is retained, and `a@200` commits. Initialization remains
unbounded. A dedicated no-compatible-pair exception rejects an update without
swallowing missing metadata or malformed-history errors.

`test_adversarial_update_bounds.py` covers every registration order with sparse
versions and within-version counts, containment and joint-search bounds, avoiding
loads below the floor, and single-root full-tree validation and failure isolation.

Locations: [`consistent_cut.py`](../src/hgraph_fabric/consistent_cut.py#L135) and
[`data_subscription_manager.py`](../src/hgraph_fabric/data_subscription_manager.py#L85).
Reproduction: `test_unrelated_pending_history_does_not_block_independent_progress`.

## Limits

The matrix is exhaustive only over its stated small graph/history spaces.
It is not a proof for all graphs. The publication streams obey per-ID ordering
and dependency-before-consumer availability; the separate failure cases probe
unavailable metadata and changing descendant revision counts. No concurrency,
transport, namespace routing, durable-storage or dataframe-content behaviour is
claimed. Containment uses the project's agreed rule rather than imposing a
stricter all-descendant comparison.
