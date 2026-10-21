# Conflict tracker and dependency-update review

Reviewed implementation: `87a9f56` (PR #13). This review adds tests and findings;
it does not change production behavior. Three gaps reproduce with a single
manager. A fourth finding is conditional on writes to a shared cache outside
the owning manager.

## Results and scope

The original 130 tests pass. The new pack adds a 72-scenario successful-refresh
matrix: direct, nested and diamond paths, all six registration orders, sparse
root versions or revision counts, and dependency advancement or omission. It
checks the returned versions, committed cuts against the raw-record oracle,
reverse-index contents, tracker cleanup and duplicate suppression.

Five expected-failure tests cover the three single-manager findings (two retry
variants) and the conditional shared-cache finding. They assert the desired
behavior, not the current defect. Run after installing the project:

```sh
python -I -m unittest discover -s tests
python -I tests/run_adversarial.py --strict
```

The normal suite reports 137 tests: 132 pass, five explicitly expected failures.
The strict adversarial runner reports those five as failures. A green normal
run therefore does not mean these findings are fixed.

The exploratory generator checked 31,269 deterministic seeds, retaining 9,259
internally valid monotonic histories. Of these, 2,839 satisfied the two-dependent
and one-behind gate. After rejecting invalid refreshed trees, 66 had a newly
possible forward cut. Eight did not advance. These were discovery candidates,
not eight distinct defects: some reflect pair-search preferences. The minimized
single-manager finding below needs neither crossed histories nor an assumption
that the initial cut must be maximal.

## 1. P2: Chained downgrades can leave the relevant dependent out of the tracker

Locations: `data_subscription_manager.py:114-136` records the original pair's
conflict paths; `:60-62` intersects recorded roots with current dependents.

Use `middle@100 -> y@1` and these root histories (all revision counts zero):

| Root | Version 100 | Version 200 | Version 300 |
| --- | --- | --- | --- |
| a | y@1 | y@2 (latest) | — |
| b | middle@100, x@2 (latest) | — | — |
| c | middle@100 | x@2 | y@3 (latest) |

Register `a, b, c`. Comparing a with b downgrades a and records
`middle@100 -> {a, b}`. Comparing c with the downgraded a then downgrades c.
After commit, b is already latest, so the marker becomes `middle@100 -> {a}`.
But middle's actual dependent roots are `{b, c}`. Their intersection is empty.

A new revision of middle removes y. Two subscriptions still depend on middle,
and c is behind latest. The valid forward cut is `{a@200, b@100, c@200}`; the
old cut remains valid too. Nevertheless the update performs no resolution.
Re-delivering c's existing latest notification immediately finds that forward
cut, confirming the pair resolver can produce it when invoked.

Proposed direction: once a version has a conflict marker, consider its current
waiting dependents rather than intersecting them with only the original pair.
Alternatively, propagate conflict causes through later downgrades. The original
pair alone does not identify every affected waiting root.

Regression: `test_conflict_tracker_wakes_a_dependent_downgraded_by_an_earlier_comparison`.

## 2. P2: Tree freshness is mistaken for completion of an update attempt

Location: `data_subscription_manager.py:45-59` uses whether any tree was
invalidated to decide whether a dependency notification merits resolution.

Two roots wait on middle@100, which changes from x@1 to x@2 and would release
both. After both trees refresh, inject a transient exception at the comparison
boundary. No current selection changes. Retrying the same notification returns
an empty set without attempting resolution: the trees are already fresh, so
`affected` is empty. Re-delivering a root notification succeeds.

A second reproduction uses no injected resolver failure. Let middle's update
reference temporarily unavailable metadata. After the missing record arrives,
read both subscriptions' latest graphs through their public accessors. These
reads repair the graphs but do not resolve a cut. Retrying middle's notification
is then suppressed for the same reason. Those reads can also leave the manager's
dependency index behind the freshly built graphs.

Proposed direction: retain a pending dependency-resolution obligation until an
attempt completes (whether it advances or is validly blocked). Graph freshness
and event completion need separate state; a read must not consume a retry.

Regressions: `test_duplicate_retries_resolution_that_failed_after_refresh_completed`
and `test_reading_repaired_trees_does_not_consume_a_pending_retry`.

## 3. P3: Conflict entries retain versions no selected tree references

Locations: `data_subscription_manager.py:105-109` removes unused reverse-index
entries without retiring their conflict markers; `:166-170` trims only roots
that reach latest.

Keep a and b blocked by a peer consuming x@2. Repeatedly publish newer middle
versions that still consume x@1, then publish new latest a/b versions referencing
that new middle version. Their current trees remain dependency-free. After ten
such rounds, the dependency index contains only middle@1100, but the conflict
tracker retains all eleven middle versions from 100 through 1100.

Those retired entries cannot wake anything because the dependency-index lookup
has no dependents for them. The retained state grows with rejected publications
until the roots eventually catch up, even though the active dependency set is
constant. This is a storage/maintenance gap, not an inconsistent-cut claim.

Proposed direction: retire unreachable version entries and reconcile conflict
markers with the active current/latest dependency index as it changes.

Regression: `test_retired_versions_are_removed_while_the_roots_are_still_waiting`.

## 4. Conditional P1: Shared-cache writes can bypass dirty-tree validation

Locations: `data_subscription_graph.py:79-84` refreshes on lazy access;
`data_subscription_manager.py:240-243` assumes unchanged, non-dirty peers still
agree. Dirty flags are set only by that manager's own update handler.

Manager M has current `a -> middle@100 -> x@1`, `b -> x@1`, and an independent
leaf. Another manager sharing its DataCache applies `middle@100` count 1, now
consuming x@2. M next receives an update for the independent leaf. Pair
comparisons lazily refresh a's tree, but M never marks a dirty. Both a and b
remain in the assumed stable set. M commits the independent advancement even
though a now consumes x@2 and b consumes x@1.

This requires cache mutation outside M's update handler. It does not reproduce
when every relevant update is routed through M before it performs another
resolution. The graph accessor explicitly supports freshness after external
cache updates, but the manager currently lacks the matching invalidation signal.

Proposed direction: either notify every owning manager of cache changes, detect
changed tree generations before trusting the stable set, or explicitly restrict
the cache ownership/update-routing contract. Do not call shared-cache operation
safe without one of those rules.

Regression: `test_shared_cache_refresh_cannot_certify_an_inconsistent_cut`.

## Agreed policy boundaries, not additional implementation findings

An update with fewer than two dependent subscriptions, or with all dependent
subscriptions already latest, intentionally skips resolution. This can leave
unchanged root selections whose refreshed trees no longer agree until a later
resolution. The pack preserves this gate in a passing test, including the case
where only a non-dependent peer is behind. It is not counted as a defect above.

Same-version dependency counts remain dynamic, explicit root revisions stay
pinned, weak dependencies stay excluded, versions remain sparse, and dependency
membership may disappear/reappear. The root-containment shortcut is honored.
The tests assume ordered per-ID delivery except exact retries; they do not
claim concurrency, durable transaction, transport or dataframe validation.
