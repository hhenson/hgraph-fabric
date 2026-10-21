# Conflict tracker and dependency-update review

Originally reviewed implementation: `87a9f56` (PR #13). Conflict-path wakeups
and cleanup are now covered by passing regressions. The recovery finding is
withdrawn because unexpected exceptions are fatal. The shared-cache finding is
withdrawn because cache writes and all associated notifications are coordinated.

## Results and scope

The original 130 tests pass. The new pack adds a 72-scenario successful-refresh
matrix: direct, nested and diamond paths, all six registration orders, sparse
root versions or revision counts, and dependency advancement or omission. It
checks the returned versions, committed cuts against the raw-record oracle,
reverse-index contents, tracker cleanup and duplicate suppression.

At the completion of this review, the suite contained 140 passing tests with no
expected failures. The later [deep transition review](DEEP_TRANSITION_REVIEW.md)
adds coverage and documents a separate count-search ordering finding. Run after installing the project:

```sh
python -I -m unittest discover -s tests
python -I tests/run_adversarial.py --strict
```

The exploratory generator checked 31,269 deterministic seeds, retaining 9,259
internally valid monotonic histories. Of these, 2,839 satisfied the two-dependent
and one-behind gate. After rejecting invalid refreshed trees, 66 had a newly
possible forward cut. Eight did not advance. These were discovery candidates,
not eight distinct defects: some reflect pair-search preferences. The minimized
single-manager finding below needs neither crossed histories nor an assumption
that the initial cut must be maximal.

## 1. Conflict-path revision tracking and wakeups (addressed)

Use `middle@100 -> y@1` and these root histories (all revision counts zero):

| Root | Version 100 | Version 200 | Version 300 |
| --- | --- | --- | --- |
| a | y@1 | y@2 (latest) | — |
| b | middle@100 (latest) | — | — |
| c | middle@100 | no dependencies | y@3 (latest) |

Register `a, b, c`. Their full trees include `a@200 -> y@2` and
`b@100 -> middle@100 -> y@1`, so comparing a with b downgrades a and records
`middle@100 -> {a, b}`. Comparing c with the downgraded a then downgrades c.
After commit, b is already latest, so the marker becomes `middle@100 -> {a}`.
But middle's actual dependent roots are `{b, c}`. Their intersection is empty.

A new revision of middle removes y. Two subscriptions still depend on middle,
and c is behind latest. The valid forward cut is `{a@200, b@100, c@200}`; the
old cut remains valid too. Previously the update performed no resolution.
Re-delivering c's existing latest notification immediately finds that forward
cut, confirming the pair resolver can produce it when invoked.

The tracker now watches consumers on the paths to a mismatch. For
`A -> Y` versus `B -> M -> Y`, revisions of A, B or M can change the referenced
Y version. Y's own revision cannot change that version mismatch. A and B already
have subscriptions, so M alone needs a soft watch. Traversal includes all paths
through nested and diamond dependencies and skips already-subscribed IDs.

A tracked version's update selects affected waiting roots from the dependency
index, rather than intersecting them with the original compared pair. This
includes c above, while preserving the two-dependent and one-behind gate.

Regression: `test_conflict_tracker_wakes_a_dependent_downgraded_by_an_earlier_comparison`.

## 2. Retry-after-error finding (withdrawn: fatal errors)

The reproductions caught unexpected exceptions and continued processing, then
expected the same notification to retry a failed resolution. That recovery
requirement was never part of the contract. Unexpected exceptions propagate;
the caller terminates processing. No pending-retry machinery is required.

The tests now check propagation of resolver errors and missing dependency
metadata, without attempting recovery.

Regressions: `test_unexpected_resolution_error_propagates` and
`test_missing_dependency_metadata_propagates`.

## 3. P3: Conflict entries retain versions no selected tree references (addressed)

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

`cleanup_conflicts()` now retires unreachable version entries and roots that
have caught up. It runs after completed updates (including those that remain
blocked) and initial cuts, and is also callable explicitly. It preserves active
current-only and latest-only dependencies and markers whose waiting root is
blocked by a peer's dependency. Cleanup does not run midway through searches or
after an operation raises; unexpected errors are fatal.

Regression: `test_retired_versions_are_removed_while_the_roots_are_still_waiting`.

## 4. Shared-cache notification finding (withdrawn: coordinated contract)

The reproduction omitted delivery to one associated manager, then performed an
unrelated cut operation in that manager. The clarified contract excludes that
sequence: cache writes and delivery to every associated manager are coordinated.

The replacement regression applies each update to the shared cache and both
associated managers before proceeding. Both managers mark refreshed current
trees dirty, reject the unrelated advancement while trees disagree, and advance
when the matching peer update arrives. No additional notification mechanism is
introduced; coordination belongs to the existing update-delivery owner.

Regression: `test_coordinated_shared_cache_updates_revalidate_each_manager`.

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
