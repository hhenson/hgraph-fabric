# Containment transitions: adversarial review

Originally reviewed production tree: `b91f385` (PR #19, including the newest-first
bounded history search). The finding is now addressed; all five reproductions
are ordinary passing regressions. The examples below describe the original defect.

## P1: historical candidates retain the initial containment constraints (addressed)

`consistent_cut.py:96-102` reduces the fixed comparison to a contained root when
that root's proposal is behind the consuming tree. The one-sided history loop
at lines 140-153 retains that comparison even when an older candidate no longer
contains the root. It then accepts a candidate that omits the root without
checking its other shared dependencies. This can publish an inconsistent cut.

A complete, monotonic example, with every record available and all counts zero:

| Subscription | Revision | Strong dependencies |
| --- | ---: | --- |
| A | 100 | X@1, Y@1 |
| A | 9000 (latest) | X@3, Y@2 |
| B | 100 | X@1 |
| B | 200 | X@2 |
| B | 9000 (latest) | A@9000 |
| C | 100 (latest) | Y@1 |

Register A, C, B and request the initial cut:

1. A/C lowers A to 100 to agree on Y@1.
2. A@100/B@9000 compares the contained A root. B consumes A@9000, so search B's
   history for A@100 or a compatible omission.
3. B@200 omits A. The search accepts it, although it consumes X@2 while A@100
   consumes X@1. The root-only shortcut no longer applies to this candidate.
4. C/B share nothing, so the manager commits A@100, C@100, B@200.

The valid witness is A@100, C@100, B@100. B's expanded X progression is 1, 2, 3;
there are no decreasing published versions or crossed input frontiers. A and C
really do overlap on Y. The defect does not depend on dynamic dependency
refresh, soft-update gating, missing metadata, error recovery, or notification
ordering.

The public manager reproduction covers initial cuts, `apply_update` before any
cut, adding B followed by `get_consistent_cut`, and adding B followed by its
update. Four of 16 entry-path/registration-order combinations commit the invalid
cut. Other orders happen to recheck X and avoid it. A pair-level floor excluding
B@100 also returns incompatible B@200 instead of rejecting the bounded search.
No claim is made that every ordinary update of an established subscription
reproduces this specific sequence.

### Reverse transition: a valid containment match can be skipped

The same loop can retain full descendant constraints when an older candidate
gains containment:

| Subscription | Revision | Strong dependencies |
| --- | --- | --- |
| A | 100/count 0 (proposed) | X@1 |
| A | 100/count 1 (latest) | X@2 |
| B | 100/count 0 | none |
| B | 200/count 0 | A@100 |
| B | 9000/count 0 | A@100 |
| B | 50000/count 0 (proposed) | X@3 |

A's explicit proposal pins count zero; expanding B's A@100 dependency uses
count one. The agreed containment rule makes A@100/count 0 and B@9000 compatible:
only the contained root's **data version** must match. But the one-sided search
continues comparing X, rejects B@9000 and B@200, then selects the older B@100
omission. This violates exact-match precedence. Both argument orders and a
within-version-count encoding reproduce it. Excluding the omission via a lower
bound reaches joint fallback, which already re-evaluates containment and returns
the correct newest candidate. That adjacent case is a passing regression.

### Implemented correction

Both search paths now use `_compare_candidates` to classify the candidate pair
as a conflict, exact match, or compatible omission. The normal search retains the
full fixed tree: if containment disappears, it compares all shared nodes; if
containment appears, it applies the root-version shortcut. A matching contained
root is exact even when an originally shared descendant has disappeared.

Newest-first traversal, current floors, immediate exact-match resolution and
deferred newest **consistent** omission are preserved. No manager-wide validation
pass is added. The five reproductions pass, and two additional tests cover a
removed descendant in normal and joint search.

## Added coverage

`test_adversarial_containment_transitions.py` uses the independent raw-record
oracle in `adversarial_cases.py`; it does not derive candidate validity from the
production resolver. All generated trees are internally coherent. Matrix tests
collect every mismatch before failing, so a known failure does not stop the rest
of the matrix from running.

| Family | Cases | Observed result |
| --- | ---: | --- |
| All transitions between three-node forward DAGs | 3,072 initial cuts | Pass: 8 old topologies, 8 new, 8 root-head vectors, 6 registration orders |
| Changing-topology publication streams | 1,176 streams / 3,528 updates | Pass: all nonempty root subsets and all dependency-before-consumer publication orders; safe cuts, forward currents, exact advancement sets and final convergence |
| Containment membership catalog | 1,856 bounded pair comparisons | All pass: 116 monotonic four-record histories, sparse versions or counts, four floors, both argument orders; formerly 504 omission/no-solution failures |
| Minimal lost-containment pair and excluded witness | 2 | Both pass: reject the conflicting omission and respect the floor |
| Manager entry paths and registration orders | 16 | All pass; formerly 4 inconsistent commits |
| Gaining historical containment | 8 | All pass, including floors excluding the omission |
| New containment removes an original shared descendant | 6 | Pass: four normal-search cases across sparse versions/counts and argument orders, plus two joint-search cases |

The catalog allows absence, direct X@1/2/3, and containment of A@100 or A@9000.
Present expanded X versions and consumed A versions are nondecreasing, while
membership may disappear and reappear. Exact containment outranks an omission;
without an exact match the oracle chooses the newest compatible omission inside
the bounds, or requires `NoConsistentCutError` if none exists.

Existing packs continue to exercise deep chains/diamonds, conflict-path changes,
coordinated shared managers, latest-count refresh, tracker retirement, dirty-tree
validation, sparse links, weak dependencies, fatal metadata errors and repeated
block/release cycles. No additional failure was observed in those tests.

## Validation and limits

- Built the wheel and source distribution and tested the installed wheel locally
  on Python 3.12 and 3.14: **161 passing tests, no expected failures**.
- Strict adversarial run: **54 passing tests**.

```sh
python -I -m unittest discover -s tests
python -I tests/run_adversarial.py --strict
```

These are bounded exhaustive matrices and retained reproductions, not proof of
all possible graphs or histories. The tests honor coordinated cache/manager
delivery, fatal unexpected exceptions and the agreed soft-update gate. They do
not impose recovery, concurrency, global maximality of every manager cut, or
stronger descendant agreement while root containment applies.
