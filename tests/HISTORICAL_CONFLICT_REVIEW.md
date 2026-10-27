# Historical conflict-path notification review

Baseline: `1636cbc` (PR #38). Version labels below follow the consecutive
allocation now used by the publication fixture. A dependency notification could
leave a waiting cut unchanged even though repeating the search immediately found a forward cut.
The reproduction uses `DataFabricSession.publish`, persistent records shared
by independent caches, and coordinated notification delivery. No records are
deleted, no current revisions are assigned by the test, and no producer
publishes a decreasing input version.

## Publication sequence

1. Publish X@1 and G@1 accepting X@1. M's producer subscribes to X and G,
   then publishes M@1/r0 accepting X@1 and G@1. N@1/r0 accepts M@1.
   Publish dependency-free A@1 and C@1.
2. Publish X@2 and G@2 accepting X@2. M's input cut advances to X@2/G@2,
   but it has not published another revision yet. B@1 accepts X@2.
3. Publish X@3. M remains at X@2/G@2 because G still requires X@2.
   Restart N with a direct X subscription and publish N@1/r1 accepting X@3.
   A@2 and C@2 each accept N@1.
4. A reader subscribes to A, C and B. N/r1 reaches X@3, while N/r0 through
   M/r0 reaches X@1. Neither agrees with B's X@2. The reader selects
   A@1, C@1 and B@1.
5. M publishes M@1/r1 accepting its actual X@2/G@2 input cut. This changes
   only M's dependency record. N's latest record no longer consumes M, so no
   new N publication is needed or produced.

The coherent forward cut is now A@2, C@2 and B@1, using:

```text
A@2/r0 -> N@1/r0 -> M@1/r1 -> X@2
C@2/r0 -> N@1/r0 -> M@1/r1 -> G@2/r0 -> X@2
B@1/r0 -> X@2
```

These arrows follow dependency discovery from consumer to input. M's record
contains both X@2 and G@2; the two displayed paths abbreviate the shared graph.
N/r0 records M's data version 1, not a pinned M revision count.

## Why watching only the visible paths failed

The baseline watched N@1 from the latest conflict paths. M@1 was absent
from both current and latest expansions: the currents were dependency-free,
and N's latest record used X directly. Although search inspected the older
N/r0 -> M path, the manager discarded that information. It cached M's later
notification without retrying the cut. An explicit retry immediately advanced
A and C, establishing a missed wakeup rather than a search failure.

Watching conflict paths is the right rule, provided it includes paths in
rejected historical candidates. It does not require watching every historical
dependency. The resolver now reports consumer versions on paths that disagreed,
and the manager retains those paths while their root candidates remain within
the current/latest bounds and a recorded subscription is still waiting.

The existing wakeup policy still requires two dependent subscriptions and an
affected waiting root; this example has both A and C. Historical path owners
count alongside indexed current/latest dependents. Changing X's revision
cannot change the X version recorded by a consumer, so the mismatched endpoint
does not acquire a soft watch merely because of this conflict.

## Retirement and regression coverage

Root candidates below current and caught-up waiters are retired. A successor's
revision-zero link records a predecessor snapshot, not its final count. Older
versions remain revisable, so publishing a successor does not retire a still
admissible historical watch. Duplicate or older notifications do not retry an
unchanged historical watch.

Eligibility uses the dependent subscriptions recorded before processing the
notification. Resolution uses the updated records; watch retirement follows
the attempt. The baseline already copied current/latest dependents before
refreshing indexes. The fix makes that ordering explicit for historical path
owners too. A regression delivers a successor before the predecessor's delayed
revision that removes its dependencies: both historical owners must count for
that notification, even though their watches can retire immediately
afterwards. Another regression removes both paths to X through N; eligibility
uses the watched consumer N, not the post-update count at the endpoint X.

`test_historical_conflict_watches.py` covers all six registration orders,
data-version and revision-count advances, M catching up or removing its inputs,
historical paths omitted by both endpoint trees, retirement, delayed
notifications and new revisions after a successor, pre-update eligibility when dependencies disappear, and repeated
blocked attempts. The shared-cache transition oracle also validates historical
watch reachability against raw records.

```sh
python -I -m unittest discover -s tests -p test_historical_conflict_watches.py
```
