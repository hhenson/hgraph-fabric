# Historical conflict-path notification review

Baseline: `1636cbc` (PR #38). A dependency notification could leave a waiting cut
unchanged even though repeating the search immediately found a forward cut.
The reproduction uses `DataFabricSession.publish`, persistent records shared
by independent caches, and coordinated notification delivery. No records are
deleted, no current revisions are assigned by the test, and no producer
publishes a decreasing input version.

## Publication sequence

1. Publish X@3 and G@100 accepting X@3. M's producer subscribes to X and G,
   then publishes M@100/r0 accepting X@3 and G@100. N@100/r0 accepts M@100.
   Publish dependency-free A@100 and C@100.
2. Publish X@7 and G@200 accepting X@7. M's input cut advances to X@7/G@200,
   but it has not published another revision yet. B@400 accepts X@7.
3. Publish X@9. M remains at X@7/G@200 because G still requires X@7.
   Restart N with a direct X subscription and publish N@100/r1 accepting X@9.
   A@200 and C@200 each accept N@100.
4. A reader subscribes to A, C and B. N/r1 reaches X@9, while N/r0 through
   M/r0 reaches X@3. Neither agrees with B's X@7. The reader selects
   A@100, C@100 and B@400.
5. M publishes M@100/r1 accepting its actual X@7/G@200 input cut. This changes
   only M's dependency record. N's latest record no longer consumes M, so no
   new N publication is needed or produced.

The coherent forward cut is now A@200, C@200 and B@400, using:

```text
A@200/r0 -> N@100/r0 -> M@100/r1 -> X@7
C@200/r0 -> N@100/r0 -> M@100/r1 -> G@200/r0 -> X@7
B@400/r0 -> X@7
```

These arrows follow dependency discovery from consumer to input. M's record
contains both X@7 and G@200; the two displayed paths abbreviate the shared graph.
N/r0 records M's data version 100, not a pinned M revision count.

## Why watching only the visible paths failed

The baseline watched N@100 from the latest conflict paths. M@100 was absent
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

Root candidates below current are retired. A cached successor's revision-zero
link also proves the final revision count of its predecessor version. Once that
count has been observed, a historical watch for that version can retire. If the
final notification is delayed, its watch remains until that count arrives.
Duplicate or older notifications do not retry an unchanged historical watch.

Eligibility uses the dependent subscriptions recorded before processing the
notification. Resolution uses the updated records; watch retirement follows
the attempt. The baseline already copied current/latest dependents before
refreshing indexes. The fix makes that ordering explicit for historical path
owners too. A regression delivers a successor before the predecessor's final
revision that removes its dependencies: both historical owners must count for
that final notification, even though their watches can retire immediately
afterwards. Another regression removes both paths to X through N; eligibility
uses the watched consumer N, not the post-update count at the endpoint X.

`test_historical_conflict_watches.py` covers all six registration orders,
data-version and revision-count advances, M catching up or removing its inputs,
historical paths omitted by both endpoint trees, retirement, delayed final
notifications, pre-update eligibility when dependencies disappear, and repeated
blocked attempts. The shared-cache transition oracle also validates historical
watch reachability against raw records.

```sh
python -I -m unittest discover -s tests -p test_historical_conflict_watches.py
```
