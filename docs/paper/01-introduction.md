# Introduction

Many distributed computations communicate through the datasets they produce.
A source publishes a new observation, a derived computation processes it, and
a consumer combines that result with other data. These components may run on
different schedules and take different amounts of time. Their outputs become
available independently, even when the consumer needs to interpret them
together.

Versioning each dataset makes individual publications identifiable, but does
not by itself establish which publications form a coherent view. The latest
version of one dataset may depend on an older version of another. A further
difficulty arises when a computation processes a new input and retains its
existing output: the data has not changed, but the computation has made
progress. Downstream consumers need a way to distinguish this progress from
an input that has not yet been processed.

Consider a sensor publishing a dataset named `reading`, and a classifier
publishing a dataset named `band`. The classifier assigns each reading to
either **below 20** or **at least 20**. A dashboard subscribes to both datasets
so that it can display a reading alongside its classification. Each dataset
can be represented by a single-row table; the same reasoning applies to
larger datasets.

Initially, `reading@100` contains 19.1, and `band@7` contains **below 20**.
The classifier records that it accepted `reading@100` when producing
`band@7`. When the sensor publishes `reading@101`, containing 20.1, the
classifier may still be processing the update. Combining the latest versions
would display 20.1 alongside **below 20**. The dashboard instead retains
`reading@100` with `band@7` until a compatible classification becomes
available. Once the classifier publishes `band@8`, containing **at least 20**
and accepting `reading@101`, the dashboard can advance both selections.

Now suppose the sensor publishes `reading@102`, containing 20.4. The
classifier processes it, but its result remains **at least 20**. The existing
table `band@8` is still suitable. Its version alone, however, cannot tell the
dashboard whether the classifier has processed `reading@102` or is still
working from `reading@101`. The dashboard does not execute the classifier's
logic and cannot infer acceptance merely because the displayed values happen
to agree. Republishing the same table solely to communicate acceptance would
introduce another data version without a new result.

hgraph fabric distinguishes a **data version** from a **dependency revision**.
A version identifies an immutable published table. A revision records the
input versions accepted by the producer while producing or retaining that
table. In this example, the classifier publishes a new revision of `band@8`
that accepts `reading@102`. It leaves the table unchanged. This metadata update
allows the dashboard to advance its reading while retaining the same band.

The sequence is shown below. The notation `band@8/r1` denotes revision 1 of
data version 8. Revision counts start at zero within each data version.
Version numbers are local to their dataset: 102 and 8 are not positions on a
shared clock. The rows describe an illustrative sequence, not a requirement
for a global event order.

| Publication or computation | Latest reading | Latest band record | Reading accepted by that record | Dashboard's coherent selection |
| --- | --- | --- | --- | --- |
| Initial state | `reading@100`: 19.1 | `band@7/r0`: below 20 | `reading@100` | `reading@100`, `band@7` |
| Sensor publishes 20.1 | `reading@101`: 20.1 | `band@7/r0`: below 20 | `reading@100` | `reading@100`, `band@7` |
| Classifier publishes a changed result | `reading@101`: 20.1 | `band@8/r0`: at least 20 | `reading@101` | `reading@101`, `band@8` |
| Sensor publishes 20.4 | `reading@102`: 20.4 | `band@8/r0`: at least 20 | `reading@101` | `reading@101`, `band@8` |
| Classifier acknowledges an unchanged result | `reading@102`: 20.4 | `band@8/r1`: at least 20 | `reading@102` | `reading@102`, `band@8` |

The last transition separates three kinds of progress. The source has
published new data; the classifier has accepted it without publishing a new
table; and the dashboard can now expose a newer coherent view. Only the
reading changes in the dashboard's selected data. The band revision advances
consistency without creating a new band value.

Acceptance is an assertion made by the producing computation. It states that
the retained output is suitable for the recorded inputs; it does not rewrite
the historical fact that the table was originally produced from an earlier
input. The fabric relies on producers recording these assertions correctly.
In this example the reading is a strong dependency, so its version constrains
the dashboard's selection.

The model extends this reasoning to consumers of multiple datasets with
shared, transitive dependencies. Each producer records its immediate accepted
input versions. Consumers resolve those relationships to select a coherent
combination, or *cut*, and retain their existing selections while an update
cannot yet be reconciled. A dependency revision can make a newer cut
admissible even when an intermediate dataset's value remains unchanged.

The resulting coordination is expressed through dataset identities, local
version histories, and dependency acceptance. It does not require comparing
timestamps across producers or assigning all publications to a common clock.
Local timestamps may still be useful for other purposes, but they do not
establish coherence. This separation is the central idea of hgraph fabric:
independently executing computations publish data and acceptance information,
and consumers use that information to advance coherent views.
