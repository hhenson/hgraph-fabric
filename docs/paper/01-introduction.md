# Introduction

Risk and batch trading computations bring together datasets produced on
independent schedules. Market data is refreshed, positions are reconciled,
portfolio risk is calculated, and trading processes construct batches of
orders. These computations can take different amounts of time. A new market
snapshot may be available while a risk calculation using the preceding
snapshot is still the latest published result. A downstream trading process
needs to know which of these datasets can be used together.

Versioning each dataset makes individual publications identifiable, but does
not by itself establish which publications form a coherent view. The latest
version of one dataset may depend on an older version of another. A further
difficulty arises when a computation processes a new input and retains its
existing output: the data has not changed, but the computation has made
progress. Downstream consumers need a way to distinguish this progress from
an input that has not yet been processed.

Consider a batch trading process that subscribes to a market-price snapshot
named `market` and a portfolio risk table named `risk`. The risk calculation
consumes the same market dataset. For this example, the risk table reports
market-value exposure for a portfolio holding 100 units of asset A. The market
snapshot also contains asset B, which is not currently held but is a candidate
for the next trading batch. Positions, model parameters, and other inputs
remain fixed throughout the example.

Initially, `market@100` prices A at 100 and B at 50. The corresponding
`risk@7` reports exposure of 10,000 and records that it accepted
`market@100`. The trading process can use these two versions together.
When `market@101` raises A's price to 105, the existing risk table still
reflects the preceding snapshot. Combining the latest publications would
construct orders using the new prices alongside exposure calculated from
older prices. The fabric instead retains `market@100` with `risk@7` for
this consumer. Once the risk calculation publishes `risk@8`, reporting
exposure of 10,500 and accepting `market@101`, the consumer can advance
both selections.

Now suppose `market@102` changes only B's price, from 50 to 51. The risk
calculation processes the new snapshot, but the portfolio still holds only A,
whose price is unchanged. Its exposure remains 10,500, so the existing
`risk@8` table can be retained. The trading process nevertheless needs the
new price of B when constructing its next batch.

The risk table's version alone cannot tell the consumer whether the risk
calculation has accepted `market@102` or has not yet processed it. The
consumer should not need to reproduce the risk calculation to establish that
its result remains applicable. Republishing the same table solely to
communicate acceptance would introduce another data version without a new
risk result.

hgraph fabric distinguishes a **data version** from a **dependency revision**.
A version identifies an immutable published table. A revision records the
input versions accepted by the producer while producing or retaining that
table. Here, the risk calculation publishes a new revision of `risk@8`
that accepts `market@102`, leaving the risk table unchanged. This metadata
update allows the trading process to use the new market snapshot with the
existing risk result.

The sequence is shown below. The notation `risk@8/r1` denotes revision 1 of
data version 8. Revision counts start at zero within each data version.
Version numbers are local to their dataset: 102 and 8 are not positions on a
shared clock. Fixed dependencies, such as positions, are omitted from the
table. The rows describe an illustrative sequence, not a requirement for a
global event order.

| Publication or computation | Latest market snapshot | Latest risk record | Market version accepted by that record | Trading process's coherent selection |
| --- | --- | --- | --- | --- |
| Initial state | `market@100`: A=100, B=50 | `risk@7/r0`: 10,000 | `market@100` | `market@100`, `risk@7` |
| A's price changes | `market@101`: A=105, B=50 | `risk@7/r0`: 10,000 | `market@100` | `market@100`, `risk@7` |
| Risk calculation publishes changed exposure | `market@101`: A=105, B=50 | `risk@8/r0`: 10,500 | `market@101` | `market@101`, `risk@8` |
| Only B's price changes | `market@102`: A=105, B=51 | `risk@8/r0`: 10,500 | `market@101` | `market@101`, `risk@8` |
| Risk calculation acknowledges unchanged exposure | `market@102`: A=105, B=51 | `risk@8/r1`: 10,500 | `market@102` | `market@102`, `risk@8` |

The last transition separates three kinds of progress. The market-data
publisher has supplied new prices; the risk calculation has accepted them
without publishing a new table; and the trading process can now use a newer
coherent view. Only the market dataset changes in the consumer's selected
data. The risk revision advances consistency without creating a new risk
value.

Acceptance is an assertion made by the producing computation. It states that
the retained output is suitable for the recorded inputs; it does not rewrite
the historical fact that the table was originally produced from an earlier
input. The fabric relies on producers recording these assertions correctly.
In this example market data is a strong dependency of the risk calculation,
so its version constrains the consumer's selection.

The model extends this reasoning to consumers of multiple datasets with
shared, transitive dependencies, such as market data, valuations, portfolio
risk, and proposed trades. Each producer records its immediate accepted input
versions. Consumers resolve those relationships to select a coherent
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
