# Abstract

Distributed computations often produce datasets at different rates and on
independent schedules. Combining their latest outputs can expose an incoherent
view when those outputs depend on different versions of shared inputs.
Furthermore, a computation may accept new inputs without changing its output,
making data versions alone insufficient to describe its progress.

We present **hgraph fabric**, a model in which independently executing
computations communicate through immutable, versioned datasets and explicit
dependency records. The model separates a dataset's value from its acceptance
of input versions: a data version identifies a published value, while a
revision records the input versions accepted by the computation producing or
retaining that value. This allows a computation to acknowledge progress without
republishing unchanged data.

Consumers use these records to select coherent combinations of dataset
versions by reconciling their shared dependencies. Coherence is established
through dependency relationships rather than a globally coordinated clock or
a common publication schedule. Dependency-only revisions can make newer
combinations admissible without introducing new dataset values, while
incremental resolution advances consumer views without rolling back previously
delivered versions.

The model provides a foundation for distributed data-centred computation in
which publication, computational progress, and consumer-visible consistency
can advance independently.
