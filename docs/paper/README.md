# Revisioned Dataflow paper draft

Working statement:

> A model for distributed, data-centred computation that maintains coherent
> views of independently versioned datasets without requiring a globally
> coordinated clock.

**Revisioned Dataflow** names the computational model described by this paper.
The **hgraph-fabric** package provides an implementation of that model. The
model's definitions and semantics are independent of the implementation's
runtime, storage, and transport choices.

The paper is being developed as a sequence of Markdown sections. These files
are the working manuscript; a more formal publication format will follow.

1. [Abstract](00-abstract.md)
2. [Introduction](01-introduction.md)

The abstract captures the agreed framing. The introduction motivates the model
with a desired-positions, trading-universe, validation, and order workflow.
Repeated workflow diagrams show the information available in each scenario
before introducing the identities and revision records needed to reconcile it.
Formal guarantees, comparison with related work, and evaluation remain to be
developed; the draft does not claim that those results have already been
established.
