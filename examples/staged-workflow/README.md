# Synthetic staged-workflow adapter

This pure application executable consumes `contracts.WorkflowAdapterInput` on stdin
and emits exactly one bounded `WorkflowAdapterOutput` on stdout. Stderr is bounded
operational logging. It submits nothing and inherits no controller credentials.

The controller stages only the named canonical accepted decision and immutable
settings into a private working directory. Decision JSON is an explicit
`{"continue": true}` or `{"continue": false}`. Settings JSON contains `next`, a
`contracts.WorkflowNext` (a complete new sealed submission and its next decision
unit/path). A true decision without a next graph is an operational error, not a
prescribed stop. A false decision returns a prescribed application stop.

Settings own the numerical meanings and identities of the next graph. To reuse
accepted outputs or preparation, its unit inputs and package preparation refer to
those existing artifacts; do not include earlier accepted units in the next graph.
This example demonstrates deterministic selection only, not scientific branching,
a second scheduler, terminal/unavailable handling, or platform session rollover.

The controller API implementation is separate from startup, HTTP and CLI wiring.
An operator must explicitly approve the adapter contract, executable digest and
absolute argv. The controller verifies and executes a protected private copy;
its root must remain protected against external mutation. The executable must be
a pure leaf process, not a launcher of child processes or an external-effect agent.

`MaxSubmittedUnits`, when configured, counts initial and successor graph units
once. Original workflow creation time and stage links remain across restarts.
There is no implicit elapsed-time deadline. Platform usage budgets require the
platform owner's later integration; this example makes no rollover qualification
claim. Workflow stop drains linked runs and stops continuation; resume only allows
future continuation and never clears existing run drains.
