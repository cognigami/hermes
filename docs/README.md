# Hermes

Hermes is a standalone Go execution system for application-defined computations across native macOS LAN workers and finite Kaggle CPU sessions. Applications own their numerical runtimes, units, validation and phase decisions. Perry is the first intended consumer, not the owner of Hermes.

The first-release design covers one macOS coordinator, independently supervised agents, durable per-unit acceptance/recovery, automatic resource rescheduling and managed Kaggle staging/submission/collection. Applications submit fixed graphs and continue authorized phases automatically. Windows and GPU execution are outside the first release.

**Status:** design and implementation planning; no Hermes implementation or qualified deployment yet.

## Working documents

| Document | Purpose |
|---|---|
| [Design](design.md) | Authoritative architecture and behavioral contracts |
| [Implementation plan](implementation-plan.md) | Ordered delivery slices, dependencies, task progress and coordinator start brief |
| [Coordination](COORDINATION.md) | Delegation, agent placement, review and integration workflow |
| [Project instructions](../AGENTS.md) | Durable repository rules and documentation ownership |

## Background references

| Document | Purpose |
|---|---|
| [Distributed-execution discovery](distributed-execution-discovery.md) | Historical requirements exploration, platform trials, optional Dagu reuse assessment and design provenance |
| [WP05-D compute lessons](wp05d-distributed-compute-lessons.md) | Historical failure analysis informing the contracts |

Background observations are not current operating instructions or readiness claims. Optional links into `../../perry/root/docs/` require a sibling Perry checkout. Implementation and deployment follow the operator's actual release; these documents do not authorize remote execution, research-data movement or Kaggle submissions.
