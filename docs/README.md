# Hermes — discovery handoff and documentation index

## Status and reading order

Hermes (the Greek messenger) is a **new standalone project** in `/Users/ricardo/Documents/hermes`. It has its own users, reusable contracts, lifecycle, platform integrations and acceptance requirements. Perry is its first prospective consumer, not its owner. Do not simply generalize or import Perry's runner.

This is the self-contained handoff for a fresh formal design conversation. **Discovery only: no approved formal design, implementation release, code move, dependency installation, prototype, market-data replication or remote execution is authorized by these documents.** No prototype has been performed. The next step is discussion, not implementation.

1. **This handoff:** authoritative record of subsequent direction and unresolved decisions.
2. [Distributed execution discovery and Dask assessment](distributed-execution-discovery.md): requirements, rough mechanisms and dated November 21, 2026 documentary comparison. Read proposed mechanisms as proposals, not settled contracts.
3. [WP05-D distributed-compute lessons](wp05d-distributed-compute-lessons.md): historical failure analysis motivating requirements, not Hermes readiness evidence or operating policy.

The two source documents were relocated from Perry's `docs/research/`. Their links to `../../perry/root/docs/` are **optional historical evidence requiring a sibling Perry checkout**. Hermes discovery and this handoff do not require those files. Inline `perry/...`, `scratch/...` and other historical source paths refer to Perry, not files to import into Hermes. Dated findings and numerical observations remain historical; relocation did not rerun their checks or reverify external sources.

## Agreed subsequent decisions and requirements

### Ownership, technology and contracts

- Explicit reusable contracts are the central deliverable for future consumers. Hermes is not a Perry feature or a shared numerical library.
- **Custom infrastructure is favored over Dask** because lifecycle control, durable independent execution and offline finite batches dominate. Formal design remains to be discussed. Retain the Dask evaluation as evidence, not an instruction to implement Dask or proof from a running cluster.
- **Hermes infrastructure will be Go.** Controller, worker agent and CLI are intended Go components. Applications may write computations in any language, including Python, C++ or CUDA. Numerical environments belong to applications, not infrastructure.
- Earlier speculation about Python for Hermes batch infrastructure is superseded by the operator's direction: **“We're Go, others write what they want.”** Go packaging/deployment in Kaggle remains an investigation question, not a reason to assume a Python worker agent.

### Work, policy and attempts

- Separate stable work, mutable operational policy and individual attempts. Changing machines, concurrency, memory or thread profiles for **future work** must not restart the run. Live mutation of a running solver is not required.
- A profile change may drain/replace worker processes while other workers continue. Policy application boundaries still need precise contracts.
- Applications define atomic computation units, for example one optimization. A packager groups them at a higher level; packages are **not indivisible acceptance units**.
- Drain finishes the current safe unit, publishes completed units and returns/repackages the remainder under the same unit identities. No arbitrary optimizer checkpoint/resume promise follows from this.
- Both individual-worker and whole-run drain need finite deadlines and explicit forced-stop behavior. An interrupted long unit may lose in-flight work; acceptable bounds remain open.

### Failure and execution environments

- Controller crash/reconnect to surviving workers is a **core requirement**. The controller process should be cheaply replaceable; controller recovery must not restart surviving issued computations or repeatedly process all completed payloads.
- After worker crash, resume missing units only after ownership reconciliation; do not repeat completed units. Distinguish controller crash, scheduler crash and worker crash. Losing an interactive client is not evidence of scheduler/controller crash survival.
- Connected LAN workers and finite/offline Kaggle batches share work/result semantics without pretending to share connectivity or live control capabilities.
- Windows LAN GPU and Kaggle GPU are targets, **not exercised or certified environments**. Native Windows versus WSL remains undecided.

### Preparation, locality and native workloads

- Preparation placement depends on work type and data locality. Local-only preparation was **not proved necessary** in Perry. Ingesting chronological data, building independent monthly inputs and preparing dependency-bound downstream work are distinct operations.
- An authorized immutable bounded row subset could potentially be staged once and reused remotely, perhaps leaving generated packages near numerical consumers. This is a possibility, not a measured advantage or data-transfer permission.
- Compare staging, cold versus warm reuse, serialization, validation, shared disk/network/memory contention and large output transfers. Remote preparation can help or hurt. No market-data replication is authorized.
- **Dask supports Python tasks invoking custom C++ libraries.** Native kernels were not a blocker. Perry wrappers use `ctypes.CDLL` and `.dylib`/`.so` libraries; workers need compatible builds for each OS/architecture and worker-side loading. A Windows native wrapper/build was not established.
- Dask does not parallelize C++ internals, convert CPU code into GPU code, or checkpoint arbitrary native calls. Internal native threads still must be coordinated with task concurrency. This clarification does not change the lifecycle/offline reasons for favoring custom infrastructure.

## Proposed first-release shape — positively discussed, not settled design

A narrow first release could contain:

- One authoritative durable/recoverable controller and independent workers.
- Immutable referenced inputs and independently durable per-unit results.
- Explicit resource profiles and simple placement.
- Drain, interruption and ownership/result reconciliation.
- Connected and finite-batch modes using common work/result contracts.

Initially avoid active-active control, work stealing, autoscaling, distributed shared memory, arbitrary solver checkpoints and a scheduling optimizer. This is scope direction for discussion, not a chosen database, broker, protocol or schema.

An **application-owned executable/process boundary** is proposed: a Go agent launches the workload, which could be a Python wrapper loading C++. Infrastructure would not import the application's numerical environment. Exact invocation, packaging, result validation and lifecycle contracts remain to be designed.

### Proposed decisive future synthetic check

Kill the controller while workers compute; restart and reconnect; drain one worker and reassign its remainder without rerunning completed units or disturbing other workers. Observe computation identity/continuity, publication, acceptance, ownership and quiescence—not just process reconnection.

**This check has not been executed and is not authorized now.** It should use synthetic, application-neutral work if later released. Real Kaggle/Windows/GPU acceptance would be separate evidence, not implied by a synthetic pass.

## Open for the formal design conversation

- Exact work, attempt, result and control contracts, including integration of application/domain validation and dependency readiness.
- Controller/worker ownership, fencing, stale commands, ambiguous dispatch, late results and reconciliation; exact scheduler-failure survival semantics.
- Storage topology, accessibility, durability under process versus machine loss, and cheap resume without full historical revalidation.
- Drain bounds, handback confirmation, forced-stop semantics and acceptable in-flight unit loss.
- Native Windows versus WSL, process-tree ownership and shutdown behavior.
- Packaging and deploying Go artifacts in Kaggle's finite/offline environment.
- Policy application boundaries for future, queued and already-started work.
- Resource admission versus actual enforcement: host RAM, VRAM, device ownership, aggregate usage, native threading, preparation and collection.
- Data staging/cache permissions, locality and reuse horizons; no implicit permission to replicate market data.
- Trusted execution, authentication and transport boundaries.
- Minimal first-release scope and its acceptance criteria.

**No database, broker, protocol or message schema has been selected.** No code moves or implementation are authorized. Start the design chat from these requirements and unresolved choices rather than treating the historical sketch as an approved architecture.
