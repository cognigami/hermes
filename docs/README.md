# Hermes — discovery handoff and documentation index

## Status and reading order

Hermes (the Greek messenger) is a **new standalone project** in `/Users/ricardo/Documents/hermes`. It has its own users, reusable contracts, lifecycle, platform integrations and acceptance requirements. Perry is its first prospective consumer, not its owner. Do not simply generalize or import Perry's runner.

The operator has now requested a concrete design based on discovery. [The first-release design](design.md) is **proposed for review, not approved or implemented**. No implementation release, code move, dependency installation, prototype, market-data replication or remote execution is authorized by these documents. No prototype has been performed.

1. **This handoff:** authoritative record of agreed direction and documentation index.
2. [Proposed first-release design](design.md): authoritative home for proposed architecture, contracts, failure behavior, verification gates and decisions requiring review.
3. [Distributed execution discovery and Dask assessment](distributed-execution-discovery.md): requirements, rough mechanisms and dated November 21, 2026 documentary comparison. Its earlier sketches are historical proposals, not competing architecture contracts.
4. [WP05-D distributed-compute lessons](wp05d-distributed-compute-lessons.md): historical failure analysis motivating requirements, not Hermes readiness evidence or operating policy.

The two source documents were relocated from Perry's `docs/research/`. Their links to `../../perry/root/docs/` are **optional historical evidence requiring a sibling Perry checkout**. Hermes discovery and this handoff do not require those files. Inline `perry/...`, `scratch/...` and other historical source paths refer to Perry, not files to import into Hermes. Dated findings and numerical observations remain historical; relocation did not rerun their checks or reverify external sources.

## Agreed subsequent decisions and requirements

### Ownership, technology and contracts

- Explicit reusable contracts are the central deliverable for future consumers. Hermes is not a Perry feature or a shared numerical library.
- **Custom infrastructure is favored over Dask** because lifecycle control, durable independent execution and offline finite batches dominate. A concrete Go design is now proposed for review. Retain the Dask evaluation as evidence, not an instruction to implement Dask or proof from a running cluster.
- **Hermes infrastructure will be Go.** Controller, worker agent and CLI are intended Go components. Applications may write computations in any language, including Python, C++ or CUDA. Numerical environments belong to applications, not infrastructure.
- Earlier speculation about Python for Hermes batch infrastructure is superseded by the operator's direction: **“We're Go, others write what they want.”** Go packaging/deployment in Kaggle remains an investigation question, not a reason to assume a Python worker agent.
- Agents are native OS/architecture-specific binaries, not VM bytecode. The operator requires an explicit host inventory and target build recipe; the [host and build contract](design.md#agent-host-and-build-contract) specifies the data to retrieve and the proposed `just build-agent <goos> <goarch>` interface. Matching a compilation target is not platform/resource admission.

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

## Proposed design and review

[The first-release design](design.md) now makes concrete proposals for single-host authority, local SQLite metadata plus immutable artifacts, independent agents, application-owned executable protocols, connected HTTPS exchange and finite export/import. These are **design proposals**, not additional agreed requirements or implemented capabilities.

The design owns the details rather than duplicating them here:

- [Scope and invariants](design.md#2-scope-guarantees-and-limits).
- [Durable storage and authority](design.md#4-durable-storage-and-authority).
- [Agent host inventory and cross-compilation recipe](design.md#agent-host-and-build-contract).
- [Crash and reconciliation](design.md#10-crash-disconnect-and-reconciliation).
- [Drain and deadlines](design.md#11-drain-deadlines-and-interruption).
- [Verification gates](design.md#15-verification-and-staged-delivery--not-authorization), including the still-unexecuted decisive controller-crash/reconnect/drain check.
- [Decisions requiring review](design.md#16-decisions-to-review-before-implementation), including durability scope, application adaptation, uncertainty/retry tradeoffs, OS/resource guarantees, finite exchange and deployment permissions.

No implementation, experiment, data movement or Perry job control is authorized by drafting the design.
