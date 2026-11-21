# Hermes — distributed execution discovery baseline and Dask assessment

**November 21, 2026. Discovery / rough comparison, not an approved formal design or implementation authorization.** Hermes is a distinct standalone project in this repository (`/Users/ricardo/Documents/hermes`), not a Perry feature. The repository location and name are resolved. This document and the [WP05-D retrospective](wp05d-distributed-compute-lessons.md) were relocated from Perry; the [Hermes handoff](README.md) records subsequent decisions, including direction favoring narrow custom infrastructure implemented in Go. The dated Dask investigation below remains documentary evidence, not runtime proof or an instruction to implement Dask. No dependencies, prototype, data replication or remote execution are authorized/performed by this document.

**Subsequent design:** [the concrete first-release proposal](design.md) now owns proposed architecture and contracts. This document remains the dated discovery baseline; its earlier open sketches are not a competing design. The proposal is for review, not an implementation authorization.

Links into `../../perry/root/docs/` are optional historical evidence requiring a sibling Perry checkout. Inline `perry/...` and `scratch/...` paths refer to paths inside Perry, not Hermes. This discovery and its handoff are understandable without that checkout.

## 1. Objective and agreed direction

Run application-defined computations across changing LAN machines and finite cloud sessions, retain useful work through interruption, and change execution policy without restarting the application run. Compare Dask Distributed with a minimal purpose-built implementation against the same behavior, rather than selecting a technology first.

Agreed discovery points:

- Separate stable **work**, mutable **execution policy**, and individual **attempts**.
- Add/remove workers and change future allocations while a run continues. Changing an already-running solver's memory/thread behavior is not required.
- Applications define atomic computation units; packaging groups units to amortize preparation/startup and exploit reuse.
- Drain workers or the run, retaining completed units and repackaging unfinished units.
- Recover a crashed controller and reconnect/reconcile still-running workers, without interrupting them or scanning/revalidating all historical results.
- Support both connected LAN workers and finite batch sessions such as the existing offline Kaggle approach. They need common work/result semantics, not identical connectivity.
- Optimize accepted useful work, not worker count or occupied slots.

Interpretation recorded during discovery: “pick up after a crash and connect to still-running workers” refers to the controller. Worker crashes are also covered, but an atomic computation killed midway is not presumed resumable internally. Subsequent discussion confirmed controller crash/reconnect as a core requirement; exact ownership/reconciliation mechanisms remain open for formal design.

### New controller-cost evidence

The [WP05-D retrospective, lesson 2 addendum](wp05d-distributed-compute-lessons.md#november-21-addendum-repeated-whole-input-hashing-and-delayed-polling) records the November 21 isolated baseline confirming repeated whole-input hashing during completion lookups, plus reported queue scans delaying worker collection. This is concrete evidence for reusing verified immutable identities at read-only lookups and polling workers between package checks—not for treating source liveness as useful progress or restart overhead as unavoidable. Detailed measurements and Perry repair status live there; after-fix measurement is pending, with no validated/deployed fix or speedup claimed. This evidence adds no Hermes implementation authorization.

## 2. Boundary: what the application owns

The application defines calculations, input eligibility, scientific or business acceptance, dependencies and allowed numerical/runtime equivalence. It supplies:

- An immutable unit specification and stable identity, including required inputs, calculation/runtime compatibility, and dependencies.
- Its executable operation and the inputs/artifacts it needs, without requiring arbitrary client memory to remain alive.
- A result-validation operation and any complete domain failure result. The executor must not invent domain conclusions from a timeout or missing file.
- Resource/capability needs and locality constraints/preferences. Unknown demand remains unknown, not a fabricated precise estimate.

The execution system owns assignment, worker registration, resource admission, attempt state, delivery, canonical completion publication, drain/recovery and operational reporting. Validation may be expensive: execute it as explicit bounded work rather than blocking the scheduling/control path. Accepted completion follows durable result publication and required validation; a raw receipt is not acceptance.

This is not a general data-ingestion, research, modelling, cloud-account or agent-management platform. Start with one authoritative controller at a time, not active-active scheduling. No exactly-once execution promise, arbitrary in-solver checkpointing, live mutation of running numerical profiles, or automatic GPU conversion.

## 3. Sketch of state and execution

The mechanisms in this sketch are proposals, not settled contracts or implementation instructions; see the handoff for agreed requirements versus proposed first-release shape.

### Stable work

A run has immutable calculation/input definitions and application dependency barriers. Units can be generated in stages as dependencies become known; there is no requirement to know every future unit at startup.

Each atomic unit has an independent identity and accepted completion record. A package is a **temporary assignment envelope** containing unit references, reusable preparation references and runtime/resource bounds. Repackaging changes neither the unit identities nor their already accepted results.

Atomic means the minimum application-defined durable completion boundary, not a guarantee of short duration. Killing an optimizer can lose that optimizer's in-flight work. Partial internal state is usable only when the application explicitly defines its checkpoint/resume semantics.

### Mutable policy

Policy controls eligible hosts, admission/concurrency, resource ceilings, placement preferences, retry allowances and operating windows. Changes apply to future assignments, including queued-but-not-started work where safe. Already-running assignments retain their issued limits unless explicitly drained/stopped. Record the applied policy/profile on each attempt.

Memory/thread profile changes can require replacement worker processes, without replacing the run or draining unrelated workers. Scheduling reservations are distinct from actual RAM/VRAM enforcement. Host aggregate budgets must include worker processes, native children, preparation and collection.

### Attempts and durable results

An attempt identifies the assigned units, worker/session, runtime profile and deadline. Persist the assignment before dispatch can become ambiguous. A worker publishes each completed unit independently, then acknowledges it; large payloads need not pass through controller memory.

Raw result publication, validation and accepted completion are distinct boundaries. Compatible duplicate delivery must be idempotent; conflicting results require explicit handling, not silent overwrite. External side effects must be idempotent or explicitly outside the execution contract. At-least-once execution may occur after failures; canonical accepted completion is once per unit identity.

### Minimal operating sequence

1. Application exposes ready units and their artifact requirements.
2. Eligible capacity receives a bounded assignment, considering resource needs and data locality.
3. Worker loads/stages inputs and executes units, reusing compatible local preparation.
4. Worker durably publishes completed units and a compact inventory, including any unfinished assignment remainder.
5. Required validation publishes accepted completions; dependencies become ready.
6. Controller continues assigning work under current policy, independently of an interactive client or LLM.

No particular database, queue, storage service or wire protocol is chosen here. The implementation language is now agreed as Go; the application executable/process boundary remains proposed. “Durable” must be defined by deployment: worker-local disk may survive a process crash but not machine loss; notebook-local storage is not a durable handback channel. Store placement and accessibility are part of the design, not implementation details to discover after failure.

## 4. Drain and crash behavior

### Connected workers

**Worker drain:** stop new assignments; signal the current package to stop taking new units after its current atomic unit, publish completed units/remainder, and retire. Other workers keep running. A drain acknowledgment establishes when that worker stopped taking new units; a controller's request timestamp is not an instantaneous global barrier.

**Run drain:** stop issuing new assignments, then apply that boundary behavior to all current work. Completion means handback/quiescence, not application/scientific completion. Allow a “finish whole package” option where useful, but do not require it for useful handback.

**Drain deadline:** before a finite deadline, allow current units to finish. At the deadline, interrupt according to the supported platform contract; retain accepted units, raw published results and honest unfinished/unknown states. A unit longer than the remaining window may have to restart later. Drain is not a zero-loss guarantee for arbitrary running units.

### Controller crash

Workers continue their issued bounded assignments and persist results while disconnected. They must not take unlimited new work without the authority to do so. A replacement controller obtains exclusive authority, restores compact durable state, queries worker/session identities and inventories, accepts pending results, and reattaches live attempts before considering redispatch. Full historical revalidation is unnecessary where immutable accepted completion already establishes compatibility.

A lost heartbeat or expired lease is not proof that a numerical process stopped. Handle stale-controller commands/duplicate assignment through a concrete ownership/reconciliation mechanism; do not automatically redispatch uncertain offline work. The exact mechanism is open, but this correctness requirement is not.

### Worker crash and finite batch sessions

After a worker crash, recover published complete units; rerun only missing units after resolving old attempt ownership. Unfinished optimizations restart under their prescribed initialization unless application checkpoints explicitly support more.

An offline notebook cannot receive a live drain command or reconnect to a controller mid-session. Its package must carry finite bounds and a predeclared stop-taking-new-units/flush policy. Drain means no new sessions plus reconciliation/collection of existing sessions within their issued lifetime. Immediate cloud cancellation is platform-specific and must not be promised by a common API if unavailable.

## 5. Work type and data locality

**Work type matters a great deal.** Placement must distinguish:

- CPU work, internally threaded native work, GPU work, memory-heavy work and I/O-heavy preparation.
- Required runtime/OS/device capabilities and transfer/output size.
- Hard restrictions (data rights, authorized locations, unavailable runtime) versus soft preferences (data already cached here).
- Persistent reusable inputs/preparation versus per-attempt scratch.

A GPU label is an eligibility constraint; the application still needs a GPU implementation. An efficient preparation worker may need no GPU. Host RAM, device VRAM, device assignment and numerical threads are separate dimensions.

### Was local-only WP05-D preparation necessary?

Not established. The inspected monthly builder reads already-ingested immutable row files, constructs a package and writes it. The local preparation slice was explicitly scoped to the iMac; that is not evidence that remote execution was impossible or slower. Chronological ingestion, monthly construction and dependent forecast preparation are different operations and need not have the same placement.

Remote monthly preparation could plausibly stage an authorized row subset once, reuse it across several monthly builds, and execute builds elsewhere. It might then keep large packages near their numerical consumers. Alternatively, replication, duplicated decoding, disk contention and returning large packages could outweigh saved compute. The useful ten-input check produced about 1.3 GB of package bytes; output transfer is not negligible merely because the input rows are reusable.

For a placement decision compare, over the relevant batch/reuse horizon:

`remote staging + remote queue/build + required result transfer/validation`

against

`local queue/build + required downstream transfer/validation`.

Account for overlapping histories and shared link/disk contention; do not assume advertised bandwidth, linear core scaling or free replication. Prefer staging one bounded reusable input set over independently copying full histories into every task. Do not replicate entire source publications by convenience or silently expand data access. No such replication or timing investigation is performed here.

Dask locality only helps with data it knows about. Passing a local filename does not make that file present remotely or tell Dask its dependency size. Arbitrary on-disk caches need explicit staging/availability information; in-memory futures are not a durable artifact catalogue.

## 6. Required behavior: comparison baseline

| ID | Requirement / discriminating scenario |
|---|---|
| R1 | Stable units and independently accepted outputs; repackage the remainder without rerunning completed units. |
| R2 | Add capacity, drain one worker and change future allocations without restarting the run. |
| R3 | Restart the application controller while issued worker computations continue; recover compact state and reconnect. |
| R4 | Recover execution-scheduler/controller infrastructure failure without killing/restarting surviving issued computations. Distinguish this from merely losing an interactive client. |
| R5 | Drain at application atomic-unit boundaries, publish completed parts and expose the remainder; deadline behavior explicit. |
| R6 | Worker loss, duplicate/late results and uncertain submission do not cause false completion or silent duplicate acceptance. |
| R7 | Resource-aware, locality-aware admission with explicit actual enforcement scope; no automatic retry loop under the same inadequate allocation. |
| R8 | Connected LAN workers and offline finite batches use compatible work/result identities, with honest differing control semantics. |
| R9 | Recovery and validation cost scales with changed/incomplete work rather than repeatedly processing all accepted payloads. |
| R10 | Runtime/native/GPU compatibility, data permissions and authenticated transport are explicit. Useful progress distinguishes computing, saving, validating and waiting. |

R3 and R4 are deliberately separate. The strongest reading of the operator's crash requirement includes R4. If acceptable failure semantics are narrower, decide that explicitly rather than quietly dropping it to accommodate a candidate.

## 7. Dask assessment — documentary, not prototype evidence

Reviewed public stable Dask/Distributed documentation (reported version 2026.8.0) and rendered scheduler/worker source on November 21. No local package installation, version-specific runtime check, cluster or benchmark. Source links below are mutable; recheck a selected version before implementation.

| Area | What Dask contributes | Gap against this baseline |
|---|---|---|
| Dependencies/concurrent tasks | Futures/graphs and resource-constrained task scheduling. | Application still defines the correct graph, units and acceptance; singleton submission can still serialize it. |
| R1 durable units | Individual tasks can correspond to atomic units. | Futures/in-memory results are not our durable accepted-unit ledger. A multi-unit task still needs application publication/yield semantics. |
| R2 changing capacity | Workers can join later; unsatisfied resource requests can wait for them. Worker retirement APIs exist. | We can drain/replace processes for new profiles, but full atomic drain and queued-policy-change semantics need integration. In-place resource mutation is not assumed. |
| R3 application controller loss | A separately running Dask scheduler can outlive its submitting client. `fire_and_forget` requests execution even after futures are released. | This is at-least-once execution, not durable result retention or reattachment to our run. Client reference lifetime matters; externally durable outputs and explicit ownership are still required. |
| **R4 scheduler loss** | Resilience docs discuss workers/clients reconnecting. | **Documented mismatch:** no scheduler-state persistence; ongoing computation records are lost. Worker source also closes on a broken scheduler stream. Cannot claim surviving calculations are preserved merely because processes reconnect. |
| R5 drain | Retirement evacuates unique in-memory data and marks workers unavailable for new tasks. | Explicit retirement of a busy worker does not establish “finish current atomic unit.” Neither retirement docs nor inspected code is sufficient proof of our boundary-drain contract. |
| R6 failures/retries | Failed tasks propagate exceptions; worker loss can trigger recomputation from the graph. | Automatic recomputation may violate our “reconcile live/uncertain execution first” requirement or repeat side effects. Bounded retry policy and external idempotent acceptance remain ours. |
| R7 resources/memory | Abstract resource tags, worker process memory monitoring, spill/pause/nanny termination. | Resource tags are admission quantities, not physical RAM/VRAM enforcement. Spill cannot evict arbitrary active native-solver allocations. Nanny termination can reschedule/recompute; profile compatibility must be deliberate. |
| Locality | Schedules near known dependent data, minimizing transfers; explicit worker restrictions/preferences and scatter/broadcast. | External artifact staging and file availability remain ours. Broadcast is not automatic replication to future workers or durable storage. |
| R8 offline Kaggle | Could be used inside a notebook for local scheduling if useful. | An offline notebook cannot join the LAN's live scheduler. A finite-batch transport/executor remains necessary. Dask-in-notebook does not unify that control plane. |
| R9 cheap recovery | Maintains useful live scheduling/data state while healthy. | Does not establish durable run restart proportional to incomplete work; persistence/reconciliation/validation still need application infrastructure. |
| R10 GPU/runtime/security | Schedules Python functions using GPU libraries; resource tags/device pinning; supports mutually authenticated encrypted TLS. | Does not convert CPU kernels, provide every native/GPU runtime, prove native Windows compatibility for our stack, or authorize data transfers. |

### Important corrections to loose earlier descriptions

- `retire_workers` is not credited as application atomic-boundary drain. Scheduler retirement explicitly addresses unique in-memory data; automatic worker selection generally avoids busy workers, but explicitly named workers bypass that selection check. “Graceful” alone is insufficient.
- Reconnecting to a restarted scheduler is not recovery of the old execution state. The resilience page explicitly says no persistence mechanism, and inspected `Worker.handle_scheduler` closes when its stream ends. Exact consequences require a selected-version experiment, not optimistic interpretation.
- `fire_and_forget` retains execution intent while a scheduler lives, not completed output retention: its docs say results are not kept after completion without an active future. Side-effecting result publication needs our own durable/idempotent contract.
- Dask resource annotations are not a physical memory sandbox. Worker memory management can kill a process and lose/recompute its unique in-memory results.

### What the source examination actually establishes

Scheduler `retire_workers` sets a closing-gracefully status and `_track_retire_worker` waits for its data-retirement policy. This is not proof that it waits for arbitrary busy user functions to complete. Worker `close_gracefully` asks the scheduler to retire its address and then closes. Worker `handle_scheduler` calls `close(reason="worker-handle-scheduler-connection-broken")` when the scheduler stream ends. These observations justify withholding the required guarantee, not inventing exact timing for every possible runtime configuration.

## 8. Comparison with a minimal custom implementation

Both choices require unit identities, durable outputs, validation, domain dependency barriers, finite offline batches and platform-specific runtimes. Dask does not eliminate that layer. A custom design must not be compared with a fantasy of free reliable scheduling, however.

**Dask-backed approach:** retain a small durable run layer and Kaggle batch lane; delegate connected task scheduling, dependency readiness and in-memory locality to Dask. Strongest benefit when connected CPU/GPU tasks exchange reusable intermediate data and R3 suffices. Main risk: satisfying strict R4 requires scheduler-independent execution ownership, potentially an independent attempt process/service that Dask merely launches. If that rebuilds the difficult lifecycle under Dask, the claimed simplicity benefit shrinks.

**Minimal custom approach:** one durable controller, bounded pull/dispatch assignments, independently owned worker execution and durable result receipts; finite batch packaging uses the same unit/result contract. Start with explicit profiles and simple placement, not work stealing, automatic scaling or an optimizer. Strong fit for coarse, file/artifact-based, long-running units with expensive in-flight work. Cost: implement and maintain network/protocol behavior, exclusive ownership, ambiguous dispatch recovery, retries, authentication, fair resource admission and observability. These are exactly the areas WP05-D showed are not trivial glue.

**A third practical option is a narrower first release.** If strict scheduler-crash survival and offline batches dominate, build only that minimal durable execution seam first, leaving connected fine-grained scheduling to Dask later if measured need warrants it. This is an option to compare, not authorization or a recommendation to implement now.

### Provisional judgment

Dask is a credible **connected execution backend**, not an out-of-the-box implementation of the agreed framework. Its strongest benefits are live scheduling/dependencies/locality. Its largest mismatch is scheduler-crash continuity; its other substantial gaps are durable per-unit completion, atomic-boundary drain and offline batch integration.

Do not select Dask simply because it has “retire,” “resources” and “resilience” APIs. Equally, do not reject it because it does not own domain acceptance or durable application artifacts—those may reasonably remain outside any task scheduler. The decision turns on how much missing lifecycle code remains and whether it displaces or duplicates Dask's own control mechanisms.

**Subsequent direction:** discussion now favors a narrow custom Go implementation because independent durable execution, lifecycle control and offline batches dominate. This supersedes the open technology preference above, not its dated findings or qualifications. No prototype has been performed; formal design is still to be discussed.

## 9. Small decisive investigations, if later authorized

No prototype or performance campaign is released here. Prefer synthetic, application-neutral units in Hermes:

1. **Crash separation:** kill submitting controller, then separately kill Dask scheduler while an instrumented long-running unit continues. Observe execution identity, computation continuity, accepted results and reattachment—not merely worker reconnection. Run the equivalent custom scenario if prototyped.
2. **Drain:** one package with several independently published units; request worker/run drain mid-unit. Establish last unit started, receipts, returned remainder and deadline behavior. Compare Dask native retirement with the proposed integration explicitly.
3. **Policy:** add a worker, remove another, replace one profile and change eligibility for pending work while other units remain live. No global restart or accepted-work invalidation.
4. **Loss/ambiguity:** disconnect control traffic, lose an acknowledgment, deliver a result twice, and restart the controller after publication but before acceptance. No silent duplicate dispatch/acceptance or full historical replay.
5. **Locality:** synthetic representative-size reusable inputs and outputs, local build versus staged remote build. Separate cold transfer from warm-cache reuse; count serialization, validation, disk/network contention and repeated work. Only later apply to explicitly authorized real data.
6. **Batch handback:** simulate a disconnected finite worker with a hard deadline and delayed output delivery, using the same unit IDs and acceptance path. Real Kaggle/Windows/GPU admission is a separate later release.

The repository location/name are now settled. Before those investigations, settle the exact R4 survival semantics, acceptable in-flight unit loss, storage topology/durability and initial worker platform/runtime. Do not choose a full message schema or framework extension architecture before those decisions. The handoff records the proposed decisive custom crash/reconnect/drain check; none of these checks is authorized now.

## Sources

These Dask links are retained references from the dated investigation, not newly fetched or reverified during relocation.

- [Dask resilience](https://distributed.dask.org/en/stable/resilience.html): scheduler non-persistence; recomputation; scattered-data loss.
- [Dask API](https://distributed.dask.org/en/stable/api.html): retirement, futures, `fire_and_forget`, retries.
- [Scheduler source](https://distributed.dask.org/en/stable/_modules/distributed/scheduler.html): `workers_to_close`, `retire_workers`, `_track_retire_worker`.
- [Worker source](https://distributed.dask.org/en/stable/_modules/distributed/worker.html): `close_gracefully`, `handle_scheduler`.
- [Worker resources](https://distributed.dask.org/en/stable/resources.html): late joining, abstract/per-process quantities.
- [Worker memory](https://distributed.dask.org/en/stable/worker-memory.html): spilling, pausing and nanny termination/recomputation.
- [Data locality](https://distributed.dask.org/en/stable/locality.html): placement, scatter/broadcast and future workers.
- [GPU execution](https://docs.dask.org/en/stable/gpu.html): GPU functions, device assignment and concurrency.
- [TLS](https://distributed.dask.org/en/stable/tls.html): supported authenticated encrypted transport; default TCP is not that boundary.
- [WP05-D preparation checkpoint](../../perry/root/docs/research/wp05d-local-package-preparation-plan.md) (optional historical evidence requiring sibling Perry) and Perry-internal `perry/range_placement_programme.py`, `_build_monthly_input`: actual local immutable-row boundary and measured package volume, not a remote timing comparison.
