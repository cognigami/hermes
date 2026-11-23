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

The [WP05-D retrospective, lesson 2 addendum](wp05d-distributed-compute-lessons.md#november-21-addendum-repeated-whole-input-hashing-and-delayed-polling) records the November 21 isolated baseline confirming repeated whole-input hashing during completion lookups, plus reported queue scans delaying worker collection. This is concrete evidence for reusing verified immutable identities at read-only lookups and polling workers between package checks—not for treating source liveness as useful progress or restart overhead as unavoidable. Detailed measurements and Perry repair status live there: the matched after measurement and repaired-code relaunch are now recorded, with about 34.5× improvement limited to the measured completeness operation. Whole-restart/pickup and optimizer-throughput gains are not established. This evidence adds no Hermes implementation authorization.

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

## 10. Reuse assessment: retryable atomic runs (November 22)

**Documentation-only investigation requested by the operator.** Native macOS coordination without a Linux VM/container is a hard deployment requirement. Airflow's [prerequisites](https://airflow.apache.org/docs/apache-airflow/stable/installation/prerequisites.html) distinguish macOS development from Linux-only supported production, excluding it on that basis. The operator is now exploring whether inexpensive atomic runs, with package aggregation later, can replace the stronger requirement to preserve every surviving computation. This is a proposed requirement relaxation, not a claim that current Perry optimizations are cheap or permission to change their execution.

Evaluate equivalent application outcomes, not compatibility with our proposed Go agent, database, schemas or executable protocol. Direct adoption is preferable to wrapping a product in a second scheduler. Application-specific validation and offline transport alone do not justify building a general executor.

### Proposed smaller contract and what it removes

```text
immutable package manifest: expected stable unit IDs and input/runtime references
                    |
        platform schedules independent unit attempts
                    |
        per-attempt complete result artifacts
                    |
      application validates and commits first valid result per unit
                    |
        aggregate exactly the package's expected unit IDs
```

- A platform run is one bounded, restartable application unit, not necessarily a whole monthly package. Changing host, scheduler run ID or attempt does not change the unit's meaning. One optimization is only a candidate boundary; its loss/preparation cost must be acceptable before selecting it.
- At-least-once execution is acceptable under this proposed model: an interrupted or unreachable attempt may be repeated, even while an old attempt might still exist. No non-idempotent external side effects. This relaxes execution ownership, not data integrity or machine-resource safety.
- An attempt writes isolated result bytes and publishes a completion manifest only when they are complete. Platform success or a log line is not sufficient. A result lost before durable publication may be recomputed; already committed valid results are reused.
- Retain the agreed first-valid acceptance rule: application validation establishes downstream suitability, and only the first valid result to commit becomes canonical for a stable unit. Never overwrite it because another attempt differs. This can be an application-owned validate/publish step using a single canonical storage boundary, not a new Hermes service. Ordinary platform caches are not automatically that boundary.
- Aggregation is a separate retryable operation over an explicit expected-unit manifest and canonical result references. It never zips unordered successful child outputs or treats a missing unit as a scientific negative. Aggregation must not depend exclusively on one live parent retaining Python objects or child process handles.
- Application/runtime/input definitions stay immutable across retry, even if the platform reloads deployment files. Packages and accepted artifacts need retention independent of execution-history cleanup.
- Droppable machinery includes surviving-process adoption, no-overlap redispatch proof and persistent multi-unit session/control protocols. Whole-package atomic acceptance is unnecessary. Stop-taking-new-runs plus finishing/killing the current atomic run may suffice instead of handback of a resident package process.
- Still necessary: finite run/retry bounds, approved data locations, actual host budgets and deadlines, prevention of orphaned-process accumulation, complete durable results and domain validation. Allowing duplicate execution does not make overlapping RAM/VRAM consumption or false host-handback reports safe.

The key unresolved input is the acceptable loss per attempt, including staging/preparation, rather than whether a scheduler advertises retries. Making runs extremely small can increase repeated input loading, publication traffic and scheduling overhead. Immutable input references and permitted per-host caches may amortize staging without requiring immortal numerical processes. No numerical timing investigation is authorized by this assessment.

### Dagu: native platform candidate, not merely a workflow wrapper

Official docs describe a single binary with no required database, broker or language runtime; Intel/Apple Silicon macOS builds are published. There is a native [macOS launchd deployment guide](https://docs.dagu.sh/server-admin/deployment/macos), not merely a development-server example. Its example is a GUI-session LaunchAgent; choose the required boot/login lifetime explicitly instead of claiming machine-wide unattended service from that example. The platform table explicitly says macOS is not tested on every change, so release/host admission remains necessary. See [system requirements](https://docs.dagu.sh/server-admin/system-requirements).

A concrete direct-adoption shape is a coordinator-local root DAG that fans out a child DAG per unit, each executing an application-owned executable, followed by a validate/publish and package-aggregation stage. Dagu supports dynamic `parallel.items`, `parallel.max_concurrent`, and per-child `worker_selector` routing. Keep the parent local so named child resolution does not require a worker-local DAG catalogue. Workers execute whole child DAGs; arbitrary inline steps are not independently assigned to different machines. See [sub-DAGs](https://docs.dagu.sh/writing-workflows/sub-dags), [execution control](https://docs.dagu.sh/writing-workflows/execution-control) and [worker labels](https://docs.dagu.sh/server-admin/distributed/worker-labels).

What could be omitted: custom coordinator API/UI/history, dependency/fan-out scheduler, worker polling/dispatch, label-based placement, ordinary retry/concurrency configuration and run-level drain plumbing. Application code supplies unit manifests, the executable, validation/canonical publication and aggregation. No custom Go agent or second scheduler is assumed.

Consequential boundaries from the documentation:

- [Workers](https://docs.dagu.sh/server-admin/distributed/workers/) poll via gRPC; a heartbeat stale for over 30 seconds causes running tasks to be marked failed. This conflicts with the original no-reassignment-with-uncertain-owner requirement, but may be acceptable with cheap, repeatable attempts. It does not prove old computation stopped. Retry/cleanup behavior under coordinator loss still needs verification for a selected release.
- [Worker deployment](https://docs.dagu.sh/server-admin/distributed/workers/shared-nothing) explicitly requires coordinator connectivity throughout execution. Do not use that mode as a disconnected Kaggle executor. Offline execution/import can remain a separate finite application path producing the same unit-result contract.
- [File dependencies](https://docs.dagu.sh/writing-workflows/file-dependencies) create fresh dispatch snapshots, with 64 MiB compressed / 256 MiB extracted limits including the DAG and selected files. Do not embed the existing 118.7–255.5 MiB monthly payloads in each unit dispatch by default. Use small manifests and separately staged immutable inputs; pin definitions so redispatch does not silently use changed source files.
- [Artifacts](https://docs.dagu.sh/writing-workflows/artifacts) are worker-staged and uploaded before distributed run completion. They are not inherently our immutable acceptance store: local writes go directly to the final directory, stdout/stderr artifact paths are replaced on attempts, and history cleanup removes run artifacts. Use unique attempt paths, completed manifests and explicit canonical publication/retention. Partial/delayed delivery must not make a unit complete.
- A sub-DAG parent's `results` preserve item order, but its successful `outputs` array omits failed children; never associate results to units by success-array index. Include stable IDs in every result. Parent-step retries can rerun a child as a unit; application result reuse must remain valid across platform attempts.
- [Operations](https://docs.dagu.sh/server-admin/operations) offers no-new-runs draining with time limits; this is not proof of our original per-unit gate within an opaque multi-unit program. One unit per child makes the platform boundary useful. [Execution control](https://docs.dagu.sh/writing-workflows/execution-control) documents process-tree termination on step timeout; qualify that on the actual Mac release/backend rather than treating the documentation as a test result.
- Labels such as `memory=64G` are exact-match routing values, not demonstrated physical RAM/VRAM enforcement. Explicit host/profile limits and residual-process accounting remain necessary.
- The inspected upstream [license](https://github.com/dagu-org/dagu/blob/main/LICENSE) is GPLv3. Review applicable obligations before distributing modifications or embedding code; prefer using the platform as a separate executable over importing its implementation into Hermes.

**Provisional finding:** Dagu merits a direct-replacement check under the smaller contract. The timeout/ownership mismatch is no longer an automatic disqualifier if the operator accepts repeated attempts; durable publication, native process cleanup and acceptable unit cost become the decisive questions.

### Alternative selection

**Operator decision: Prefect is excluded, not a fallback.** The choice is Dagu or custom Hermes; do not resume a platform survey if Dagu fails. Earlier Prefect findings remain in source-control history, not an active research agenda.

### Recommendation and decisive evidence

Evaluate **Dagu directly**, not a Hermes wrapper by default. If Dagu cannot meet the retained requirements with reasonable study commands, return to custom Hermes rather than Prefect. The current documentary evidence does not establish the actual Mac lifecycle/resource envelope. Native service examples, product state names and cache features are not runtime verification.

Before a later authorized synthetic check, settle:

1. Maximum acceptable lost compute/preparation time per atomic unit and maximum concurrent duplicate CPU/RAM/VRAM cost. No real-study optimization has been demonstrated to fit that tolerance.
2. Connected host/OS list and one accessible durable result-store/publication boundary. Preserve first-valid accepted references independently of platform retries and retention.
3. Whether a separate finite Kaggle executable/import path is sufficient; requiring an offline platform worker would retain much more custom machinery.

### Next low-load session: WP05D-revival as a Dagu project

**Planned fit test, not executed or scheduled by this note.** The operator proposes the next low-load session ("tomorrow" in discussion). The question is not whether Dagu can run two toy commands: could WP05D-revival have been credibly expressed as study definitions and ordinary commands, without implementing another scheduler? Pin the Dagu release, agree finite wall-time/RAM/disk/concurrency budgets and identify permitted hosts before starting. Nothing here authorizes interruption of current work, real-study numerical experiments, production-state writes, market-data replication or Kaggle submission.

**Gate A — faithful expression before stress:** map WP05D-revival's actual preparation boundaries, independent work, dependency/stage barriers, accepted-prefix reuse, normal/exclusive retry profiles, finite cutoff, publication, collection and scientific validation to Dagu YAML plus named application commands. Use source contracts and metadata, not scientific values. Distinguish pre-existing commands from new application glue. Do not conceal a replacement supervisor inside a shell command or assume an existing package-level command exposes one optimization independently. Identify the smallest usable atomic boundary and its preparation/retry cost; do not invent a universal duration.

**Restart-cost clarification:** [lesson 2's recovery-session clarification](wp05d-distributed-compute-lessons.md#november-21-addendum-repeated-whole-input-hashing-and-delayed-polling) records the current call boundaries. The 144 prepared inputs total 25.75 GiB; many are unfinished work, so their retained scientific history must not be confused with completed results. Programme cache hits still fully decode/snapshot/check prepared packages before dispatch, and the supervisor traverses/loads packages again. The narrow hash repair did not remove these paths; the receipt's roughly 99-minute launch-to-first-worker delay is not a controlled speedup comparison. The target is metadata-only scheduling/completion recovery, **not** eliminating necessary input verification, result-integrity checks or first scientific acceptance. Give each Dagu task a concrete unit/input identity; invoking the existing whole-study resume driver per node would retain the problem. Account separately for construction, JSON decoding/copying, hashes, integrity checks, new scientific validation, reconciliation/collection and actual computation. The clarification used existing code/receipts only, with no new monitoring or experiments.

**Existing operating mechanisms:** remote authentication/deployment already has an operator-trusted SSH workflow; reuse it, including forwarding where appropriate, rather than inventing a new host-authentication platform. Check the concrete worker/coordinator reachability boundary without assuming SSH login alone secures an exposed Dagu API. The operator accepts a study-specific wrapper issuing Kaggle commands from Dagu instructions if needed; that glue is not a failed adoption or a reason to create another scheduler.

**Gate B — bounded native synthetic execution:** exercise the mapped shape using application-neutral fixtures, representative input sizes and controlled operational failures. Local isolated workers can establish local concurrency/lifecycle behavior; they do not certify remote Mac/Windows hosts. Actual multi-host deployment needs explicit host access/admission. Use compact metadata for the 144-input/25.75 GiB history and a bounded representative payload plus read/hash counters where sufficient; do not manufacture or scan 25 GiB merely for realism. Report that this tests the no-history-reprocessing property, not whole-study throughput. Set performance thresholds before measuring rather than declaring any result acceptable afterward.

| Question / scenario | Required evidence and pass condition |
|---|---|
| **Native Mac operation and agent footprint** | Start the pinned Mac coordinator with no VM/container. Record exact OS/architecture, binary and compressed sizes, service lifetime, config/state/credentials and ports. Determine whether each connected agent needs only the matching Dagu binary plus configuration/service definition, or any additional runtime. Application Python/native libraries remain separate. Show install/uninstall or portable deployment, including retained data, without claiming an arbitrary downloaded binary needs no setup. |
| **Atomic computations and later aggregation** | Express independent units as child runs with stable application IDs, not platform attempt IDs. Publish complete per-unit results; fail a unit and restart the parent/reducer. Aggregate the exact expected IDs only after application validation, preserving first-valid acceptance. Reordered/duplicate outputs, missing units and a successful process with invalid output must not produce false package completion. Confirm stage barriers without serializing genuinely independent months. |
| **Normal pools to one larger-memory retry** | Run multiple bounded synthetic units under normal host profiles. Inject a classified resource failure, retain completed units, and route only the failed/missing unit to a permitted larger-memory exclusive profile with bounded retries. Independent normal work continues elsewhere; no blanket serialization. Do not treat historical fixed allocations or local-only larger-memory routing as intrinsic hardware/platform limits. Only approved synthetic budgets are exercised; no actual cloud policy is changed. On the same host, an "exclusive" label is not enough: prove normal admission has drained/stopped or that shared host accounting excludes overlap. Unclassified failures must not silently trigger memory escalation. Record whether this is declarative YAML/existing commands or requires new application routing logic. |
| **Stop/restart without reprocessing 25 GiB** | Restart coordinator, root workflow and a worker separately; distinguish graceful stop from crash. Instrument payload reads/hashes and accepted-result validation. Recover from compact metadata and existing valid unit results without eagerly loading/decoding/copying/hashing the corpus merely to reconstruct scheduling/completion state, or redoing accepted computation. Instrument the producer/application iterator as well as the Dagu coordinator so eager input work cannot hide outside the scheduler. A genuinely new/retried unit may load its own input; report that cost separately. Preserve explicit remaining/failed/pending states. |
| **Add/remove workers and future policy** | Add a compatible worker while other units run and show it receives eligible work. Drain/remove one worker, then simulate abrupt loss. Observe child processes and host resources, not just UI state. Apply future thread/concurrency/profile changes without restarting unrelated jobs. Bound resubmission/duplicate cost without resetting consumed unit retry budgets or absolute deadlines under new platform attempt IDs; no retry storm, orphan accumulation or inference that heartbeat failure proves physical exit. |
| **Kaggle adapter and package limits** | Map the real submission/runtime/data/result channels and their separately verified limits. The historical demonstrated 1 MB embedded source limit is not evidence of one universal binary/data limit. Compare actual Linux-target executable/runtime sizes, compression and required files with the intended channel; keep this distinct from Dagu's 64 MiB compressed / 256 MiB extracted dependency-bundle limits. Determine whether existing finite bootstrap/submit/collect/validate commands suffice, new study-specific adapter code is needed, or a Dagu binary in the notebook is unnecessary. Never assume an offline notebook can reach the LAN coordinator. No real upload/run is implied by this check. |
| **Offline result survival and merge** | Simulate finite batch execution without coordinator access, incremental per-unit publication, termination before the final session envelope and later collection. Import complete results twice through the same application validator/publisher; retain useful units and leave missing ones missing. Finish package aggregation after local/offline results combine. Test retrievable output layout and file-count/collection behavior, not only local writes. This does not certify Kaggle's actual retrieval API or expiry behavior. |
| **Publication crash, duplicates and retention** | Race two valid differing attempts, interrupt one after publication but before scheduler acknowledgment, and retry under a new run identity. No partial canonical result or overwrite of the first accepted result. Test cleanup/retention of execution history without losing accepted study artifacts; stale input/runtime identities must not reuse old results. Keep large bytes out of step-output variables. |
| **Controller responsiveness and finite handback** | While payload transfer/validation/preparation is slow, service worker reports, collection and stop/deadline handling. Measure these phases separately from synthetic compute and restart bookkeeping. At cutoff verify whole-tree/resource behavior and saved outputs; unreachable/pending delivery remains explicit rather than falsely drained. |

**Gate C — account for every missing piece:** classify each requirement as (1) native Dagu feature/configuration, (2) existing study command, (3) new small study-specific command, or (4) new general control-plane/executor machinery. Publish the actual YAML/command map, test outcomes, measured overhead, failures and unresolved platform gaps. No additional proof archive/framework is needed; ordinary test output and source-controlled definitions suffice.

The desired result is **Dagu plus instructions for a study designer/planner**, with no Hermes software if existing study tools cover the application boundary. Planner guidance should cover stable identities, explicit dependencies, atomicity/loss bounds, profiles, durable publication, aggregation, retries, offline exchange and stop limits. Do not promise literally zero application code: Dagu does not implement the scientific calculation/validator, and its connected workers do not themselves supply a Kaggle finite-batch adapter. If a missing command is study-specific, add it at the application boundary rather than creating a generic wrapper by reflex. Reconsider a Hermes software component only for a demonstrated recurring gap that cannot reasonably stay in study definitions/commands.

If those pass with small application adapters, omit the custom Hermes coordinator, agent, control database and session protocol. If adoption requires recreating them to satisfy a specific retained requirement, identify that requirement before choosing a wrapper or returning to a custom build. No installation, dependency change, deployment, live-job control or scientific inspection was performed for this assessment.

## 11. First native Dagu trial — November 23

**Executed, bounded synthetic observations; not WP05D admission.** The operator authorized a small trial and explicitly directed its files to an ignored in-project location after an attempted outside-project cache write was denied. `/.local-trials/` was added to `.gitignore` before any fixtures/binaries were created. Trial scripts, executable, application results, logs and runtime state remain under `.local-trials/dagu-v2.27.0/`, outside source control. No installed services, Perry jobs/data, remote-host deployment or Kaggle submission were used.

### Release and setup

- Released **v2.27.0**, native Darwin/amd64, rather than the source-review checkout `v2.27.0-178-g5bf6cd741`. The GitHub release archive SHA256 matched `c34102b3d6e9225560e771e0e94ca5a172e76ff270ec7a472bd9260af0bc7b52`, and the extracted executable reported `2.27.0`.
- Archive: **65,530,594 bytes**; executable: **203,086,608 bytes**. These are Mac artifact sizes, not measurements of the Linux/Kaggle build or evidence that any Kaggle transport channel admits them.
- Coordinator/UI bound to loopback ports 50065/18080, with separate local worker homes, explicit peer token and at most two simultaneous workers, each admitting one run. No VM, container, database or broker installation. HTTP auth was explicitly disabled only in the isolated loopback trial; the initial default-builtin-auth startup correctly returned HTTP401 and its harness stopped/cleaned up before adjustment.
- Synthetic foreground Python commands used a shared **1 MiB input** and second-scale sleeps. Each application unit wrote a validated one-file candidate and atomically linked the first accepted reference, then skipped computation when that accepted reference already existed. This was trial application logic, not a built-in Dagu acceptance/cache guarantee. Application runtimes are additional to the Dagu executable.

### Observed behavior

| Check | Result / limitation |
|---|---|
| Native coordinator and distributed worker mode | Worked locally: distinct worker IDs `trial-w1`/`trial-w2`, both reporting darwin/amd64 and v2.27.0. Two independent root runs were concurrently running on different workers. This is one physical Mac, not a multi-host LAN test. |
| Brief coordinator crash/restart | SIGKILL of the coordinator followed by restart after about one second preserved the original two application PIDs and single starts. Both Dagu runs reached succeeded. This does not establish survival of an arbitrary outage. |
| Published-result reuse across fresh run identity | A new Dagu run for the already accepted application unit emitted `reused` without another input read. This demonstrates the adapter boundary; it does not measure 144-file/25.75 GiB recovery or instrument Dagu's filesystem reads. |
| Worker removal/addition | An idle worker stopped and a differently identified worker registered while the coordinator stayed up. Later units ran. Active drain/policy replacement on real remote machines remains untested. |
| Resource failure and bigger lane | A synthetic command exited with classified code42. An **explicit harness resubmission** of that application unit through a separate `big` queue completed. No actual memory exhaustion, automatic escalation classifier, larger physical allocation or native enforcement was tested. |
| Same-host exclusivity | **Not supplied by separate queues:** a normal-queue run and big-queue run overlapped on the same Mac despite respective queue limits. This reproduces the admission gap, not a successful exclusive-memory retry policy. |
| Aggregation | The application harness assembled the exact four expected accepted unit IDs after their completion. A Dagu-scheduled reducer, missing/invalid-result barrier and actual WP05D stage graph were not exercised. |
| Worker SIGKILL | In two observations the tracked foreground application PID was absent two seconds after killing its worker; no accepted result or graceful termination event was published. Coordinator metadata could still show running immediately afterward. Released-source review identifies a Unix parent-exit watchdog as a mechanism consistent with this observation; it does not establish protection of descendants that escape the original process group. |
| Longer coordinator outage | During a **105.39 s outage**, the worker logged owner-unreachable cancellation at about90 s. The tracked application PID was absent by105 s, no result had been accepted, and the restarted coordinator marked the run failed with `distributed run lease expired`. This confirms finite owner-connectivity behavior in this release, not independent offline execution. Automatic replacement was not configured/tested. |

### Failure and cleanup limits

The first failure harness incorrectly waited for a graceful completion/termination event after hard worker death and timed out. A corrected harness observed the missing child but then timed out starting another normal-queue run while failed-worker runs still held running metadata/queue slots. These are retained unsuccessful observations, not silently counted as passes or a throughput benchmark. A fresh isolated controller state was used for the long-outage check.

**Permission-sensitive cleanup remains unresolved; no further runtime attempts planned:** the long-outage log reported `Failed to stop step: operation not permitted`, followed by aborted worker state; the tracked application PID was nevertheless absent at the later observation. Read-only **v2.27.0** source review traced `Node.Stop → commandExecutor.Stop → ManagedProcess.Stop → syscall.Kill(-pid, signal)`. There is no synthetic own-process-group rejection on that path: this is an actual OS/security-layer signal denial, not an intentional Dagu safety guard. Source does not identify whether the cause is the sandbox or another OS policy. `Node.Stop` marks aborted before signaling, so terminal metadata does not prove the signal succeeded. Do not change permissions or try alternative signal mechanisms to make the test pass without operator guidance.

The worker did not exit within the harness's12 s cleanup wait (with5 s worker drain configured), and normal harness cleanup forcibly stopped its directly owned process group. Reporting/cleanup waits also exist; this timing alone does not identify the cause. All directly launched coordinator/worker processes were reaped. No alternative signaling workaround for the denied Dagu operation was attempted, and arbitrary orphan-tree cleanup remains uncertified.

**Released Unix parent-exit containment:** `internal/cmn/cmdutil/parent_exit_watcher_unix.go` starts a separate shell watchdog with a pipe. Parent death closes the pipe; absent the normal release token, the watchdog sends SIGKILL to the managed command's process group. This explains why worker death need not leave the foreground command running and is consistent with the observed missing PIDs/no cleanup events. It corrects the earlier assumption that no cleanup can act after worker SIGKILL. However, the watchdog suppresses signal errors and cannot cover descendants that leave the group; neither source nor this small trial establishes universal containment. Sources were inspected through the operator-provided Dagu agent, without modifying its checkout.

The main concurrency/reuse trial completed in **50.71 s**; the long-outage observation in **124.70 s**, including cleanup. Those are harness elapsed times, not scheduler performance claims. Trial artifacts include `report.json`, the unsuccessful `failure-report*.json`, `lease-report.json`, per-process logs, YAML and application events, all ignored. Source-controlled findings are this section; the fixtures are not a reusable Hermes implementation.

### Consequence for the adoption decision

Native Mac dispatch, short-restart continuity, small referenced units and application-owned result reuse are credible. **Instructions/YAML alone have not yet demonstrated the parallel-to-exclusive retry policy**, and long disconnection is not the connected worker mode's contract. The largest remaining questions are shared-host admission, application publication/aggregation interfaces, permission-sensitive cleanup, and a faithful mapping of WP05D's preparation/stage graph. Kaggle command glue and packaging limits remain documentary/unexercised. No decision to build a wrapper or adopt Dagu follows from this first trial.

## 12. Literal source extraction from Dagu v2.27.0

The operator means **copy selected source into Hermes and maintain it independently**, not depend on a library or follow upstream interfaces. Go's `internal/` visibility and Dagu's experimental embedding API therefore do not obstruct this approach. The relevant questions are coherent extraction boundaries, dependencies, tests, defects and maintenance cost. No source has been copied or modified; this is a read-only review of the released **v2.27.0 tag**, not the newer checkout HEAD.

Dagu is modular internally, but taking whole packages would often carry unrelated runtime/configuration vocabulary. Its public embedded engine also registers many built-in executors and is not a smaller replacement for the code we actually need. Prefer a selected mechanism plus its failure-path tests over preserving Dagu package boundaries.

| Candidate | Copy/adapt verdict | Boundary and cautions |
|---|---|---|
| **Durable no-replace publication** | Highest-payoff small extraction | `internal/cmn/fileutil/fileutil.go`: `WriteFileAtomicExclusive`, `WriteFileAtomic`, `ReplaceFileDurable`, `RemoveFileDurable`, `WriteJSONAtomic`; plus the narrow Unix replace/sync helpers. Core roughly100 lines plus platform helpers, using standard-library temp files, sync, hard-link no-replace installation and directory sync. Extract only needed functions, not the package's cache/log/encoding utilities. Same-filesystem and filesystem durability assumptions remain explicit. This is a primitive for canonical commitment, not scientific validation or an atomic multi-file result set. Adapt `atomic_test.go` contention/existing-file/publication tests. Do not take the artifact-handler overwrite semantics instead. |
| **Mac process lifecycle and parent-exit watchdog** | Worth copying/adapting if the implementation needs it | Narrow slice of `internal/cmn/cmdutil/{lifecycle.go,lifecycle_unix.go,parent_exit_watcher_unix.go,cmd_unix.go,termination.go}`, with only necessary signal constants. Several hundred lines and focused tests, not merely a `Kill` wrapper. Retain process-group setup, pipe-based watchdog, idempotent release and ownership/lifetime semantics; strip unrelated shell parsing or generic control vocabulary. Propagate signal errors, bound cancellation waits and do not normalize the observed EPERM into success. Escaped process groups and silent watchdog kill errors remain limitations. Useful tests include `TestManagedProcessForceStopTerminatesCommand`, `TestManagedProcessReleaseIsIdempotent`, `TestStartManagedProcessCleansUpAfterContainmentFailure`, `TestParentExitWatcherTerminatesChildWhenParentExits` and `TestParentExitWatcherIgnoresInheritedShellTimeout`. Native containment is still uncertified. |
| **Backoff helpers** | Little reason to copy | `internal/cmn/backoff` has small policy/jitter code but its retry wrapper imports Dagu logging. A tiny local loop or independently chosen upstream helper is simpler than carrying those dependencies. Operational backoff is not a durable unit retry/escalation budget. |
| **DAG planning/retry runtime** | Do not extract wholesale | `internal/runtime/plan.go` depends on Dagu IR, nodes and persisted step state; the node/runtime dependency fanout pulls much of the engine. Its basic cycle traversal is only roughly30 lines, so importing the surrounding model does not save work. Independent units plus later aggregation may eliminate most graph machinery. Use cycle/dependency/retry test scenarios as requirement input rather than carry Dagu's state types. |
| **Worker/coordinator RPC, leases and persistence** | Do not extract wholesale | Worker/client/remote-handler components total thousands of lines with substantial config, logging, IR, lease, protocol conversion, registry and runtime coupling. The protobuf schema encodes Dagu's root/child attempts, snapshots, status and provider cleanup. Copying this is adopting much of the architecture we are considering replacing. Learn from stale-claim, ambiguous dispatch and rolling-replacement tests; design only the retained Hermes contract. It supplies neither the missing same-host exclusive admission nor our desired coordinator-independent lifetime unchanged. |

There are no hidden special libraries behind the two leading Mac mechanisms: their core uses `os/exec`, context, process-group signals, pipes, temp files, `Link`/`Rename` and file/directory sync. Upstream packages used elsewhere by Dagu—such as `gofrs/flock`, `fsnotify`, `gopsutil`, `x/sync` and `robfig/cron`—can be evaluated independently **if a concrete need remains**, rather than copied through Dagu. Their licenses/versions need independent review before adoption. File locks are not a scheduler, metrics are not enforcement, and existing SSH deployment should not be replaced merely because Dagu contains an SSH executor. Hermes currently has no implementation; this is not a claim that it already has retry/queue/process primitives.

**License boundary:** Dagu's `LICENSING.md` and the candidate file SPDX notices identify **GPL-3.0-or-later**; no special copying/embedding exception was found for these candidates or their tests. Moving, renaming and independently maintaining copied source does not remove those conditions. Private use and distribution are different questions; choose Hermes's licensing/distribution policy before taking code and preserve applicable notices. This is not a finding that extraction is forbidden or that every separately communicating application must use Dagu's license. No licensing choice is inferred from the request to investigate reuse.

**Recommendation:** shortlist only publication and native lifecycle/watchdog source plus selected tests. Confirm the required Hermes interfaces and licensing decision before copying. Do not begin a fork, import the graph engine, or recreate Dagu's worker protocol merely to reuse those small components. The original native trial's unresolved signaling denial remains a blocker to certifying copied lifecycle behavior, not a reason to conceal it in an abstraction.

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
