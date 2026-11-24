# Hermes — first-release design

Architecture and behavioral contracts for implementation. This document specifies intended behavior, not implemented capabilities. Delivery sequence and temporary progress belong in the [implementation plan](implementation-plan.md); coordination rules belong in [COORDINATION.md](COORDINATION.md). Historical investigations and design provenance belong in [discovery](distributed-execution-discovery.md), not this contract.

Implementation, deployment and application execution follow the operator's actual release. This document alone does not authorize remote access, data movement, cloud submissions or Perry job control.

## 1. Design in one paragraph

Build a Go controller, Go worker agent and Go CLI. One controller owns a local transactional metadata database and immutable artifact directory. Independently supervised agents own application processes and persist issued work/results locally. Agents pull bounded assignments, run an application-owned executable, publish results per application-defined unit, and reconnect without restarting computation when the controller disappears. Scheduling uses compact metadata, not input payloads. A finite agent runs the same assignment/result contracts without live controller connectivity. The controller schedules Kaggle alongside LAN resources through a platform adapter that stages, submits, tracks and collects finite notebook sessions. Explicit export/import remains available for manual exchange and recovery, not as the normal Kaggle operating path. Start with one controller host, connected LAN agents and managed finite Kaggle execution—not a broker, general cloud management platform or shared numerical runtime.

## 2. Scope, guarantees and limits

### Required invariants

1. A unit's scientific/business meaning and immutable inputs do not change when its package, host, resource profile or attempt changes.
2. An accepted unit has one canonical acceptance record referring to durable result bytes and a successful application-validation decision. Process exit or file receipt is insufficient.
3. No normal reassignment while a previous owner might still start or execute the unit. Disconnection, timeout and a stale heartbeat do not establish quiescence.
4. Worker computation does not depend on a controller process, interactive CLI or chat agent remaining alive. Work continues only within the finite assignment already issued.
5. Drain prevents future starts at an acknowledged boundary. Completion additionally requires process quiescence and an explicit result/remainder inventory.
6. Status, scheduling and recovery use verified identities and indexed records. They do not deserialize, hash or revalidate all accepted payloads.
7. Operational failures remain operational failures; Hermes never infers a scientific negative from missing work, memory exhaustion or a deadline.

### First-release boundary

- One native macOS coordinator host, with no Linux VM/container requirement. Supervise/restart its process automatically. Durable state survives controller process failure; issued remote work continues through coordinator outages, but new scheduling waits while its host is unavailable. Host/disk disaster recovery is manual, not automatic failover.
- Agents are trusted executors, not a sandbox for hostile applications. Workloads have no non-idempotent external side effects in the supported execution contract.
- Atomic units and domain validators are application-defined. No arbitrary optimizer checkpoint/resume or mutation of an executing unit's resource profile.
- Native macOS LAN agents and managed finite Kaggle CPU sessions. Schedule, stage, submit, track and collect Kaggle work within approved capacity/budgets; manual file exchange is a fallback, not the normal operating path. Connected agents and finite sessions share work/result semantics, not live connectivity.
- Windows/WSL and GPU execution are outside the first release. Windows build/service examples describe a future contract, not a current deliverable. Declare supported OS/process-control/resource capabilities from exercised behavior, not compilation alone.
- No active-active controller, distributed database, automatic controller-host failover, work stealing, autoscaling or optimization-based placement.

## 3. Components and ownership

```text
application producer / operator
              | CLI or versioned metadata API
              v
 controller (one authority) ---- local SQLite metadata
       |       |                 immutable artifact directory
       |       +---- bounded application validator processes
       |
       +---- authenticated agent pull / reports / artifact transfer
                     |
              independent Go agent
              durable local assignment/result spool
                     |
              application executable (any language)

 controller -- Kaggle adapter: stage/submit/track --> finite Go agent
 controller <-- collect result inventory + blobs -- Kaggle adapter
 controller <---- explicit export/import ----> manual finite exchange
```

**Controller:** run/work catalogue, dependency readiness, unit ownership, scheduling, resource reservations, policy revisions, raw publication, validation queue, canonical acceptance and operator control.

**Agent:** durable assignment receipt, local admission, application process tree, unit-start gating, artifact staging, local result publication, finite deadlines, drain and reporting. Use one agent per host, with a host-local exclusive service lock, so aggregate admission and device ownership have one authority. A controller service manager must not own worker process trees. Agents run under their own service lifetime.

**Application:** immutable unit definitions, compatible runtime artifacts, preparation/reuse within a package, calculation, domain result schema and validator. An application supplies fixed execution graphs and deterministic continuation logic for authorized later phases. Continuation runs unattended through the chosen orchestration mechanism, not a live Pi/LLM or manual forwarding step. Hermes does not import Perry modules, reconstruct research state or choose scientific branches.

**CLI:** a replaceable API client for submit, status, policy, drain, reconciliation and batch exchange. Exiting the CLI has no execution effect.

## 4. Durable storage and authority

### Controller storage

Use SQLite on a supported local filesystem for small records and transactions; store large artifacts in a sibling immutable directory. Do not place the database on a network share. Select and document the Go driver and supported SQLite build when implementing controller storage.

- Acquire an OS-held exclusive controller lock for the state root before opening service authority. Hold it for the process lifetime. A second controller fails closed. All metadata writers, including CLI operations, go through that authority.
- Use durable transactions, foreign keys and unique constraints; acknowledge metadata only after commit. Database and artifact durability settings must be tested against the declared process-crash boundary.
- Artifact publication streams into a private temporary file **within the artifact filesystem**, verifies declared length/digest, syncs bytes, and publishes atomically without replacing existing immutable content. Sync directory metadata before acknowledging durable publication where required by the platform.
- Commit metadata references only after referenced blobs are durably published. A crash may leave an unreferenced blob, never an acknowledged reference to incomplete bytes. Garbage collection is deferred; retain artifacts in the first release. Incomplete ingress scratch
  is not a published artifact: an exclusive store owner may remove its reserved temporary files
  after establishing the previous publisher process is gone, before new publishers start. This does
  not establish application/validator child quiescence or authorize reassignment.
- Acceptance is one transaction inserting the unique `(run_id, unit_id)` record and advancing dependency/readiness metadata. Persist validator identity and its decision. Do not rerun validation after acceptance during ordinary recovery.

The state root is the authoritative failure domain. If it is lost, Hermes does not claim that a worker spool reconstructs the entire controller. Manual restore/move must first establish the old controller cannot act and reconcile agents; restoring an old database snapshot while workers run is not a supported automatic recovery path. Backup/restore procedures are a separate prerequisite for a deployment promising machine-loss recovery.

### Agent storage

An agent persists a stable worker ID, assignment/session identities, control revisions, started units and per-unit result manifests. Worker identity is provisioned locally, not freshly minted on every reconnect. An assignment's execution-session identity survives an agent-process restart; it is not the identity of the current agent process. A restarted authenticated agent reports retained assignment IDs, start records and results for reconciliation with controller state before new execution. Restart does not itself authorize replay of previously started work. Result blobs remain in its spool until controller raw-publication acknowledgment, not merely network-send completion. Retain unacknowledged bytes across agent restart if the disk survives. Persisted execution journals
must carry explicit non-null lifecycle facts; absent/null start, quiescence, uncertainty or final-ack
fields are incomplete evidence, not a default “never started.” Local inspection/recovery must reject
ambiguous mixed legacy/active layouts and unindexed execution metadata rather than letting one known
receipt conceal another unknown execution. Historical terminal payloads need not be scanned.

Disk-full or publication failure stops new unit starts and reports an operational error. Do not compute indefinitely without a result-retention path. Worker-local persistence survives process loss, not machine/disk loss. Controller-acknowledged raw results survive worker loss; acceptance may still be pending.

A host-local retained-byte/verified-cache index is resource bookkeeping, not another controller
acceptance catalogue or scheduler. It cannot grant execution rights. Files still on disk remain
charged after raw acknowledgment and archival; only explicit deletion under retention policy could
refund their bytes. Stable archive keys must not conceal simultaneous duplicate physical files.
Verified-cache status requires successful durable publication plus its durable index commit, not
mere path visibility. Ordinary accounting/status reads metadata rather than historical payload bytes.

## 5. Data contracts

Version metadata envelopes (`schema_version: 1`); encode control messages as bounded JSON. Reject unknown incompatible versions. JSON contains references and bounded manifests, never embedded large input/output payloads. Transport schemas should follow these contracts rather than inventing another domain model.

| Record | Essential fields / meaning |
|---|---|
| Run | `run_id` identifying a fixed scheduler submission, immutable graph reference, application logical-workflow reference, application contract/validator revision, execution state, current policy revision |
| Artifact | SHA256 digest, byte length, declared media/schema type; locations and access permissions are separate mutable metadata |
| Unit | application `unit_id`, immutable spec digest, input artifact references, operation/runtime compatibility, dependency IDs, required capabilities |
| Package | immutable ordered unit references plus reusable preparation references; a scheduling envelope, not an acceptance unit |
| Policy revision | eligibility, permitted profiles, concurrency/aggregate budgets, retry limits and operating windows; immutable revision once issued |
| Assignment | unique `assignment_id`, target worker/session or finite bundle ID, unit IDs, package reference, selected runtime/profile, policy revision, deadlines |
| Platform session | durable local session ID, member assignment IDs, submission intent/state, exact external reference/version, capacity reservation, original deadlines and collection state; one platform owner for all members |
| Host inventory / agent registration | target OS/architecture and execution environment, agent build/version, observed platform capabilities/resources and separately approved admission limits; see section 14 |
| Candidate result | `unit_id`, assignment/attempt identity, immutable result manifest and blob references, application outcome tag |
| Validation | candidate digest, validator contract/runtime revision, operational state and explicit application verdict |
| Acceptance | unique run/unit key, canonical candidate digest and validation reference |
| Control | target, monotonically increasing revision, action, deadline and persisted acknowledgment |

The application chooses stable unit IDs; Hermes binds each to an immutable spec digest on registration. Re-registering an identical unit is idempotent; the same ID with different meaning is an error. Unit specs reference previously published input digests instead of embedding bytes in an identity computation. Runtime compatibility and validation meaning are pinned by the application contract; resource profiles are not calculation identities. If changing numerical settings changes meaning, the application must issue a different unit/spec.

SHA256 verifies artifact bytes at ingress/staging boundaries. A published immutable artifact's digest is then ordinary metadata. Local verified-cache entries may be reused while their storage remains protected against mutation. Corruption recovery or explicit integrity audits can reverify bytes; routine status/lookup cannot.

## 6. Work and attempt state

Do not use one overloaded status to describe computation, delivery and acceptance.

- **Unit:** `blocked → ready → owned`; candidate delivery/validation may follow; `accepted` is terminal for ordinary execution. Unsuccessful attempts can return an unaccepted unit to `ready` only after ownership and publication reconciliation.
- **Assignment execution:** `issued → acknowledged → running → quiescent`, or `uncertain` when ownership cannot be established. `issued` already reserves its units, even if the response was lost.
- **Delivery:** local-spooled, transfer-pending, controller-published, or delivery-failed. Quiescent execution may still have pending delivery.
- **Candidate:** published, validation-pending, validating, valid, or rejected. Acceptance is recorded separately; differing outputs are a reported conflict, not a validation-blocking state. A validator crash is not a domain rejection.

Use relational constraints to enforce one unresolved owner per unit and one acceptance per unit. Readiness is an indexed/materialized metadata property updated on registration and acceptance, not discovered by rereading every historical result.

A valid application outcome may be “no solution” if its validator accepts that conclusion. It still counts as a completed unit; required scientific/numerical unavailability states are preserved too. Within a fixed graph, readiness follows declared accepted prerequisites, not process exit; a reported difference between candidates does not block an accepted prerequisite. Outcome-dependent selection of later work belongs to the application's continuation logic, not an implicit Hermes branch interpreter.

### Fixed graphs and unattended continuation

First release uses fixed scheduler submissions: assemble and seal the units, packages and dependencies before issuance; do not append new units to an executing graph. An application may submit a subsequent fixed graph based on earlier accepted results and a validated decision/freeze artifact. The logical assay/workflow identity can span several scheduler submissions. Refer to original acceptance records/artifacts and preserve calculation identities rather than recomputing or counting earlier work again. When all required decisions are canonical acceptances, submitting a new graph of new units does not require old execution to be quiescent; its unresolved owners and resource reservations remain honest. Terminal-driven continuation without accepted outputs requires explicit typed application/operational facts, not an empty ready queue, missing payload or parsed diagnostic reason.

Routine authorized continuation must run automatically without a live Pi/LLM, manual intermediate forwarding, review timer or approval state. The application deterministically evaluates accepted outputs and explicit terminal/availability states, then submits the next graph or records its prescribed stop. Use the chosen orchestrator and existing durable settings/results plus idempotent submission requests; do not build a second scheduler or autonomous research planner. Persist the intended successor and its request ID so recovery reconciles an already-created submission rather than creating a duplicate. Respect workflow-level explicit stop/resume and any actual cumulative budget across all submissions; automatic restart does not authorize a stopped workflow to continue.

The application owns stage/data-access/freeze rules, numerical refinement and phase continuation. Preserve cumulative elapsed/progress history across submissions. An external human review is not a programmed stop: impose a whole-workflow deadline only when actually configured. Finite assignment/session/orphan bounds still apply. A numerical kernel or bounded probe is not a complete application continuation interface.

## 7. Connected protocol and scheduling

Use versioned HTTPS requests, agent-initiated long polling and bounded uploads/downloads. No message broker or worker-to-worker transport is required. Use provisioned mutual TLS identities for controller/agents and authenticated operator access. Credentials restrict worker identities and permitted artifact/run access; they do not make arbitrary submitted code safe. Bind controller identity/address through deployment configuration.

### Assignment exchange

1. Agent reports its session, capabilities, capacity, existing assignments and pending result inventory.
2. Controller transactionally selects ready units, reserves resources and writes an assignment **before** returning it.
3. Agent writes the assignment to its journal before acknowledging or starting. Duplicate delivery of the same ID returns the existing receipt; it never starts another process.
4. A lost acknowledgment leaves the assignment owned. Reconciliation asks that agent about the same ID; it does not dispatch a substitute.
5. Agent starts a bounded package session and durably records each unit-start grant. It can continue the issued finite package while the controller is unavailable.
6. Results are published per unit; reports are retryable and idempotent. On reconnect, agent sends pending deltas/inventories, not every completed payload.

Reports carry assignment IDs, session IDs and monotonic report/control revisions. Reject mismatched identities and ignore stale revisions. Persist outbound control intent before sending it. The single controller lock prevents concurrent authorities in this deployment; monotonic revisions handle delayed messages, not distributed split-brain. Do not claim a lease or epoch permits safe automatic host failover.

### Simple scheduler

Maintain indexed ready queues and unresolved assignments. Select eligible units in stable ready order, preferring an existing permitted artifact cache only among otherwise eligible choices. Enforce hard data-location/capability restrictions before locality preferences. Application-defined package sizing bounds unit count, runtime envelope and staged bytes; it must not create an unbounded grant of future work.

Resource reservations include preparation, application processes, native children, transfer and validation overhead. Enforce aggregate host budgets as well as per-assignment profiles. GPU placement reserves device IDs and declared VRAM; a GPU label does not turn CPU work into GPU work. Unknown demand requires an explicit conservative/operator-approved profile, not a guessed estimate.
Use explicit execution environments rather than inheriting service credentials. Project the approved
CPU reservation into Go and common numerical-library thread hints (ceil of CPU milli/1000); these
cooperative settings do not establish an OS CPU quota or universal native-thread enforcement.
Controller-owned validators and continuation adapters also consume approved host resources. A
controller crash or released metadata-root lock does not prove those child processes have exited;
persisted started claims remain reserved/uncertain until observed exit or a qualified terminal boundary
establishes otherwise. Dead controller-local timers are not orphan termination proof. Expose resulting
capacity waits honestly rather than silently starting replacements outside the aggregate budget.

Policy updates affect unassigned work immediately. Already issued assignments retain their policy/profile, including not-yet-started units in that assignment; changing them requires acknowledged drain/handback and reissue. Smaller finite assignments improve policy responsiveness. This explicit boundary avoids mutating a running solver or silently revoking work it may start while disconnected.

Resource-stop recovery is ordinary scheduler behavior, not a per-failure operator decision. After confirming the old execution is quiescent and reconciling retained results, automatically reschedule the unfinished unit among eligible operator-approved placements: a larger allocation with exclusive Hermes computation access on the same worker, or a compatible worker with sufficient available approved memory. Exclusive placement must actually permit a sufficient allocation; removing neighbours while retaining the same inadequate limit is not a remedy. Exclusivity covers Hermes-managed computations, not OS or unrelated application usage. Preserve unit identity and completed results; repeat preparation only as required by the new session/placement. Select a suitable permitted placement with the existing simple eligibility/order/locality rules, not a new optimization engine.

Do not repeat a resource-stop retry under the same inadequate allocation. Retry limits and backoff are recorded metadata; do not loop indefinitely. If no approved placement currently fits, leave that unit waiting for suitable capacity and continue independent eligible work. If its retry budget is exhausted or no approved profile can satisfy it, record a specific unresolved resource failure without pausing unrelated work. Operator action is needed to change approved resource limits or retry policy, not to select each ordinary retry placement.

### Public control surface

Expose the same logical operations through CLI and versioned API: publish artifact; submit/seal a fixed run graph with its units and packages; inspect run/unit/assignment; revise policy; drain worker/run; resume run/worker eligibility; export assignment/import results; and explicitly resolve uncertain ownership or a result conflict. Mutating requests carry a client request ID persisted with their outcome, so retrying a lost response cannot create a second export or control action. Requests pin the run and relevant revision; reject stale conflicting changes rather than silently applying them. Agent registration, assignment polling, inventory reporting and artifact transfer are separate worker-authorized operations.

A run moves `active → draining → paused` after confirmed handback, or remains `draining` with explicit pending/uncertain reasons. `resume` reopens issuance for eligible unowned work; it does not reverse a drain already issued to an assignment. Surviving assignments keep their recorded controls, and a drained agent becomes eligible again only through a newer acknowledged control revision. `completed` means all required units in the sealed graph are accepted and all issued execution is reconciled/quiescent. Report acceptance-complete separately if handback is still pending; an empty ready queue alone does not establish completion. An application continuation step polls indexed accepted/terminal metadata to submit later graphs automatically; no second notification bus is required. Completion of one scheduler submission is not automatically completion of its application workflow.

## 8. Application executable boundary

A package session is an application process, launched by the agent with an explicit executable/argv, runtime artifact reference, working directory, bounded environment and local manifest path. No shell-string interpolation or infrastructure imports of Python/C++ libraries. The application owns compatible builds for each advertised platform.

For reuse without losing unit-boundary control, use a small newline-delimited JSON protocol over stdin/stdout. Stdout is protocol-only; stderr is a separately bounded log stream. Large values go into files, never protocol messages.

- Application emits `ready` after preparation. Preparation itself is cancellable operational work under the assignment budget.
- Agent sends `execute_unit(unit_id)` only after recording the start grant. At most one unit is outstanding per package process; parallelism uses multiple admitted sessions.
- Application must not begin another unit autonomously. It writes result files to its private output area and emits `unit_result(unit_id, manifest_path)` after closing them.
- Agent verifies envelope/declared artifacts and publishes the immutable local result. Application output is not automatically a domain-valid result.
- Agent acknowledges local publication before granting the next unit. On drain it sends `finish`, grants no next unit, collects the remainder and closes the session.
- Unexpected process exit, malformed protocol, undeclared unit or missing bytes is an operational/protocol failure. Preserve already published results. An exit-zero flag cannot invent a missing unit result.

Package preparation can stay resident across units. This small cooperative protocol is chosen over opaque multi-unit processes because resident preparation and independently drainable units must coexist; no in-process plugin system is needed. Applications that do not need reuse can use a trivial one-unit executable adapter. Controller-side validators use the same process-isolation principle, with a smaller input/result verdict interface, pinned runtime, explicit resource/time limits and no execution inside a database transaction.

Application compliance is part of admission: the agent cannot infer safe boundaries inside an arbitrary native optimizer. First release supports side-effect-free calculations; exactly-once external effects are not promised.

## 9. Publication, validation and conflicts

1. Agent locally publishes a complete per-unit manifest and referenced bytes before announcing a candidate.
2. Controller receives missing blobs through bounded transfer handlers; verifies/publishes each once. It durably records the candidate and acknowledges **raw publication**.
3. A bounded validation queue runs the pinned application validator. Controller scheduling/heartbeats never wait synchronously for it. Validators may inspect large results; this is explicitly measured work.
4. The first application-valid candidate to commit acceptance atomically establishes the canonical result and dependency changes. Concurrent successful verdicts cannot overwrite that acceptance. Rejecting a candidate preserves the verdict and bytes; further computation follows the recorded retry policy, not an unconditional recomputation.
5. Agent may discard its transferred copies after raw-publication acknowledgment under retention policy; it need not wait for domain acceptance. It must not delete unpublished results on controller disconnect.

Accept the first candidate that passes application validation and commits acceptance; do not pause acceptance or independent work merely because another candidate differs. Validation must establish suitability for downstream use under the pinned unit/spec, including its exact assigned keys, input/history identities and required numerical checks, not just file/envelope correctness. First-valid acceptance chooses a candidate for the same calculation; it must not become model selection based on which scored output wins a race. Arrival order, retry status and worker identity are not reliability scores; Hermes does not invent one.

Identical candidate redelivery is idempotent. Preserve differing candidates and report `conflict` without requiring operator resolution before progress. Validate candidates independently; a known difference does not prevent a valid candidate from becoming canonical. Never automatically overwrite canonical acceptance or invalidate downstream work because a later candidate differs. An application may supply a pinned equivalence check if useful, but it is not a default gate. Reuse retained candidates before starting another computation. If none passes validation, dependent work remains blocked for lack of a valid result; independent eligible work continues, and missing work follows authorized bounded retry policy.

Late results from a known prior attempt remain candidates for the same immutable unit, not new work. Validate them normally. If they establish acceptance while a retry is live, request that retry's drain/stop, preserve honest ownership until acknowledged, and do not claim zero duplicate execution. A forced retry of uncertain work is therefore an exceptional operator override that records possible duplicate execution, not ordinary recovery.

## 10. Crash, disconnect and reconciliation

| Event | Required behavior |
|---|---|
| Controller exits while unit runs | Agent keeps its process and finite assignment; spools results. Replacement controller takes the same root lock, reads unresolved metadata and reconnects. No worker restart. |
| Assignment reply/ack lost | Resend/query the same assignment ID. Reserved units are not assigned elsewhere. |
| Agent disconnects | Mark contact stale and ownership uncertain; do not equate this with termination. No automatic duplicate work. |
| Agent process crashes | Preserve spool. On restart reconcile its local process boundary before new starts. Adopt only where explicitly supported; otherwise stop and verify the owned tree. If quiescence cannot be established, remain uncertain. |
| Worker machine dies | Controller-published bytes remain usable. Lost in-flight work is retried only after machine/attempt termination is established; collect surviving spool first if accessible. |
| Crash after blob publication, before candidate/acceptance transaction | Agent retries idempotently. Unreferenced immutable blobs are harmless. No full output replay. |
| Validator or transfer fails | Keep pending state, retry with bounded backoff where operationally appropriate, and continue other scheduling. Invalid domain verdicts are not transport errors. |
| Database disk full/unavailable | Stop new issuance and acknowledgments; surface a controller failure. Agents retain existing finite work/results. Do not substitute in-memory authority. |

Recovery loads active assignments, unresolved delivery/validation/control records and indexed ready work, not all historical artifact bytes. Agents reconcile exact assignment IDs and manifest digests. Establish compatibility from recorded contracts/manifests once, then reuse compatible completed preparation, fits/checks and product subresults without replaying or revalidating the accepted prefix. Preparation is application-defined reusable work/artifacts, not an infrastructure-owned numerical cache. Accept retained candidates before deciding whether unaccepted units need another computation. Restarting incomplete numerical work preserves the application's prescribed initialization; resource recovery does not authorize changing scientific settings, numerical acceptance or valid unavailability outcomes.

For a connected assignment with a surviving spool, retry requires its durable final inventory and confirmation that the old process tree is quiescent or never started. If termination is established and the spool is confirmed irrecoverably lost, do not wait for an impossible final worker inventory. Reconcile controller-retained candidates first, record missing results as operational data loss, and retry only units without a retained usable result; pending validation is resolved before recomputation. This is not an override of uncertain ownership. “Not found” in a restarted agent journal is insufficient termination proof if its journal was lost. Operator-confirmed machine shutdown can resolve uncertainty. No heartbeat timeout automatically fences a process that cannot hear it. Final-inventory accounting
belongs to its exact assignment and is applied at most once; delayed inventory from an assignment
superseded by terminal/lost-spool resolution must not refund or change a later attempt's accounting
or blocked reason. Late candidate publication/acceptance remains a separate supported path.

## 11. Drain, deadlines and interruption

Every assignment carries `stop_new_units_at`, `terminate_at` and a final `handoff_deadline`, with `stop_new_units_at ≤ terminate_at < handoff_deadline`. These are finite execution/delivery bounds, not a guarantee that a remote collector will be available. Profiles also include a cooperative shutdown interval. Agents use elapsed-time timers after receipt and stop conservatively when deadline/clock validity is uncertain; disconnected/offline deployment must establish usable clocks before execution.

Every unit-start grant checks the locally persisted control revision and `stop_new_units_at`, even without controller connectivity. Reaching that time closes the start gate; it does not depend on receiving a drain request.

**Worker drain:** controller persists a control revision and stops issuing assignments to that worker. Agent serializes drain with its local start-grant gate, persists the revision and acknowledges the last granted unit. Any unit granted before this acknowledgment may finish. The request timestamp alone is not a barrier. A run drain first persists a run-level issuance barrier, then applies the same operation to every issued assignment, including acknowledged-but-not-started work.

After drain acknowledgment, no new unit grants occur. The agent begins cooperative interruption early enough to fit its configured shutdown grace before `terminate_at` (or an earlier operator drain deadline), then forces termination of the entire owned process tree at that cutoff. A deadline too near for the grace shortens it; it does not move the cutoff. Retain results published before termination; incomplete files are not results. Deadlines include preparation and collection, not only solver time. Record execution quiescence separately from successful handoff.

Drain returns `complete` only when affected execution is confirmed quiescent and its result/remainder inventory is durably known to the controller. It may return `pending_delivery`, `pending_offline_expiry` or `uncertain` at the operator deadline. Do not turn an unreachable worker into a success to satisfy a timer. Offline cancellation is available only if a platform integration proves it; otherwise the preissued finite bounds remain the control mechanism.

Process-tree containment and enforcement are OS-specific admission requirements. Proposed backends are Linux cgroups, Windows Job Objects if native Windows is selected, and a separately qualified macOS process-tree backend. Do not claim hard host-RAM/VRAM limits on a platform that only samples usage. A profile requiring hard enforcement is ineligible on a backend without it. Windows versus WSL and macOS enforcement limitations must be settled before promising those release capabilities.

A process group alone is not proof of generic descendant containment: applications may create new
sessions/groups or launch external services without being hostile. A group-based backend can qualify
only an execution contract that establishes all owned descendants remain in the controlled group,
plus a reliable whole-group exit check. Otherwise containment remains unsupported/unknown and
admission must fail closed. Sending a signal proves delivery, not quiescence. If the OS refuses enforcement, promptly persist
and report enforcement uncertainty, retain ownership and withhold a terminal final; neither ignore
the error nor wait indefinitely for exit after a failed kill. A persisted numeric
PID/PGID, command name, approximate start timestamp, leader disappearance or `ECHILD` alone is
not restart ownership proof; never signal a potentially reused identity or license replacement from
such evidence. Where restart cannot establish safe ownership/quiescence, preserve the start records
and results, report uncertainty and prohibit new starts. An additional ownership helper is not required
merely to avoid reporting uncertainty; any future helper needs its own failure and qualification argument.

## 12. Finite/offline batches

The controller transactionally reserves exact units in an assignment **before** emitting an immutable bundle. Bundle contents: schema/runtime references, finite assignment and deadlines, unit/package manifests, permitted input subset, application runtime artifacts, and an inventory of all blob digests. Output is a separate compact inventory and per-unit result artifacts; runtime caches/log debris must not pollute the retrievable result set.

The finite Go agent consumes this bundle without controller connectivity, applies the same local gating/publication rules, and publishes complete units incrementally. Its final inventory distinguishes completed, never-started and interrupted units. Copying the bundle is not permission to run it twice: export is a single-use assignment, bound to one execution session by the deployment procedure. This is a trusted operational constraint, not cryptographic prevention of offline copying.

### Registered-host exchange

The implemented finite exchange is restricted to actually registered native Darwin hosts; unknown required platform capabilities remain ineligible. Sealing requires the exact current worker/session/host observation, an explicit total retained-spool snapshot (including terminal but unretired data), and no unresolved assignment ownership. Legacy registrations without that total remain usable online but cannot authorize finite export. Cold-host reservation before actual inventory is not authorized by this path.

Finite sealing reuses ordinary indexed selection, resource reservations, attempt limits and drain/pause barriers. One transaction fixes every member, allocation, original deadline and sealed start authority before filesystem emission. Retries recover the same bundle and assignments; the export revision identifies immutable intent, not a renewed lease. Offline execution does not fabricate online receipts or control acknowledgments, refill members, or extend their bounds.

Runtime approval is local controller configuration over already verified catalog artifacts, scoped to the current controller instance. Requests cannot choose an executable or grant approval. A sealed bundle pins the unique compatible runtime/contract/target/ISA binding; the agent independently approves and freshly verifies its deployed executable and declared support files. Omitting configuration after restart disables new exports without revoking old sealed bundles or blocking their import.

Managed output is confined to `hostRoot/exchange/<BundleID>`. Every SDK-created retained cache, work, original artifact, output copy and pending copy intent is charged before writing; replacement cannot reduce an older retained charge before durable physical replacement. Exact declared sources already inside the managed root are tracked even if provisioned after index initialization. External read-only operator-provisioned bundle and deployed runtime/support sources remain operator/deployment storage responsibility, not SDK spool quota; deployed sources must remain protected from mutation during issued work. Native free-space observations do not establish a quota over those external sources. The operator CLI's export destination is likewise a dedicated, exclusively owned input-transfer cache, not managed agent output or controller-host storage. Transfer failure preserves the same sealed ownership for retry.

Import binds the durable original assignments and accepts available raw candidates independently of final inventories. Raw-publication acknowledgments are neither scientific acceptance nor terminal proof. Final inventory and any qualified resource-stop evidence pass the ordinary reconciliation rules together. Returned aggregate acknowledgments retain all durably imported progress despite record replay or a later-file failure. The agent validates the entire acknowledgment against retained originals before changing any member journal; a genuine operational write failure may retain a valid prefix. Reopening the same bundle skips exact acknowledged terminal archives and continues retained members without replay. Unknown ownership closes new starts rather than manufacturing a final inventory.

See [development workflows](development.md) for local configuration, commands and tested-scope limitations.

### Managed finite sessions

**Managed Kaggle execution is the normal first-release path.** The controller's platform adapter stages permitted inputs/runtime, submits private notebook sessions, tracks exact external versions and progress, collects available outputs and routes each member through ordinary publication/validation. Operators approve resource policy and budgets upfront; individual routine submissions and collections do not require manual intervention. A study/application supplies numerical runtimes and validators; the platform adapter does not import Perry scientific logic.

Separate concurrent session capacity from concurrent package processes within each session. A finite session may group several eligible independent assignments under one durable session owner; persist the owner and membership before staging/submission. Count one session slot per notebook, reserve its members, and collect shared output once before routing member results. Do not cross unresolved application dependency barriers or wait indefinitely to fill a session. Configured capacity is not a permanent account entitlement or a guarantee that a workload fits. Session/process limits are deployment policy, not historical probe counts.

Use unique session-scoped input staging and preserve exact runtime/input identities. Credentials remain on the submitting host; stage only authorized inputs and keep inputs, notebooks and outputs private. Preserve original deadlines across recovery. Reconcile ambiguous submission responses instead of blindly resubmitting; rate-limit-aware collection retains already downloaded valid files, including when the command subsequently reports HTTP429. Pending collection or one failed member must not stop unrelated eligible work. No live remote refill or immediate cancellation capability is assumed. Managed rollover stops new unit grants early enough to flush/save outputs before the hard platform limit, collects a recoverable copy, and schedules the remaining work into a later eligible session after termination/reconciliation. Reuse compatible persisted preparation through supported saved outputs and next-session inputs; container-local files alone are not durable recovery. Do not assume immediate replacement capacity. Session rollover and successor graphs retain the logical workflow's cumulative history and any actual configured allowance rather than minting fresh budgets.

Explicit CLI export/import remains available for manual finite exchange and recovery. A staged export or managed session with unknown submission remains reserved, not automatically released. Import accepts partial per-unit evidence through the ordinary publication/validation path without requiring an all-or-nothing session envelope. Reimport is idempotent.

Offline timeout alone does not prove session quiescence or that all retrievable outputs were collected. Reassignment waits for exact-session terminal evidence plus result reconciliation, or explicit operator resolution of missing evidence. Bundle deadlines bound compliant execution, but cannot prove a crashed agent's child tree stopped on an unqualified platform. A canceled-before-submission export can be released only with confirmation it was not dispatched.

Integration references: Perry's [Kaggle operating procedure](../../perry/root/docs/kaggle-operating-procedure.md) and [capacity-upgrade design](../../perry/root/docs/kaggle-capacity-upgrade-design.md). These optional sibling-project references inform the adapter; Hermes's runtime startup, application compatibility, termination and collection must be verified against its own implementation. The adapter does not manage account provisioning or silently raise resource policy.

## 13. Efficient control, reporting and security

- Separate bounded metadata handling from artifact transfer, preparation and validators. Database write transactions contain no blob hashing, solver call, network operation or application validation.
- Service worker reports/drain/collection between bounded scheduling batches. Queue scans are indexed and paginated; one large package cannot monopolize the controller. Use separate bounded concurrency for transfer and validation, with backpressure visible to operators.
- Fetch immutable manifests by digest and reuse verified identities. Never hash a package per result record/status check. Reconciliation inventories contain compact references/deltas, not historical payload replay.
- Report separate timings for queueing, staging, preparation, compute, local publication, transfer, validation and controller recovery. Show last worker report, current unit, last published result, accepted count, uncertainty and blocked reason. A live source or reserved slot is not useful-progress evidence.
- Status reads are metadata-only. Detailed logs are bounded operational output, not a second event-sourced authority or a requirement to build a monitoring platform.
- Pin allowed executable/runtime references; reject path traversal, undeclared files and excessive manifests/uploads. Limit result/log/spool sizes. Enforce artifact access/location permissions before transfer; no implicit market-data replication.
- Use authenticated transport on the LAN. Do not build an arbitrary remote shell or expose artifact paths as authorization. Credential provisioning/rotation and service permissions require deployment documentation before networking is released.

## 14. Proposed repository and implementation boundaries

When implementation is authorized, start with one Go module and three entrypoints. Suggested packages describe responsibility, not a framework to prebuild:

```text
cmd/hermes/                 operator CLI
cmd/hermes-controller/      controller service
cmd/hermes-agent/           connected and finite agent modes
internal/contracts/        versioned metadata and executable protocol
internal/controller/       transactions, scheduling, reconciliation, API
internal/agent/            journal, start gate, spool, execution loop
internal/artifacts/        immutable publication and streaming transfer
internal/platform/         qualified process-tree/resource backends
```

Keep SQL/storage code close to controller/agent operations; do not invent repository abstractions for hypothetical alternate databases. The finite path reuses agent execution and manifests, not an independent runner. Application adapters/runtimes live outside Hermes. Perry integration should implement these contracts explicitly, not move its controller wholesale.

### Optional implementation reuse

The [native Dagu trial](distributed-execution-discovery.md#11-first-native-dagu-trial--november-23) and [source-extraction assessment](distributed-execution-discovery.md#12-literal-source-extraction-from-dagu-v2270) are implementation leads, not architecture constraints. Assess candidates in the corresponding implementation task; no separate Dagu adoption milestone is required.

The useful shortlist is durable no-replace file publication and native process lifecycle/parent-exit handling, plus relevant failure-path tests. Compare adapting the narrow mechanism with writing the minimal local implementation, including dependencies, maintenance and contract fit. Do not import Dagu's graph engine, worker protocol or state model merely to reuse these pieces, and do not build a wrapper layer solely to keep future Dagu adoption possible.

The extraction notes record GPL-3.0-or-later notices: establish compatible licensing/distribution terms and preserve applicable notices before incorporating source or tests; moving/renaming files does not remove those conditions. If reuse is not suitable, implement the needed behavior locally. In either case test Hermes's own process-containment and same-host admission contracts; signal failures must remain visible rather than being treated as successful termination. Source selection and adaptation belong in the implementation plan/task; any change to Hermes's architecture or guarantees requires an explicit design change.

### Agent host and build contract

An agent is a **native executable compiled for a specific OS and CPU architecture**, not portable VM bytecode. Its binary includes the Go runtime; the target host does not need Go, `just` or a compiler installed. A Linux executable is not a macOS/Windows executable, and `amd64` is not `arm64`. Application runtimes and native libraries have their own independent compatibility requirements.

#### Host inventory: what the operator retrieves and specifies

Collect this information **inside the environment that will actually run the agent**, not just from the physical machine's marketing specifications. Keep observed properties separate from operator-approved limits. A deployment must provide all required fields below; GPU-specific fields may explicitly be `none`, and an unverified capability must be `unknown`, not assumed available.

| Data to retrieve | Required operator specification / contract use |
|---|---|
| OS family, release/build and kernel version; Linux distribution if applicable | `goos` (`linux`, `darwin` or `windows`), exact OS/version and environment (`native`, `wsl2`, `container`, `vm` or `notebook`). WSL2 is a Linux target, not a Windows target. A notebook/container uses its guest kernel/architecture; its quotas and lifetime may differ from the host. |
| Native execution architecture and any translation/emulation; CPU model | `goarch` (`amd64` or `arm64`) and ISA baseline. First design baseline: `GOAMD64=v1` or `GOARM64=v8.0`; no implicit Rosetta/x64 emulation or higher ISA optimization. Other architectures require an explicit contract extension. |
| Effective CPU availability and memory limits inside this environment | Observed logical CPUs and RAM/cgroup/container ceiling, plus approved CPU/thread and RAM budgets. Do not equate physical totals with assignable resources. |
| Writable storage, free space and persistence across process/session loss | Absolute spool/cache location, approved byte budget and retention/failure domain. Notebook scratch is not durable handback. |
| Available process-tree containment, resource enforcement and service-account privileges | Selected qualified backend and actual enforcement capabilities. Record which limits are hard versus sampled, and reject profiles requiring unavailable enforcement. Compilation is not backend admission. |
| GPU inventory, if used: device IDs, model, VRAM, driver/runtime versions and guest visibility | Permitted devices and VRAM budgets; application runtime compatibility remains application-owned. A CUDA installation is not an agent compilation target. |
| Network access, trust material, clock synchronization and platform lifetime constraints | Connected controller address/identity and credential locations, or explicit offline mode; allowed data/staging locations, finite execution bounds and clock assumptions. Do not put private keys or tokens in the inventory. |

Identify hosts with an operator-chosen stable `host_id`; bind it to the provisioned worker identity. Inventory is configuration/admission data, not permission to access a machine automatically. CPU model/OS descriptions explain the environment; `goos`, `goarch` and the baseline select the binary. On startup the agent reports its compiled target, version, protocol version and observed capabilities; the controller reconciles these with approved inventory before issuing work. Mismatch or unknown required capability blocks admission with a specific reason, not fallback execution. Refresh admission after an environment/resource change.

Minimum target-identification commands for the operator to run on the intended host/environment:

```sh
# Linux/macOS, inside the intended guest/container/WSL environment if applicable:
uname -s
uname -m
uname -r
```

```powershell
# Native Windows (not a WSL shell):
[System.Runtime.InteropServices.RuntimeInformation]::OSDescription
[System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture
```

Map Linux → `linux`, Darwin → `darwin`, Windows → `windows`; map `x86_64`/`AMD64`/`X64` → `amd64`, and `aarch64`/`arm64`/`Arm64` → `arm64`. Under translation, shell/process architecture can differ from native architecture: confirm native execution before selecting a build; do not silently treat a translated shell as host admission. These commands identify the target, **not** the remaining resource/enforcement inventory. On Linux retrieve distribution information from `/etc/os-release` and effective cgroup/container limits; on Windows/macOS retrieve OS/resource/device details from their system facilities. Go need not be installed to collect the inventory.

#### Service lifecycle and packaging

Persistent-host agents use the host's service manager. Installation/bootstrap is explicitly operator-invoked; the host supervises the agent, and the controller manages assignments and reconciliation. Loss of contact is not permission for the controller to install or restart a remote service. Configure automatic service restart for ordinary process failures; recovery still reconciles owned application processes before restarting unfinished computation. An intentional service stop stays stopped, and restarting a service never clears persisted run/worker drain or pause controls or resumes an explicitly stopped application workflow. **Optional nice-to-have:** prevent macOS idle sleep while the coordinator has an active run or an agent is executing issued work, and release that request when no longer needed. Automatic sleep prevention is not a first-release requirement and does not promise recovery from deliberate sleep, shutdown or host loss.

The macOS packaging target is the privileged `system` launchd domain, running each service as an
explicitly configured existing account via `UserName`; a GUI-login agent alone does not meet startup
independent of interactive login. Controller and agent have distinct service identities. Persistent
intentional stop must disable the exact service and remove its registration (`disable` then `bootout`),
not merely signal it or use `launchctl stop`, which may permit immediate restart. Starting it again
requires explicit enable/registration. This selects packaging semantics; scripts and target-host evidence
are still required before qualification. Launchd's default cleanup of the job's process group is not
proof that descendants in other groups have been contained.

Each `just build-agent <goos> <goarch>` output is a deployment directory containing the target executable and its deployment assets. Persistent-host packages also contain that OS's install/uninstall scripts and required service templates. First-release macOS service packages use `launchd`. The Linux finite/Kaggle package instead contains finite startup assets and does not depend on a persistent Linux service backend.

- Persistent `darwin` packages include `install.sh` and `uninstall.sh`. Any future persistent Linux target must likewise provide these scripts and qualify its service manager. A future native Windows service target must provide `install.ps1`/`uninstall.ps1` and a service-compatible agent for Service Control Manager. WSL is a Linux deployment, not a Windows service deployment.
- Scripts run on the target host, require no Go/compiler/`just`, and document required privileges, configuration and installed locations. Package the scripts/templates alongside the binary; do not require the source checkout on the target.
- Install and uninstall tolerate repetition safely. Installation must not start duplicate agents or silently discard existing identity, assignments or results.
- Uninstall stops the agent and confirms its owned application processes are stopped, unregisters startup/restart behavior, and removes installed binaries and credentials. Preserve unpublished results and recovery state unless the operator explicitly requests a purge. Report failure or incomplete cleanup honestly; do not claim success if processes remain or ownership is uncertain. Uninstallation does not itself authorize the controller to reassign unresolved work.
- Kaggle finite execution does not install a persistent host service: its notebook entrypoint stages and launches the finite agent. Host-service scripts are not used in that mode; no cross-session persistence is inferred.

Keep platform deployment assets under `deploy/agent/<goos>/`; the build copies those files into its target output directory. Persistent service assets include install/uninstall scripts and any templates; finite Linux assets include notebook startup support, not service registration. Packaging alone does not certify service behavior or process-tree containment. Persistent-host admission includes repeated install/uninstall, service startup independent of interactive login, preservation of recovery/results, and verification that uninstall leaves no running owned processes or registered service. Finite-target admission exercises startup, execution bounds and recoverable output without installing a service.

#### Explicit cross-compilation recipe

When the Go module and `cmd/hermes-agent` exist, expose **`just build-agent <goos> <goarch>`**. Both arguments are mandatory, including for a local build, so the build machine's architecture cannot accidentally choose the target. Proposed candidate pairs are `linux/{amd64,arm64}`, `darwin/{amd64,arm64}` and `windows/{amd64,arm64}`; this list identifies targets, **not a claim that their process/resource backends are implemented or admitted**. Build failure on a missing backend must remain a failure, never a no-op enforcement substitute.

Keep the agent dependency graph buildable with `CGO_ENABLED=0`; use Go/OS-specific backend code rather than introducing a C toolchain requirement into this recipe. This restriction is for the infrastructure agent, not application kernels or the controller's eventual SQLite driver. A dependency requiring cgo would require an explicit revision of this build contract, including target toolchain/ABI requirements.

Proposed `Justfile` recipe (specification only; there is no Go module or runnable recipe in this repository yet):

```just
# Run on a POSIX build machine with just and the project's pinned Go toolchain.
build-agent goos goarch:
    #!/usr/bin/env sh
    set -eu
    target_os='{{goos}}'
    target_arch='{{goarch}}'
    case "$target_os/$target_arch" in
        linux/amd64|linux/arm64|darwin/amd64|darwin/arm64|windows/amd64|windows/arm64) ;;
        *) echo "Unsupported agent target: $target_os/$target_arch" >&2; exit 1 ;;
    esac
    suffix=''
    if [ "$target_os" = windows ]; then suffix='.exe'; fi
    out="dist/agent/$target_os-$target_arch"
    mkdir -p "$out"
    GOOS="$target_os" GOARCH="$target_arch" CGO_ENABLED=0 \
        GOAMD64=v1 GOARM64=v8.0 \
        go build -trimpath -o "$out/hermes-agent$suffix" ./cmd/hermes-agent
    cp -R "deploy/agent/$target_os/." "$out/"
```

The Go version will be pinned when `go.mod` is introduced, with support for these ISA settings and a documented minimum target OS/kernel matrix. This recipe uses the build machine's Go toolchain; it does not run the cross-compiled program or prove compatibility on the destination. A POSIX builder can produce Windows `.exe` files; Windows build-machine support is not implied by this shell recipe. No compiler/`just` installation on agent hosts is required.

Examples, **after implementation**:

```sh
just build-agent linux amd64   # dist/agent/linux-amd64/ (binary + finite startup assets)
just build-agent darwin arm64  # dist/agent/darwin-arm64/ (binary + install/uninstall files)
just build-agent windows amd64 # dist/agent/windows-amd64/ (binary + install/uninstall files)
```

Deploy the selected executable using the existing artifact identity/transport mechanism; do not invent a second binary registry. The release identifies its source/version, target and ISA baseline, Go toolchain version and executable digest. Confirm the agent starts on the actual target and reports the expected platform/protocol before admitting it; then qualify process-tree/resource behavior separately. Matching OS/architecture is necessary, not sufficient. Application Python/C++/CUDA dependencies are neither supplied nor certified by compiling the agent.

## 15. Verification contracts

Use application-neutral synthetic work. The [implementation plan](implementation-plan.md) owns sequencing, task acceptance and review assignments; the behaviors below define what the implementation must establish, not automatic deployment permission or per-job approval states.

| Behavior | Evidence required |
|---|---|
| Durable contracts | Crash injection at blob/transaction/ack boundaries; same-ID retry is idempotent; incompatible same-unit specs and conflicting outputs are visible. |
| Independent execution (R3/R4) | Kill controller during active units; workers retain process/unit identities, complete issued work and spool results; restart/reconcile without restarting those computations or scanning accepted payloads. |
| Ownership (R6) | Lose assignment reply/ack, delay controls, disconnect agent and restart it with a surviving child. No reassignment until ownership is resolved; uncertainty remains explicit. |
| Unit acceptance (R1) | Multi-unit package interrupted after publication; validate retained units and repackage only missing ones. Reject malformed/invalid candidates without false completion. |
| Unattended application continuation | A supplied-input staged fixture selects an authorized later graph or prescribed stop from accepted decision outputs without Pi/LLM/manual forwarding. Restart before/after successor submission: reconcile one idempotent successor, reuse earlier acceptances/preparation, preserve logical workflow history and any actual cumulative budget, and honor explicit stop without a review timer. |
| Drain/policy (R2/R5) | Drain one of two busy workers, then the run. Confirm start-grant barrier, deadline enforcement, retained outputs/remainder and unaffected other worker. Change future profiles without mutating issued ones. |
| Resource/platform (R7/R10) | Build each claimed agent target through `just build-agent <goos> <goarch>`; start it on the inventoried target, verify its platform/protocol report and reject mismatched/unknown required capabilities. Exercise each claimed process-tree backend, aggregate budgets, native thread limits, disk-full and required hard-enforcement rejection. GPU/native Windows/Kaggle each need their own evidence; cross-compilation alone is not admission. |
| Finite exchange (R8) | Run disconnected, interrupt before final envelope, import available units twice, recover missing remainder only after terminal reconciliation. No invented live drain capability. |
| Managed Kaggle execution | Exercise automatic staging/submission/tracking/collection, separate session/process caps, partial-session dispatch, one owner with multiple members, ambiguous submission recovery, controller restart, one collection with member routing, retained partial results and HTTP429 after files save. Preserve privacy, exact version identity and original deadlines. |
| Controller efficiency (R9) | Synthetic representative-size artifacts with many accepted units: count full-byte hashes during status/recovery/completion lookups (expected zero after boundary verification), measure control latency during transfers/validation and prove scheduling scans do not starve reports. |

## 16. Configuration and implementation decisions

The coordinator/implementers resolve routine engineering choices through the [implementation plan](implementation-plan.md), keeping the contracts above unchanged:

- Select/pin the Go toolchain, SQLite driver/build and local durability settings.
- Specify bounded wire/executable schemas, indexed queries and compatibility records alongside their first consumers.
- Implement native macOS containment accurately, and distinguish hard limits from sampled resource guards on every supported backend.
- Supply target inventory, approved budgets/profiles, finite assignment/session bounds and retry policy as deployment configuration. Do not infer current allocations from historical worker counts.
- Keep application units, prescribed initialization, validation, preparation compatibility and deterministic phase decisions application-owned. Reuse existing commands where they satisfy the contract.
- Define credential/service permissions and supported deployment procedures without turning ordinary scheduling/recovery into operator approval steps.

Escalate actual contract/scope changes, licensing consequences or unavailable required access; do not make optional reuse or routine implementation choices prerequisites for progress. No production run is a smoke-test fixture.
