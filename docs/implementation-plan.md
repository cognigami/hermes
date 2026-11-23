# Hermes implementation plan

## Purpose and authority

This is the implementation handoff and temporary task ledger. Local implementation is released;
completion of this plan does not itself certify a deployment.
[Design](design.md) owns architecture, identities, state transitions and operating contracts;
[this plan](implementation-plan.md) owns delivery sequence and temporary task status.
[Coordination](COORDINATION.md) owns delegation, launch, messaging and review rules.
If implementation exposes a contract problem, resolve it in the design rather than silently changing it here.

**Current authority: local engineering released by the operator.** Local implementation, dependency
installation, builds, tests and synthetic runtime fixtures are permitted. Remote host/cloud exercises,
research/market-data access or replication, Kaggle submissions and cross-project contact remain excluded.
Before any Perry access, integration or contact, consult the operator, who must confirm the current study
is complete. Do not use Perry as a fixture. Local service exercises, if undertaken, must be explicitly
synthetic and isolated from existing services. Sandbox denials are boundaries: report the exact blocked
operation and an operator command; do not retry through alternate tools or paths.
Neither completing a slice nor obtaining a review expands that authority.
Within released work, the coordinator decides ordinary engineering details without repeated operator consultation.

Deliver the custom Go controller, agent and CLI: one transactional local SQLite metadata root,
immutable artifacts, native macOS coordinator and LAN Mac agents, plus managed finite Kaggle CPU sessions.
Windows/WSL execution and GPU admission are deferred; compilation alone never admits a platform.
No active-active authority, generic cloud manager, arbitrary checkpointing, second scheduler or research planner.
Application numerical code and actual Perry adoption remain outside Hermes implementation scope.

## Coordinator operating model

Default to delegating substantial bounded implementation, investigation and independent review;
keep small integration edits local. The coordinator retains architectural context, decisions,
interface ownership, sequencing, integration and operator communication—not merely message forwarding.
Delegate only when independent work can continue. Agree shared contracts first; serialize shared files.

Follow local [COORDINATION.md](COORDINATION.md), including for descendant launches:
- Verify `HERDR_ENV=1` and use the coordinator's inherited `HERDR_WORKSPACE_ID`.
- Launch every worker/reviewer in a **tab in that workspace**, never a split or the visible workspace.
- Pass the exact Pi option `--presets:preset eng`; assign roles in the brief.
- Through the provided Herdr tool, create a tab with explicit workspace/cwd and `--no-focus`;
  use returned `result.root_pane.pane_id` for `agent start … --kind pi --pane … -- --presets:preset eng`.
- Use `session_list` to identify sessions and `session_send_message` with `report_back: true`
  for requested reports, `false` for one-way messages. Do not use unreachable legacy tool names.
- Continue useful work after assignment; if only the response remains, end the turn. Do not poll or sleep.
  A timeout does not prove non-delivery; inspect before assigning duplicate work.
- Brief each worker with cwd, exact allowed files, design/interface references, authority limits,
  dependencies, meaningful checks and completion report. Reviewers are read-only by default.
- No cross-project contact without explicit authorization; do not operate Perry through a worker.

Use one fresh correctness review at each meaningful dependency boundary. Review may cover a coherent
assembled slice, not every edit. Obtain an independent engineering/integration review after major
cross-module integration and before release. Review concrete behavior and failure arguments, not test counts.
Coordinator evaluates findings, applies minimal warranted corrections and requests focused re-review.
No statistical-finance panel is needed for application-neutral infrastructure fixtures.

## Task ledger and dependency map

All entries start unassigned and not started; update this table rather than creating a second tracker.
Dependencies mean integrated, reviewed behavior, not merely an interface stub.

| ID | Slice | Depends on | Suggested owner | Status |
|---|---|---|---|---|
| S1 | Foundation and executable contracts | implementation release | hermes-foundation | Complete: full build/lint/test passed; independent correctness review and focused re-review passed after three contract fixes |
| S2 | Complete local acceptance path | S1 | hermes-foundation (vertical slice); coordinator (artifact integration) | Complete within synthetic-local leaf-process scope: full build/lint/test and commit-crash evidence passed; independent correctness and engineering reviews plus focused re-reviews passed after four fixes |
| S3 | Independent execution, control and recovery | S2 | hermes-foundation (recovery); coordinator (interfaces/integration) | Complete within synthetic leaf scope: full checks and independent correctness/engineering re-reviews passed after three fixes. Generic descendants/native services remain unqualified |
| S4 | Aggregate admission and resource recovery | S3 | scheduling worker pending interface handoff | Preparing host-manager/allocation interfaces; implementation not started |
| S5 | Qualified native build/service packaging | S3; S4 for admission | platform worker | Not started |
| S6 | Finite runner and explicit exchange | S3 | finite worker | Not started |
| S7 | Managed Kaggle lifecycle | S4, S6 | platform-adapter worker | Not started |
| S8 | Automatic application successor integration | S3; S7 for rollover integration | continuation worker | Not started |
| S9 | Operator interface and documentation | S4–S8 | CLI/documentation worker | Not started |
| S10 | Integrated first-release acceptance | S9 | coordinator + independent reviewers | Not started |

Deliver S2 early, then harden its real end-to-end path rather than prebuilding a framework.
After S3, S4/S5 and S6 can proceed in lanes with disjoint ownership; reserve shared agent/controller
entrypoints for coordinator integration. S8 can proceed alongside S7 against an agreed submission interface.
CLI coverage grows with each slice; S9 completes it rather than withholding all operator controls until last.
Publish interface changes before downstream work; do not let concurrent workers independently redefine schemas.

## Ordered slices

### S1 — Minimal foundation and contract fixture

Candidate ownership: `go.mod`, `go.sum`, `Justfile`, `internal/contracts/`, `testdata/synthetic/`,
`docs/development.md` for authoritative build/lint/test/dependency workflows. Coordinator owns initial package/interface decisions.
Choose and pin Go toolchain, SQLite driver and minimal dependencies during implementation;
keep agent dependencies compatible with `CGO_ENABLED=0`. Keep SQL beside its owning operations.
Use standard Pi `deps`, `build`, `lint` and `test` workflows; document supported discovery/recipes,
including eventual `just build-agent <goos> <goarch>`. Do not invent commands before wiring them.

Define bounded versioned manifests, application stdin/stdout protocol and validator verdicts needed by S2.
Keep application unit, reusable package, execution assignment and platform session distinct.
A synthetic executable prepares once, waits for unit grants, emits files/results and supports controlled failures;
a synthetic validator distinguishes domain rejection from operational failure. No research inputs.
**Acceptance:** fixture runs through the declared protocol; same scientific unit/spec stays identical across
placement/profile changes; incompatible re-registration is rejected. Review contracts before S2 depends on them.
Do not generate unused platform scaffolding or a generic repository abstraction.

### S2 — Local complete vertical slice

Candidate ownership: `cmd/hermes/`, `cmd/hermes-controller/`, `cmd/hermes-agent/`,
`internal/controller/`, `internal/agent/`, `internal/artifacts/`; worker owns this lane initially.
Implement artifact ingress, a local metadata authority/root lock, run sealing, assignment receipt/journal,
one admitted package process, per-unit publication, isolated bounded validation, acceptance and status.
Use the real connected contract on an authorized local test endpoint, not an in-process fake replacing ownership.
Persist mutating request IDs and outcomes; reserve before reply and journal before execution.

**Acceptance:** submit a multi-unit package; accepted prerequisites release dependent units; CLI exit has no effect.
Only durable blobs can acquire metadata references. First application-valid candidate to commit is canonical;
identical redelivery is idempotent, differing candidates warn without pausing acceptance or unrelated work.
Validator failure is not a scientific negative. Reject malformed manifests, undeclared paths and oversized messages.
Crash at artifact/transaction/ack boundaries without losing acknowledged data or accepting missing bytes.
Independent correctness review follows the full path, including SQL uniqueness and raw-publication acknowledgment.

### S3 — Ownership, outage, drain and restart

Candidate ownership: `internal/agent/` journal/start gate/spool, `internal/controller/` reconciliation/control,
`internal/platform/` macOS process-tree backend; serialize edits with S2 or hand off those directories.
Persist finite bounds and control revisions; serialize unit grants with drain. Keep controller and agent lifetimes
independent. Restart reconciles owned trees, inventories and pending candidates before allowing new starts.
Implement explicit uncertainty, terminal evidence and lost-spool recovery rather than timeout-driven duplication.
Assess narrow Dagu publication/native lifecycle mechanisms here or in S2 only if useful; see discovery §§11–12.
Reuse is optional, not an adoption milestone or mandatory fork. Before copying source/tests resolve licensing
and notices; otherwise write the minimal local mechanism. Do not import Dagu's engine/protocol/state model.

**Acceptance:** kill controller while work runs; agent continues only issued finite work and retains results.
Recover without restarting those units or replaying accepted units. Lost reply/ack retries the same assignment.
An unreachable agent or stale heartbeat never licenses reassignment. Agent restart with surviving children
establishes quiescence or reports uncertainty, never silently starts another tree.
After confirmed termination plus irrecoverable spool loss, reconcile retained candidates/validation first,
record operational data loss, then retry only missing units—do not demand an impossible final inventory.
Drain acknowledges a start-grant barrier, enforces deadlines against the full owned tree and reports delivery
separately from quiescence. Persisted stop/pause survives controller, agent and service restart.
Review the failure matrix and two-worker drain behavior independently before scheduling expands.

### S4 — Aggregate scheduling and automatic resource escalation

Candidate ownership: controller scheduling/admission files, agent admission files, contract resource fields;
agree exact files with S5/S6 rather than granting overlapping whole-directory scopes.
Use stable indexed ready ordering, capability/data restrictions, then optional cache locality.
Reserve preparation, application/native children, transfer and validation overhead against aggregate host budgets.
Keep observed inventory distinct from approved limits; unknown or unavailable required enforcement blocks admission.
Policy changes affect unassigned work; issued work keeps its recorded profile until acknowledged handback.

**Acceptance:** concurrent synthetic packages cannot over-admit the host. Independent work continues while a unit
awaits capacity or validation. After quiescence and result reconciliation, resource failure automatically selects
a sufficient larger/exclusive same-host allocation or another eligible bigger worker within approved limits.
Exclusivity must actually provide sufficient allocation, not merely remove neighbours under the old small cap.
Never repeat the known inadequate profile; persist bounded retries/backoff and explicit exhausted/waiting reasons.
No per-failure approval. Verify status/recovery/readiness use indexed metadata and zero accepted-payload reads
or full-byte hashes after boundary verification. Exercise bounded transfers/validators while controls remain responsive.

### S5 — Native capability qualification and service/build packages

Candidate ownership: `internal/platform/` target files, `deploy/agent/darwin/`, build packaging recipes,
controller service packaging and operator lifecycle documentation. Coordinate manifest changes with S1 owner.
Provide explicit target builds with binary, install/uninstall scripts and service templates together;
target hosts need no compiler, Go, `just` or checkout. Preserve design ISA baselines and agent cgo-free builds.
Qualify macOS `launchd` controller/agent supervision as separate services; reject unproved hard-limit profiles.
Linux notebook finite startup belongs to S6/S7, not a speculative persistent Linux service rollout.
If any additional persistent OS target is claimed, select/qualify its service backend and bundle its scripts first.

**Acceptance:** inspect each deployment directory and verify target/version/capability reporting.
In an explicitly released host exercise, repeat install/uninstall, start independently of interactive login,
restart a failed service without replay, and confirm intentional stop stays stopped.
Uninstall confirms owned process termination, unregisters service and removes binaries/credentials while preserving
unpublished results/recovery state; purge is explicit. Uncertainty or incomplete cleanup is reported honestly.
Compilation, script presence and synthetic process tests do not alone certify service deployment or containment.
Review platform claims against actual evidence, including macOS memory-enforcement limitations.

### S6 — Same agent, finite execution and exchange

Candidate ownership: finite-mode files under `internal/agent/`, controller export/import files,
CLI exchange commands and synthetic bundle fixtures. Reuse S2/S3 execution, journal and publication code.
Reserve exact members transactionally before export; bind single-use assignment/session identities and deadlines.
Provide compact result inventory, incremental per-unit artifacts and partial idempotent import.
**Acceptance:** disconnected execution stops new grants and flushes results within original bounds;
interruption before a final envelope still permits usable published units. Import twice without duplicate acceptance.
No implicit live drain, remote refill or permission to execute a copied bundle twice.
Uncertain dispatch remains reserved; terminal evidence plus retained-result reconciliation precedes missing-unit retry.
Test finite Linux startup with synthetic inputs when released; no service installation is needed in notebooks.

### S7 — Managed finite Kaggle CPU sessions

Candidate ownership: `internal/controller/kaggle/` (one adapter, not a second scheduler), finite staging assets,
provider fixtures and adapter-specific operator documentation. Keep credentials on the submitting host.
Use a fake provider first, then only explicitly released private synthetic cloud exercises.
Persist session owner/member reservations, submission intent, exact external version, original deadlines and collection.
Separate `max_sessions` from `workers_per_session`; capacity examples are not account entitlement or defaults.
Do not cross dependency barriers or wait indefinitely to fill a session.

**Acceptance:** automatic stage/submit/track/collect reconciles ambiguous submissions without blind resubmission.
Controller restart preserves ownership/deadlines. Collect shared session output once and fan out members idempotently;
a failed member/pending collection does not block unrelated work. Keep valid files saved before HTTP429 or later errors.
Stop grants early enough to save/retain output before the platform hard cap; reconcile termination and candidates
before rollover. Reuse compatible saved preparation where supported, not vanished container-local caches.
Enforce allowed input locations/privacy and approved cumulative capacity/budget; no provisioning or policy inflation.
Fresh correctness review covers ambiguous submit, partial retention and once-only collection before real submissions.

### S8 — Deterministic application successors, not manual phase forwarding

Candidate ownership: `internal/controller/` logical-workflow/submission records and API files,
`examples/staged-workflow/` synthetic application adapter; keep scientific selection outside infrastructure.
Seal each graph. Application continuation reads accepted/terminal metadata and a pinned decision/freeze artifact,
then automatically submits the next fixed graph or records its prescribed stop under the logical workflow ID.
Persist intended successor/request ID and reconcile creation idempotently; no live Pi/LLM or review timer.
**Acceptance:** restart before and after successor submission yields one successor. Explicit workflow stop remains
stopped through service restart. Cumulative elapsed/progress history, actual configured budgets and stop state
survive submissions and Kaggle rollover; do not invent a whole-workflow deadline or reset allowances.
Reference earlier acceptances and compatible preparation rather than repeatedly preparing the accepted prefix.
Review the synthetic application boundary; actual Perry integration requires separate authorization and ownership.

### S9 — Complete CLI and authoritative operator documentation

Candidate ownership: `cmd/hermes/`, focused operator docs linked from `docs/README.md`;
coordinator serializes shared documentation/index edits. Expose the design's API-equivalent controls and stable
metadata-only status: accepted versus execution-complete, last progress, delivery, uncertainty and blocked reasons.
Document inventory/admission, credentials, supported builds, service lifecycle, finite recovery, managed Kaggle,
resource policy, stop/resume and manual reconciliation—including the duplicate-risk override distinction.
**Acceptance:** a fresh reader can operate a synthetic workflow with packaged artifacts, understand pending drain,
and recover a lost response without creating work twice. State precisely which targets/capabilities were exercised;
mark unexercised host/cloud behavior, deferred Windows/GPU and machine-loss/manual restore limitations honestly.
Do not duplicate contract definitions or promise disaster recovery not implemented and exercised.

### S10 — Integrated first-release acceptance

Coordinator assembles macOS controller/LAN agent plus Linux finite/Kaggle CPU evidence under the released authority.
Run the design §15 behavioral scenarios across integrated paths, not isolated mocks alone: controller outage,
agent restart, acknowledgment loss, retained candidate races, drain, disk-full, resource escalation,
finite partial import, shared Kaggle collection/rollover and unattended successor/explicit stop.
Use application-neutral fixtures; no research dataset, scientific experiment or Perry cutover is implied.
Independent correctness and engineering/integration reviewers inspect the assembled implementation and evidence.
**Acceptance:** no unsafe reassignment, lost acknowledged bytes, accepted-prefix replay or payload-heavy status;
claimed service/process/resource and platform behavior has target evidence. Report missing evidence as a limitation,
not success. If host/cloud release is absent, finish available engineering and identify the unexercised capabilities;
do not call mocked integration a certified first release. Resolve concrete findings before relying on the result.

## Copyable coordinator start brief

> Coordinate Hermes using `docs/design.md` for contracts, `docs/implementation-plan.md` for slices/status,
> and `docs/COORDINATION.md` for coordination. Local implementation, dependencies, builds/tests and
> synthetic runtime fixtures are released. Remote host/cloud exercises, research/market data, Kaggle work
> and cross-project contact remain excluded. Consult the operator before any Perry access or integration;
> the current Perry study must first be complete. Respect sandbox denials and report the required operator
> command without a workaround. Do not infer expanded authority from this plan or reviewer approval.
> Own interfaces/context/decisions/integration and operator communication;
> default to delegating substantial bounded work and independent review, with exact disjoint write scopes.
> Use Herdr tabs in your own inherited workspace, returned root pane IDs, `--no-focus`, and the exact
> `--presets:preset eng` option for every launched Pi worker/reviewer. No splits or convenience launchers
> that cannot meet those rules. Use `session_list` and `session_send_message(report_back: true/false)`;
> continue useful work or end the turn while awaiting reports—no polling or duplicate dispatch on timeout.
> Begin with S1/S2, review meaningful slices before downstream reliance, and update the task ledger.
> Decide ordinary released engineering choices yourself; raise genuine authority/contract conflicts plainly.
> Do not make Dagu adoption a prerequisite, require per-retry approval, or substitute manual phase forwarding
> for deterministic application successors. Report concrete outcomes, reviews, limitations and unresolved issues.
