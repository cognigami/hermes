# Local development and executable contracts

This is the authoritative development workflow and wire-usage guide. Architectural guarantees remain in [design.md](design.md). All fixture inputs are synthetic; these commands authorize no deployment or research-data access.

## Supported workflows

The module is `hermes`. The exercised native toolchain is Go **1.25.5**, macOS amd64; `go.mod` requires that version and `Justfile` pins `GOTOOLCHAIN=go1.25.5`. Go and `just` must be available on the development machine. This is not a target-host installation requirement.

Use Pi's provided tools:

- `build`: discovers `Justfile` recipe `build`; compiles the current `cmd` entrypoints with `CGO_ENABLED=0` into `dist/local/`. These are native development binaries, not qualified deployment packages. Library packages are compiled as entrypoint dependencies and exercised by `test`.
- `lint`: recipe `lint [paths...]`; formats supplied files/directories with gofmt and runs `go vet ./...`. Without paths it formats `cmd` and `internal`. When coordinating concurrent writers, supply only owned paths. Vet is read-only and repository-wide.
- `test`: recipe `test [paths...]`; runs `go test ./...` or the supplied repository-relative package directories. Focused example: `internal/contracts`.
- `deps`: discovers `go.mod`; use it for dependency add/remove/update rather than editing dependencies manually.

These recipes are also available directly as `just build`, `just lint [paths...]`, `just test [paths...]`. Run build, lint, then test after changes. The contracts tests compile and launch the two real fixture executables in test-private directories. They exercise stdin/stdout, preparation once for two units, incremental output, explicit local-publication acknowledgments, finish, domain rejection, validator operational failure and controlled preparation/unit process failure.

SQLite selection is **modernc.org/sqlite v1.44.3**, pinned through `deps` in `go.mod`/`go.sum` with its transitive dependencies. The driver is pure Go; the agent remains cgo-free and does not import controller storage. `internal/controller` owns the driver import, SQL and durability settings. The connected local integration tests below exercise SQLite behavior; a fixture build alone is not durability evidence. No copied Dagu source or dependency is used.

### Explicit Darwin packages

The supported target recipes are now `just build-agent <goos> <goarch>` and
`just build-controller <goos> <goarch>`. Both arguments are mandatory; currently only
`darwin/amd64` and `darwin/arm64` are accepted. They write self-contained directories under
`dist/agent/<goos>-<goarch>/` or `dist/controller/<goos>-<goarch>/`, containing the executable,
install/uninstall/lifecycle scripts, plist/shared service assets, `build-info.txt` and `SHA256SUMS`.
The build pins `CGO_ENABLED=0`, `GOAMD64=v1` and `GOARM64=v8.0`. `HERMES_VERSION` optionally sets
the linker version (default `dev`; only letters/digits/`.`/`_`/`+`/`-`); build metadata retains the
Go/VCS revision and dirty state rather than pretending a development build is a release.

All four target recipes and generated binary checksums were exercised at the packaging checkpoint.
Native amd64 controller/agent `--version` reported Darwin/amd64, v1, Go 1.25.5, cgo disabled,
protocol 1 and explicit unknown/unsupported containment/enforcement capabilities. Arm64 packages
were cross-compiled and inspected only, not executed. No Linux finite package exists yet. See
[deployment.md](deployment.md) for lifecycle/credentials and target evidence still required;
compilation and script presence never qualify service supervision or process-tree containment.

## Metadata and immutable identity

`internal/contracts` is the single Go definition of the implemented wire structs. JSON uses `schema_version: 1`. `Decode` accepts at most 1 MiB, rejects unknown fields, duplicate keys (including case aliases), nesting beyond 64 levels and trailing values. Call the concrete record's `Validate` **after** `Decode`: decoding does not silently apply semantic validation. `ValidateGraph` validates the bounded sealed unit list, duplicate IDs, missing dependencies and cycles before issuance. Graphs/packages/assignments have at most 1024 units; results have at most 1024 declared files. Large data is never embedded in these records.

- `Artifact`: lowercase SHA256 digest, byte size and media type; no mutable location.
- `Unit`: application unit ID and supplied scientific spec digest; input artifacts, operation, runtime compatibility, validator revision, dependencies and capabilities. `CompatibleUnit` compares the entire immutable definition and rejects mutation even when a caller reuses the supplied digest. Exact repeated registration is idempotent. Arrays are ordered and are not normalized (including nil versus empty); producers should retain one representation.
- `Package`: immutable ordered unit references and preparation artifacts. A package is not an acceptance unit.
- `Assignment`: run/assignment/worker/execution-session identity, package digest, unit references, runtime/profile/policy revision and finite deadline ordering. It contains placement, unlike `Unit`. An application spec or full unit digest does not change when placement/profile changes. Contextual checks (assignment subset of package, runtime compatibility, artifact existence and references to registered immutable units) belong to the consuming controller/agent.
- `Candidate`: unit/spec and assignment/attempt identities, outcome and declared relative file/artifact references. Paths must be clean, relative, non-traversing, at most 4096 bytes, with no backslash or NUL. Candidate validation is envelope validation, not scientific acceptance. Consumers must bind it to the issued assignment/pinned unit and verify bytes.
- `Digest` hashes Go JSON encoding of the supplied value; `Hash` hashes bytes. `Digest` is deterministic for these concrete structs, not a general cross-language canonical-JSON standard. Scientific `SpecDigest` is supplied by the application over its own immutable spec, not computed from host/profile metadata.

The foundation exposes immutable *record contracts*, not an immutable Go object abstraction; consumers must not mutate retained records and must persist/reject incompatible re-registration transactionally. There is no generic registry or controller scaffold here.

## Application stdin/stdout

Use `MessageReader`, `ReadMessage` and `WriteMessage` for newline-terminated protocol JSON (maximum 64 KiB per line). Stdout contains only protocol; stderr contains operational diagnostics and must be bounded by the agent.

1. Agent launches the application with an explicit executable/argv, local manifest and private output directory.
2. Application prepares once, then emits `ready`.
3. Agent durably records a start grant and sends `execute_unit` with `unit_id`. Only one unit is outstanding.
4. Application closes its result files and candidate manifest, then emits `unit_result` with `unit_id` and relative `manifest_path`.
5. Agent verifies and durably publishes locally, then sends `published` for the same unit. This is **local durable publication**, not controller receipt or domain acceptance; controller outages do not prevent continuing issued finite work.
6. Only after that acknowledgment may the next grant be sent. `finish` after any outstanding result is locally published elicits `finished` and successful exit.

Application exit, unexpected messages and missing bytes are operational failures. `finished` reports process completion; the agent still owns quiescence and result/remainder inventory. The fixture does not implement agent gates, deadline enforcement, journaling, durable artifact publication or controller acceptance.

## Synthetic executables

`cmd/hermes-synthetic` accepts `--manifest <fixture.json> --output <private-dir>`, optional
`--delay-millis` (per-unit delay override) and `--prepare-delay-millis` (before ready), each at most
60000 ms. These application-owned timing controls exercise outages and interruption, not production
resource policy. `testdata/synthetic/manifest.json` is a one-unit sample; its distant deadlines are fixture values, not deployment policy. The fixture manifest contains an assignment, full unit definitions, `failure` and `delay_millis` (0–60000). Failure values:

| Value | Behavior |
| --- | --- |
| empty | Correct deterministic per-unit result |
| `exit_prepare` | Nonzero exit before ready |
| `exit_unit` | Nonzero exit on first grant |
| `reject` | Complete, well-formed but domain-incorrect result |
| `validator_error` | Complete result requesting controlled validator operational failure |

`cmd/hermes-synthetic-validator` accepts `--manifest <candidate.json> --root <result-dir> --unit <pinned-unit.json>`. The pinned unit is required: self-consistency with arbitrary claimed result keys is not enough. An assignment-local fixture manifest validates its units and exact assignment correspondence; dependencies may refer to accepted units outside that assignment. The controller, not the fixture, validates the complete sealed graph and prerequisite acceptance. It verifies declared bytes and deterministic result identity/value. Success emits `Verdict{schema_version,valid,reason}` as JSON and exits zero, including `valid:false` for explicit domain rejection. Verdict decoding requires an explicit, non-null boolean `valid`; missing/null is a protocol error, not implicit rejection. Missing/corrupt files, malformed input or controlled operational failure produce stderr and nonzero exit **without a domain verdict**. A real controller must bound validator time, memory and output; a crashed validator is not a scientific negative.

The fixture accepts only its small synthetic files (at most 1 MiB). It writes closed files but does not itself promise crash-durable publication. A production agent must ingest them through durable publication before acknowledging `published`.

## Artifact-store integration

The local store in `internal/artifacts` exposes `New(root,maxSize)`, `Publish(ctx,digest,size,reader)`, `Contains(digest,size)` and `Open(digest)`. The parent directory of the store root must already exist on a trusted local filesystem. Only successful publication (verified bytes, no-replace publication and sync) permits committing metadata references. Construct the store under its controller/agent root authority before any publisher starts: reopening then discards only reserved regular ingress scratch from the previous, stopped publisher process. Cleanup enumerates names in batches once at open, never reads/hashes canonical payloads or scans on each admission. Unexpected objects in that private temporary namespace fail closed. `Publish` returning `artifacts.ErrIngressCleanup` means private scratch could not be removed: keep its affected transfer/storage reservation until successful exclusive reopen, even if a canonical blob exists. Canonical blobs, including unreferenced ones, remain retained; this is not garbage collection or proof that application children stopped. `Contains` is a metadata-only lookup, not a full rehash or proof that a concurrent publication has completed its directory sync. The controller must persist successful `Publish` outcomes before allowing new metadata references; mere path visibility is insufficient. The store has concurrency, invalid-input and ordinary reader-failure tests and a focused correctness review. Additional tests inject file/directory-sync errors and terminate a child process after file sync, after no-replace linking, and after directory sync before return; retry must preserve complete bytes and succeed without replacement. These exercise process-loss boundaries, not power loss or filesystem failure certification. The controller transaction/acknowledgment boundary is exercised separately by the connected integration tests below. See tests beside that package.

## Connected local slice (S2)

The implemented entrypoints are `dist/local/hermes-controller`, `dist/local/hermes-agent` and
`dist/local/hermes`, alongside the synthetic application/validator binaries. This is an engineering
slice for the configured **synthetic-local leaf-process profile**, not a qualified host service or
arbitrary application-process containment backend. S2 correctness and engineering reviews, including focused re-reviews of corrections, passed within
this scope. The plan owns subsequent slice gates and implementation status.

### Configuration and transport

- Controller flags: `--root`, `--listen` (default `127.0.0.1:8443`), `--ca`, `--cert`, `--key`,
  `--workers` (comma-separated authorized worker identities), `--validator` (absolute executable path).
- Agent flags: `--root`, `--worker`, `--session`, `--runtime` (absolute executable path),
  `--url`, `--ca`, `--cert`, `--key`, plus repeatable `--runtime-arg` for explicit application argv
  (never shell text). Worker and execution-session identities persist in its spool;
  do not mint replacement identities to bypass unfinished ownership.
- CLI: `hermes [--url ... --ca ... --cert ... --key ...] artifact-put <file>`,
  `submit <manifest>`, `status <run>`, `control <json>`, `control-status <run:id|worker:id>`, or
  `lost-spool <json>`. Put flags before the subcommand. A submitted manifest is
  `contracts.Submission`, not the application fixture manifest. Retain its request ID and repeat
  exactly the same submission to recover a lost response.
- Transport requires TLS 1.3 and CA-verified mutual certificates. Provisioned certificate common
  names are `operator` and `worker:<configured-worker-ID>`; the server certificate must match the
  configured URL. No plaintext mode or remote shell exists. Test certificates are created in private
  test directories; production credential provisioning/rotation and service permissions are not yet
  qualified. Keep this slice on the local synthetic endpoint.
- The entrypoints configure runtime/validator revision `synthetic-v1`, profile `synthetic-local`,
  an explicit worker allowlist, 1 MiB per artifact and finite assignment start/termination/handoff bounds of 5/6/7
  minutes from issue. These are synthetic development settings, not approved workload budgets.

### Ownership, bytes and acceptance

`internal/contracts/connected.go` owns the shared submission, poll/work, receipt, candidate/final
report, acknowledgment and aggregate status wire records. The complete package is carried with its
assignment; the agent checks its digest and assigned unit definitions before journal/start.
Inputs and preparation references are authorized for that assignment, downloaded and verified into
the local immutable store, then staged as `inputs/<sha256>` under the application's working directory.
That directory also contains `manifest.json`, `package.json` and private `output/`. Output manifests
and declared files are opened nonblocking and must be regular files before reading; a FIFO cannot
strand the agent outside the application's execution deadline. Idle poll replies must be an explicit
version-1 empty work envelope, not missing/versionless or dangling assignment data.

The controller holds an OS lock on its state root before opening authority. SQLite uses
`modernc.org/sqlite v1.44.3` (exercised SQLite **3.51.2**) with verified WAL, synchronous FULL and
foreign keys enabled. Definitions pin complete unit meaning globally; sealed run membership,
dependencies, unresolved owners and canonical acceptances have relational constraints. Assignment
reservation and request outcome commit before a reply. Empty polls reserve nothing and do not
create durable request rows; a later retry of that ID may reserve newly available work. Once a
reservation commits, reusing its request ID with a different body fails. A fresh poll cannot replay
an unresolved assignment after spool loss.

The agent journals receipt and process-start intent before execution, journals each unit grant,
and durably spools result bytes/manifests before acknowledging local publication to the application.
Receipt and result delivery retry independently of already-issued local grant progress. Controller
raw-publication acknowledgment follows durable blob registration and candidate commit, not domain
acceptance. The current agent retains even acknowledged local bytes; garbage collection is deferred.
Final execution/result inventory and canonical acceptance are separate.

A single asynchronous validator executes outside database transactions with a five-second process
timeout and 64 KiB stdout/stderr bounds. At most three completed attempts are allowed; operational
failures never become domain rejection. Controller-interrupted validation is requeued without
consuming the completed-attempt budget, including interruption during the third attempt. Exhausted
operational failures remain visible rather than becoming unschedulable pending work. A changed configured runtime/validator revision cannot validate a pending
unit under different meaning. First valid acceptance commits uniquely with dependent readiness;
differing candidates remain visible as conflicts without changing the canonical result. Status reads
aggregate metadata only; execution quiescence does not imply all units have been accepted.

### Evidence and current limits

The controller tests use a real loopback mTLS listener, a CLI subprocess, an independent agent loop,
and real synthetic application/validator processes. They exercise two prepared-once independent
units followed by a dependent package, input/preparation staging, delivery outages, lost replies,
identity/role/malformed-input checks, canonical acceptance and metadata-only status. Test-only
subprocess fault seams exit immediately before/after actual assignment, candidate and acceptance
commits; recovery checks rollback/persistence and atomic dependent readiness. A separate production
controller-binary kill test establishes post-acknowledgment persistence. None is power-loss or
service/host deployment certification.

Aggregate run status also reports uncertain/exhausted/data-loss/waiting-retained-validation counts.
Readiness uses indexed package metadata. Aggregate admission enforces approved logical reservations
with fresh native-capacity checks; this is not a hard CPU/RAM quota or generic descendant containment.
Finite exchange and managed Kaggle remain unavailable. Workflow continuation is an optional service
configured by an explicit local adapter approval. Ordinary domain rejection is not
retry authorization; resource escalation separately requires qualified same-attempt evidence,
quiescence and retained-result reconciliation. S3's leaf-only controls and
recovery additions are described below; correctness and engineering review gates passed within the
explicit leaf-only scope.

## Leaf execution control and recovery (S3)

Controller and agent are separate production executables. Killing the controller does not kill
issued agent computation; local publication continues through network outages. The supported
execution boundary is still **one synthetic leaf process with no descendants**. A live owned child
handle plus observed exit (`Wait` success or an exit-status error) can establish that leaf's quiescence.
Unknown wait errors do not. No numeric PID/PGID discovery, signaling or process adoption is used on
restart; generic process-tree containment and service deployment remain unqualified.

### Durable drain barrier

`control` consumes `contracts.ControlRequest`: stable request ID, target kind (`run` or `worker`),
target ID, expected current revision, action (`drain` or `resume`) and finite drain termination time.
It commits issuance pause plus affected assignment controls before returning `intent_persisted`.
That response is not a completed drain. Poll `control-status` for persisted revision, pending
barriers, live assignments, uncertainty and drain state. Unknown run/worker targets fail rather than
appearing eligible; a known target with no controls is eligible at revision zero. Resume permits future issuance only; it does
not reverse a drain already applied to issued work. Run and worker barriers survive restarts.

Each agent grant and drain application serialize on the same journal mutex. Drain persists its
revision and last granted unit before acknowledgment; no later grant can cross that boundary.
Already granted work may finish. A leaf monitor requests cooperative termination early enough for
the configured grace and forced termination at the earlier of the original or operator cutoff.
All signals/cancellation use one checked owned-handle path. Signal failure closes the gate, unblocks
local protocol reads and reports the bounded operational cause as enforcement uncertainty, never a
successful final. Exactly one asynchronous wait observes exit; failure/termination cleanup waits at
most 100 ms for that observation, then remains uncertain if actual exit has not been proved. A later
file or signal result alone does not clear uncertainty. The synthetic grace defaults to 100 ms;
original finite deadlines are never extended for delayed control or restart. Clock rollback against
the retained receipt closes starts conservatively.

Execution quiescence, raw-result delivery and final inventory remain distinct. Finals partition the
assignment into completed, interrupted and never-started units. A lost control acknowledgment retries
the same request/revision; stale reconciliation cannot regress recorded grant progress or clear
uncertainty. Unreachable agents and expired deadlines alone do not release ownership.

### Restart and irrecoverable spool loss

A retained marker-free receipt must reconcile before first start. A preparation/unit-start marker
without persisted quiescence remains uncertain after agent restart: retain the journal, publish any
retained candidates and refuse new starts. Do not change the worker/session identity or delete its
journal to bypass this state. Retained quiescent work republishes pending results/final inventory
without replaying accepted computation.

`lost-spool` consumes `contracts.LostSpoolResolution` through authenticated operator access. It is
only for **both** positively established exact-execution termination (or confirmed machine shutdown)
**and** irrecoverable spool loss, bound to the original assignment/worker/session with an evidence kind
and explanation. It is not a duplicate-risk override for uncertain running work. Reconcile all
controller-retained candidates/validation first; then record operational data loss and retry only
missing units within the configured attempt budget (synthetic default two attempts). Retained
acceptances are reused; pending or unresolved operational validation prevents recomputation.
Never demand an impossible final inventory from a confirmed lost disk. Newly applied final reports
for such terminal-resolved assignments are rejected: delayed old inventory must not refund attempts,
overwrite reasons or alter a newer owner's accounting. Previously committed request outcomes remain
idempotent, and late candidates still use ordinary publication/validation.

### Current evidence boundary

Local synthetic tests exercise separate production controller/agent processes during controller
SIGKILL and restart, surviving issued computation without accepted-prefix replay, two logical-worker
drain/run barriers, persisted pause, grant/drain races, sync failure, finite leaf cutoffs including a
SIGTERM-ignoring leaf, uncertain agent-process restart with retained candidate publication, offline
stop-new-unit expiry, late old-candidate acceptance while retry ownership remains live, and
terminal-proven missing-only recovery. Two roots on one test host model separate logical workers;
they do not qualify multiple agents on a physical host or aggregate admission. Full S3 checks and
independent correctness/engineering re-reviews passed within this scope; subsequent slice gates
remain tracked in the plan. These tests certify neither arbitrary descendants
nor launchd/service-account behavior.

## Host concurrency foundation (S4 partial)

The reviewed host-manager migration uses one agent root lock, stable worker/session identity and
shared immutable blob store for a bounded set of assignment-local runners. Each assignment retains
its own journal, start/control gate, deadlines, process handle and delivery inventory. Two synthetic
package processes have been exercised concurrently under that one host authority; this does not yet
qualify aggregate resource admission, which remains in progress.

`platform.ObserveResources(path)` is the shared native resource-fact source for controller and agent:
available processor count, Darwin `hw.memsize`, and filesystem total **and separately available** bytes.
An available-byte value of zero means a full filesystem, not permission to substitute total capacity.
These observations can restrict but never enlarge operator-approved budgets; they prove no hard
memory enforcement. Errors, invalid quantities and arithmetic overflow return unknown/zero capacities
with an error, not a partial admission fallback. Non-Darwin observation currently fails unsupported;
Linux host physical RAM must not be mistaken for effective notebook/container/cgroup limits.

`platform.NativeThreadEnvironment(cpuMilli)` derives Go and common OpenMP/BLAS thread hints from
the ceiling of a positive approved CPU allocation, without inheriting the caller's environment.
Callers must admit the allocation against actual capacity and explicitly construct the remaining
execution environment. These cooperative hints are not OS CPU quotas, universal library enforcement
or descendant containment; coupled caller/native qualification remains part of the S4 gate.

`active.json` holds the bounded active receipt/admission inventory and one durable pending poll.
Assignment journals/work directories live under `active/<assignment-hash>/`; acknowledged terminal
archives live under `completed/<assignment-hash>/`; `blobs/` is shared. A final acknowledgment is
persisted before archival and active-index removal. Recovery loads only indexed active metadata,
with exact-ID lookup for an interrupted archive move, not historical result payloads. Missing,
unindexed or mismatched active records block starts. Ambiguous mixed legacy/active metadata also
blocks inspection/recovery; migration accepts only the exact single indexed legacy receipt and an
absent/empty target directory, never guessed lifecycle facts.

Poll requests carry retained assignment identities/digests. All unresolved work must be represented
and reconciled before another assignment can be issued; extra entries are allowed only for exact
known terminal assignments, accommodating final-acknowledgment races. The pending request/body is
durable before sending, and an empty read-only response reuses that ID rather than fsyncing a new
idle request each poll. Missing old journals never become permission to recover execution rights
from a controller response alone.

Persisted journals are private metadata bounded at **16 MiB**, separately from the 1 MiB wire bound.
Both serialization before replacement and recovery decoding enforce the same limit. Start,
quiescence, uncertainty and final-acknowledgment facts require explicit non-null booleans. Candidate
append and journal commit share delivery's mutex; a failed save rolls back unpublished candidate
visibility and closes grants. The application receives no local-publication acknowledgment for a
candidate whose durable record cannot be recovered.

`hermes-agent check-quiescence --root <absolute-path>` needs no TLS or runtime arguments. It reads
under the existing exclusive host lock and emits version-1 JSON: `quiescent`, `uncertain` or `busy`,
with bounded reasons/assignment details and pending-delivery count. Only `quiescent` exits zero.
It creates no files, contacts no controller and signals no process. A missing/incomplete/ambiguous
root is uncertain, not an idle root to initialize. Proven leaf exit may coexist with unpublished
results; those bytes and identity/recovery metadata must be preserved. The lock is released before
return, so lifecycle callers must first disable/unregister managed starts and exclude concurrent
manual starts. The inspector does not clear intact uncertain execution or authorize reassignment.

The retained-byte/cache index in private `spool.sqlite` is separate local bookkeeping, not a second
controller authority. Its cached totals/revision and indexed owner rows keep ordinary snapshots free
of historical filesystem/payload scans. A one-time bootstrap uses confined regular-file metadata;
root service/index files belong to the explicit baseline, while similarly named assignment files
still count. Actual archive rename preserves stable keys, but simultaneous active/completed copies
are an accounting error that blocks admission, not an excuse to merge their bytes. The host must
track all retained worktree/debris before releasing an allocation and must not use stale totals after
tracking errors.

Blob publication reserves a durable declared-byte intent before streaming, without holding SQL or
host/admission locks across hashing. Cache verification becomes true only after successful durable
artifact publication and index commit; incoming bytes are still verified on publication. Failed
ambiguous intents remain charged, and retained files remain charged after raw acknowledgment. This
is bounded admission accounting, not a hard filesystem quota or a garbage collector.

## Workflow CLI transport (S8)

The CLI implements `workflow-register <json>`, `workflow-control <json>` and
`workflow-status <workflow-id>` using its existing operator mTLS client. Registration and control
files are bounded strict `WorkflowRegistration` / `WorkflowControl` JSON from
`internal/contracts/workflow.go`; preserve their explicit request IDs on retry. Responses print
versioned, identity-matched workflow status JSON. The application submits graphs, settings references
and decision identities, never executable argv or a server approval. Adapter approval remains local
controller configuration.

Enable the optional controller service with `--workflow-adapter-config <file>`. The bounded local
JSON requires `schema_version: 1`, `contract`, lowercase SHA256 `runtime_digest`, absolute executable
`argv`, and positive `timeout_millis`, `max_metadata_bytes`, `max_log_bytes`. This is a trusted,
side-effect-free leaf application; all copied bytes and invocations consume the controller's
approved storage/helper budget. Private runtime copies remain retained and charged, including
failed partial copies. The timeout bounds one invocation, not the logical workflow's lifetime.

Controller startup always installs workflow metadata and exposes operator-only workflow routes.
Without an approval file, existing status/control remain available but registration returns 503 and
no continuation loop runs. With approval, the controller verifies a protected private executable
copy and runs continuation asynchronously; insufficient capacity waits without consuming adapter
attempts. Shutdown cancels and joins validator/continuation loops before closing controller authority.

Real mTLS/CLI tests cover dispatch, replay, role guards and disabled registration. A built-controller
process test covers optional startup, charged runtime-copy creation, joined shutdown and restart
without approval. These are not a fresh multi-stage numerical application demonstration, service
manager qualification or cloud execution; those remaining gates are tracked in the implementation
plan.
