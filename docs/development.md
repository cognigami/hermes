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

Target deployment packaging and `just build-agent <goos> <goarch>` are not implemented by this foundation. Compilation alone does not qualify service supervision or process-tree containment.

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

`cmd/hermes-synthetic` accepts `--manifest <fixture.json> --output <private-dir>`. `testdata/synthetic/manifest.json` is a one-unit sample; its distant deadlines are fixture values, not deployment policy. The fixture manifest contains an assignment, full unit definitions, `failure` and `delay_millis` (0–60000). Failure values:

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

The local store in `internal/artifacts` exposes `New(root,maxSize)`, `Publish(ctx,digest,size,reader)`, `Contains(digest,size)` and `Open(digest)`. The parent directory of the store root must already exist on a trusted local filesystem. Only successful publication (verified bytes, no-replace publication and sync) permits committing metadata references. `Contains` is a metadata-only lookup, not a full rehash or proof that a concurrent publication has completed its directory sync. The controller must persist successful `Publish` outcomes before allowing new metadata references; mere path visibility is insufficient. The store has concurrency, invalid-input and ordinary reader-failure tests and a focused correctness review. Additional tests inject file/directory-sync errors and terminate a child process after file sync, after no-replace linking, and after directory sync before return; retry must preserve complete bytes and succeed without replacement. These exercise process-loss boundaries, not power loss or filesystem failure certification. The controller transaction/acknowledgment boundary is exercised separately by the connected integration tests below. See tests beside that package.

## Connected local slice (S2)

The implemented entrypoints are `dist/local/hermes-controller`, `dist/local/hermes-agent` and
`dist/local/hermes`, alongside the synthetic application/validator binaries. This is an engineering
slice for the configured **synthetic-local leaf-process profile**, not a qualified host service or
arbitrary application-process containment backend. S2 correctness and engineering reviews, including focused re-reviews of corrections, passed within
this scope. The plan owns subsequent slice gates and implementation status.

### Configuration and transport

- Controller flags: `--root`, `--listen` (default `127.0.0.1:8443`), `--ca`, `--cert`, `--key`,
  `--worker`, `--validator` (absolute executable path).
- Agent flags: `--root`, `--worker`, `--session`, `--runtime` (absolute executable path),
  `--url`, `--ca`, `--cert`, `--key`. Worker and execution-session identities persist in its spool;
  do not mint replacement identities to bypass unfinished ownership.
- CLI: `hermes [--url ... --ca ... --cert ... --key ...] artifact-put <file>`,
  `submit <manifest>`, or `status <run>`. Put flags before the subcommand. A submitted manifest is
  `contracts.Submission`, not the application fixture manifest. Retain its request ID and repeat
  exactly the same submission to recover a lost response.
- Transport requires TLS 1.3 and CA-verified mutual certificates. Provisioned certificate common
  names are `operator` and `worker:<configured-worker-ID>`; the server certificate must match the
  configured URL. No plaintext mode or remote shell exists. Test certificates are created in private
  test directories; production credential provisioning/rotation and service permissions are not yet
  qualified. Keep this slice on the local synthetic endpoint.
- The entrypoints configure runtime/validator revision `synthetic-v1`, profile `synthetic-local`,
  one worker, 1 MiB per artifact and finite assignment start/termination/handoff bounds of 5/6/7
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

Preparation-started or unit-started unfinished work fails closed as uncertain on agent restart;
there is no process adoption or automatic replay. Drain, full process-tree lifecycle/reconciliation,
aggregate admission/resource recovery and indexed ready-package scheduling remain later slices.
The current scheduler scans package metadata, and status exposes only acceptance/quiescence,
validation-failure and conflict counts. There is no automatic retry of domain-rejected calculations,
finite exchange, managed Kaggle or workflow successor implementation yet.
