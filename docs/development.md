# Local development and executable contracts

This is the authoritative development workflow and wire-usage guide. Architectural guarantees remain in [design.md](design.md). All fixture inputs are synthetic; these commands authorize no deployment or research-data access.

## Supported workflows

The module is `hermes`. The exercised native toolchain is Go **1.25.5**, macOS amd64; `go.mod` requires that version and `Justfile` pins `GOTOOLCHAIN=go1.25.5`. Go and `just` must be available on the development machine. This is not a target-host installation requirement.

Use Pi's provided tools:

- `build`: discovers `Justfile` recipe `build`; compiles `./...` with `CGO_ENABLED=0`.
- `lint`: recipe `lint [paths...]`; formats supplied files/directories with gofmt and runs `go vet ./...`. Without paths it formats `cmd` and `internal`. When coordinating concurrent writers, supply only owned paths. Vet is read-only and repository-wide.
- `test`: recipe `test [paths...]`; runs `go test ./...` or the supplied repository-relative package directories. Focused example: `internal/contracts`.
- `deps`: discovers `go.mod`; use it for dependency add/remove/update rather than editing dependencies manually.

These recipes are also available directly as `just build`, `just lint [paths...]`, `just test [paths...]`. Run build, lint, then test after changes. The contracts tests compile and launch the two real fixture executables in test-private directories. They exercise stdin/stdout, preparation once for two units, incremental output, explicit local-publication acknowledgments, finish, domain rejection, validator operational failure and controlled preparation/unit process failure.

SQLite selection is **modernc.org/sqlite v1.44.3**, pinned through `deps` in `go.mod`/`go.sum` with its transitive dependencies. It is a pure-Go driver, keeping the agent build cgo-free. S1 does not open a database; S2 owns driver import, SQL and SQLite settings/qualification. Until that import exists Go marks the dependency indirect. No SQLite durability or runtime behavior is claimed by a successful fixture build. No copied Dagu source or dependency is used.

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

The local store in `internal/artifacts` exposes `New(root,maxSize)`, `Publish(ctx,digest,size,reader)`, `Contains(digest,size)` and `Open(digest)`. The parent directory of the store root must already exist on a trusted local filesystem. Only successful publication (verified bytes, no-replace publication and sync) permits committing metadata references. `Contains` is a metadata-only lookup, not a full rehash or proof that a concurrent publication has completed its directory sync. The controller must persist successful `Publish` outcomes before allowing new metadata references; mere path visibility is insufficient. The store has concurrency, invalid-input and ordinary reader-failure tests and a focused correctness review, but injected file/directory-sync crash-boundary evidence remains outstanding. See tests beside that package.
