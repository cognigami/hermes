# WP05-D revival — distributed-compute lessons

Retrospective, November 21, 2026. Discussion input, **not an implementation plan, new operating policy, readiness certificate or execution release**. Current Perry procedures remain in [COMPUTE](../../perry/root/docs/COMPUTE.md), the [Kaggle operating procedure](../../perry/root/docs/kaggle-operating-procedure.md) and the Perry charter. Run-specific state remains in [readiness](../../perry/root/docs/research/wp05d-run-readiness.md).

**Relocation note:** This historical retrospective now lives in Hermes. See the [Hermes handoff](README.md) for subsequent decisions. All links into `../../perry/root/docs/` are optional historical evidence requiring a sibling Perry checkout; they are not Hermes procedures or prerequisites for understanding its discovery. Inline source paths such as `scratch/` refer to paths inside Perry, not Hermes.

## Scope and evidence

Reviewed the available indexed `revival-coord` thread and all six recorded conversation forks, including the abandoned internal continuation described in `mid-week`. Session recalls inspected the transcripts; original session records and repository documents were used for corroboration. Inherited messages were not counted as new work in every fork. Also reviewed the six supporting reviewer/investigator/watcher/check sessions below, and two separate repository-cleanup discussions for context. No study datasets, live compute controls or remote hosts were accessed. No numerical experiment, platform investigation or code change was performed for this retrospective.

This is a historical snapshot through the November 21 midday recovery discussion, not a live status report. The earlier, withdrawn revival coordinator is predecessor context from its freeze checkpoint, not a reconstructed deleted chat tree. Neither Kaggle GPU nor Windows execution was demonstrated by these sessions.

### Conversation coverage

```text
revival-coord  01a1e11d-c9be-77ff-9475-e7d90da47a11
├── round-robin  01a1ec70-6f95-73e7-aaca-cdd03b8b070e
└── mid-week  01a1eba0-b8d5-7238-b5e7-7ede41547416
    ├── multi-process-prep  01a1ef1b-d2ca-7238-b5e7-7ee00cf34466
    ├── weekend  01a1efcd-ff53-76f9-a09e-7b77f973bc2c
    ├── thu-check  01a1f3cc-0101-76f9-a09e-7b78150b64a0
    └── fri-check  01a1f940-543f-76f9-a09e-7b83f459ddc3
```

Supporting sessions:

- Cutoff/preparation reviewer: `01a1ebae-5ca6-75f3-a44f-93212ead9ba6`.
- Resource/retry reviewer: `01a1f3e5-51cd-70a0-8ab8-0065fb3fbc6a`.
- HTTP429 reviewer/investigator/stager: `01a1fba9-4fdb-74c9-8032-95e90505c2cc`.
- Scheduled 01:30 check child: `01a1fc46-4c2c-76f9-a09e-7b8ffc3360fa`. Recall could not interrogate this deferred session; its original transcript was inspected instead.
- Boundary/pickup watcher: `01a1fe65-a57b-7238-b02c-dbf30d97ed73`.
- Raw-recovery reviewer: `01a1feac-2191-708c-bcde-ab5e99fbfc79`.

Separate related, not fork descendants: `cleanup` (`01a1f9ac-03ef-75fd-9098-1fda355c3a29`) and `cleanup-design-review` (`01a1fb62-f2c6-75fd-9098-1fdcfaf06c3b`). Their organization proposals are not completed migrations or compute evidence.

## Overall assessment

**The central failure was repeatedly treating successful components as evidence of a working unattended system.** Valid kernels, a concurrent dispatcher, tiny host checks, passing focused tests and static review each established something useful. None alone established that the programme supplied independent work promptly, recovered cheaply, handled ordinary remote failures, saved results before expiry, or returned machines on time.

A second pattern was that the operator repeatedly supplied the missing system-level reasoning: distinguish chronology from execution order; prepare before competing with workers; retain compatible work; isolate one failed package; do not turn one exceptional allocation into the normal profile; distinguish hardware capacity from configured policy.

This was not simply an absence of requirements. COMPUTE already specified validate-once recovery, finite lifetimes, bounded whole-tree memory, actual progress and separate scientific outcomes. Several failures were gaps between those contracts and the integrated implementation.

## Incident spine

Operational dates below are November 2026; explicit clock times are EST.

| Period | Observed problem | Resolution / limit |
|---|---|---|
| Predecessor revival | Relocated paths/shebangs, poor logs, 3.79 MB embedded Kaggle source against its demonstrated 1 MB limit; submission diagnostics discarded; normal worker ran tests/admission work. | Freeze checkpoint withdrew readiness. Later repairs must not be attributed to the original failed launch. |
| Nov 15 trunk | Unnecessary fresh-versus-recovery question; production worker carried development work. | Removed automatic test/admission execution; reused admitted runtime and unchanged calculations. Small CPU production-product check passed and temporary resources were deleted. Not full-study throughput evidence. |
| Nov 17–18 | Programme submitted one month and awaited a barrier, rotating hosts rather than using them concurrently. First batching fix still let expensive lazy preparation starve dispatch/collection. | Independent-month batching, then explicit prepare-before-compute phases and reusable package pointers. Actual three-host process overlap followed. |
| Nov 18 preparation fork | First serial/parallel comparison exhausted its shared allowance during second parallel receipt validation. | Correctly inconclusive despite equal completed files. Later useful-work comparison built ten reusable inputs; indicative group-time ratio 1.41x, not identical-input or whole-study speedup. |
| Nov 19 | One package hit its individual memory ceiling and interrupted the programme. | Retained partial work, deferred that dependency, continued independent work. Exact-package single-task trial completed at about 6.37 GiB total owned peak. |
| Nov 19 evening | Coordinator generalized the single-task mitigation into blanket one-worker profiles on both local hosts. | Operator required normal pools plus one durable per-package exclusive retry. No evidence supported blanket serialization. |
| Nov 20 | Missing `CANCEL_ACKNOWLEDGED` mapping, then known download HTTP429 escaped startup recovery and killed the source. | Terminal-state fix, then narrowly scoped pending-collection handling. Independent progress continued; unresolved outputs still blocked eventual scientific transitions. |
| Nov 21 | Scheduled assistant started but denied process inspection ended its work without recovery/report. Two cloud jobs had raw outputs but lacked the ordinary retrievable session envelope. | Later explicit staged salvage passed normal validation: 34 full records plus 20 partial records. Missing remainder remained missing. General finalization/retrieval limitations were not thereby solved. |

## Lessons to carry into the next discussion

### 1. Define readiness around the complete operating path

The `round-robin` diagnosis was decisive: `Supervisor.execute((path,))` could only run one offered package. Causally correct historical inputs did not require monthly fits to finish in chronological order. Adapter concurrency and configured slots did not establish programme concurrency.

**Lesson:** name the capability being claimed. Numerical correctness, deployability, concurrency, interruption/recovery, representative capacity and total completion time are separate claims. Acceptance should follow the actual producer → preparation → dispatch → worker → save → collection → validation → dependency transition path. A small check can establish that path without pretending to forecast full runtime.

### 2. The controller is part of the workload

Expensive input reconstruction inside a lazy iterator delayed both dispatch and collection. Saved packages were rebuilt to rediscover identity. Later resumes still took hours scanning/rechecking retained packages before fresh dispatch. A successful memory trial had roughly seventeen minutes of collection after numerical completion.

**Lesson:** fitting speed is not delivered throughput. Preparation, serialization, transfer, acceptance and restart scans need separate cost and lifetime accounting. For this run the operator explicitly chose prepare-before-compute; that was a resource policy, not a universal ban on pipelining. More workers or GPUs cannot repair controller starvation.

COMPUTE's existing validate-once contract is the starting point, not permission to skip required checks. The following measurement resolves one concrete source of redundant resume work; it does not quantify all controller overhead.

#### November 21 addendum: repeated whole-input hashing and delayed polling

**Confirmed defect reported by the Perry recovery session:** `WorkPackage.work_key` recomputes SHA256 over canonical JSON of the entire payload on every access. `ResultStore.completed` accesses it twice per existing record, and `Supervisor._complete` repeats that lookup per expected subresult. Routine result/scheduling lookups therefore repeatedly serialize and hash large immutable inputs. The controller also postpones worker polling behind its ordinary-queue scan. Relevant Perry code is `perry/range_placement_compute.py` (`work_key`, `read_package`, `ResultStore`) and `perry/range_placement_supervisor.py` (`_complete`, `_collect`, `execute`). The reported 144 monthly inputs total 25.75 GiB of JSON (median 182.5 MiB; range 118.7–255.5 MiB).

**Isolated measured baseline:** one existing 130,528,699-byte package, with 34 already accepted records:

| Phase | Wall time | Full key hashes | Time in key hashing |
|---|---:|---:|---:|
| `read_package` | 11.5239 s | 2 | 4.6399 s |
| Bare `Supervisor._complete` | 153.6605 s | 68 | 153.5179 s (~99.91% of phase) |

The baseline JSON was inspected for this addendum. The reporting session describes a ≤300 s hard wall, one numerical thread, no optimizer/fitting or scientific-value inspection, no production writes and no job-control actions. This is a controller-cost measurement, not a fitting benchmark or an extrapolated whole-run speedup. Evidence: Perry-relative `data/wp05d-controller-efficiency-20261121/baseline.json`, with `measure_controller.py` alongside it (absolute artifact root: `/Users/ricardo/Documents/perry/root/data/wp05d-controller-efficiency-20261121/`). Run context remains in the [Perry recovery checkpoint](../../perry/root/docs/research/wp05d-kaggle-recovery-20261121.md).

**Reported operational observations (November 21, EST):** an 11:13 resume first dispatched a worker at 14:21; ordinary-queue checks ran from 12:28 to 15:40. A normal worker resource-stopped at 14:43, but the controller did not reach collection until about 15:40. These observations complement the isolated measurement; they do not attribute every elapsed minute to hashing. Source liveness or active-assignment metadata is not proof of useful computation: controller administration can dominate elapsed time while compute capacity is idle. Accepted outputs/prefixes remained durable. At the time of the report, both 13 GiB local retries were running and live work had not been interrupted for the fix.

**Proposed repair and validation, not a deployed result:** verify expensive input identity at the appropriate boundaries, reuse verified identities for read-only lookups, and poll workers between package checks. The operator authorized narrow Perry fixes, tests, measurement and review while fits continue; a package-boundary cutover is only a possibility if justified. After-fix measurement is still pending: no validated speedup or deployment is claimed. No scientific, source-data or model changes, cloud-memory policy expansion, or Hermes implementation/job-control work follows from this report.

**Lesson:** measure controller, collection and setup overhead separately from optimizer time. Do not treat restart warmup as historically fixed or unavoidable. Repeated safety proof on ordinary lookups can defeat existing immutable-publication guarantees rather than strengthen them; retain boundary checks without repeatedly re-proving the same large payload's identity.

### 3. Preserve the distinction between work, attempt and outcome

Useful distinctions were repeatedly collapsed:

- predicted cloud reference versus acknowledged submission;
- platform terminal/quiescent versus numerical worker outcome;
- complete file bytes versus published ready/completion marker;
- raw saved output versus scientifically validated acceptance;
- accepted intermediate record versus available forecast;
- resource interruption versus scientific unavailability;
- partial package versus completed dependency/stage.

**Lesson:** keep these meanings explicit across adapters. Exactly-once canonical acceptance is more realistic than promising exactly-once remote execution after an ambiguous submission. Do not infer absence or termination from an isolated 404, or numerical success from platform COMPLETE/exit zero.

These distinctions were a strength during salvage: the recovery did not fabricate `saved/session.json`, convert deadline into memory failure, or turn missing results into scientific negatives.

### 4. Recoverable is not the same as inexpensive to recover

Accepted prefixes, immutable results and attempt journals repeatedly saved real work. But resuming still had substantial serial cost, and live controllers did not support pickup of newly staged recovery evidence. Safe integration required a controlled stop/resume.

**Lesson:** separate retained durability, usable restart, restart latency and live repair capability. Do not add hot reload by reflex; first establish whether avoiding its complexity is worth the measured restart cost. Likewise, source hashing protected compatibility but repeatedly required exact reviewed hash aliases for orchestration-only changes. That is evidence for examining the calculation/operations boundary—not broadly excluding inconvenient files from identity.

### 5. Isolate ordinary failures without hiding broken invariants

An individual resource stop initially interrupted the whole programme. Later a rate-limited download did the same during startup. Narrow containment repairs allowed independent work to proceed while preserving unmet dependencies.

**Lesson:** resource exhaustion, temporary collection failure, uncertain ownership, programming error and scientific unavailability need different responses. A pending collection should not occupy a running slot or authorize duplicate work. Containment must also have an eventual resolution: the HTTP429 cooldown only retried on later encounter/resume, not periodically; unresolved results could still prevent completion.

Review found state-lifetime defects that simple happy paths missed: repeated execute calls, duplicate A before first poll in `[A,A,B]`, and restart with an old host disabled could bypass intended suppression/retry rules.

### 6. Resource allocation is policy, not hardware truth

The exact failed-package trial supported one larger allocation. It did not support serializing every fit or every local host. Later evidence broadened the concern: all 48 ordinary attempts in one overnight report were resource-stopped. The initial exception policy therefore deserved reassessment against new workload evidence, rather than remaining unquestioned forever.

Kaggle's configured 6 GiB worker ceiling was later incorrectly explained as a platform limitation. The runner actually used four slots/26 GiB aggregate; its exclusive retry was local-only by policy.

**Lesson:** distinguish machine capacity, per-attempt limit, aggregate budget, concurrency and routing eligibility. A policy can be correctly implemented and still poorly matched to the workload. More slots can reduce throughput when tasks repeatedly hit individual limits or overload collection. Neither one successful large fit nor a batch of resource-stop labels establishes the optimal policy or proves an OS OOM cause.

### 7. Final delivery is part of computation

Kaggle jobs had published raw result pairs but lacked the ordinary retrievable saved-session envelope. SDK filename filters were applied after listing/pagination; ZIP-first then raw fallback could repeat a large listing polluted by runtime files. Narrow-looking requests were not necessarily cheap requests.

Repeated prefix seeding serialized/fsynced existing immutable results. Observed 606/439-second heartbeat gaps exceeded the 120-second flush reserve. Finalization overrun is a plausible mechanism; the exact operation at hard kill was **not proved**.

**Lesson:** a fast calculation that cannot publish its result is unfinished work. Keep output sets deliberate, understand SDK request behavior, and account for growing checkpoint/finalization cost. A larger grace period alone cannot bound an uninterruptible growing operation. Raw salvage worked for two explicit cases; it did not fix the general bootstrap.

### 8. Stop, drain and deadline are different contracts

Per-job ten-hour clocks did not enforce morning handback. An absolute cutoff had to cover setup, workers and collection. Cooperative stops sometimes took longer than the immediate command wait. Later checks were between records, not inside every download/validator.

The successful boundary watcher issued one stop, then verified eventual source/worker quiescence. A new assignment could begin between observing the qualifying collection and processing stop: this was not an atomic drain barrier. Kaggle's permitted expiry-pending exception was not confirmed live cancellation.

**Lesson:** state what can be stopped, what may continue, what is preserved and when handback is actually confirmed. Do not promise zero lost in-flight work or bounded shutdown from a command receipt alone.

### 9. Progress should answer the operator's question

Worker labels and visible dispatch/collection events enabled discovery of serialization. Conversely, stale stopped/running snapshots, accepted counters delayed until refresh, changing work units and a “no advance” timer based only on collection repeatedly needed interpretation.

**Lesson:** separately show controller activity, worker computation, saved raw results, accepted outputs and remaining scientific dependencies. Calendar coverage is not remaining cost. Three-host process overlap is not continuous CPU saturation. A running source with no numerical workers may be correctly preparing—or stuck in expensive repeated control work. The log should make that difference understandable without an LLM investigation.

### 10. Reviews and tests need explicit claim boundaries

Independent review caught consequential defects: pre-submit timeout poisoning resume; duplicate/repeated-call deferral; disabled-host retry bypass; and a recovery wrapper missing `flush_seconds`, which would break unrelated cutoff-aware cloud dispatch. Review was valuable, not futile.

But most reviewer CLEARs were static inspections, not independent execution. Many passing focused tests did not establish production output-tree behavior, full-size memory, integrated utilization or restart cost. A fixture itself omitted a cloud reference and leaked sampler activity when reporting failed.

**Lesson:** prefer a few representative cross-boundary sequences over interpreting test counts as readiness. Include unfavorable ordering and interruption points. A bounded useful-work check can be more informative than another disposable smoke test; preserve honest inconclusive results when the allowance does not finish both arms. No need for an indiscriminate benchmark campaign.

### 11. Agent completion is also an operational contract

The 01:30 child started on time, then hit denied `/bin/ps` access and stopped, as sandbox policy required. Launcher return code zero meant Pi returned, not recovery succeeded. No completed report/notification reached the parent. Another staging task finished its transfer well before its completion handoff was sent. Forked sessions also encountered stale/unavailable coordinator IDs and duplicate live-session registration.

**Lesson:** deterministic run/control behavior should not depend on a chat agent improvising tools at night. Agents can investigate and supervise bounded interventions, but task outcome and notification must not be inferred from launch success. Clear allowed tools and a completion report matter as much as a detailed initial briefing. This is not a recommendation to build an agent scheduler.

### 12. Scope discipline is a system property

Small fixes repeatedly acquired wider interpretations: path/log requests became refactoring; unchanged work became a fresh-run decision; one oversized package became global serialization. The operator repeatedly restored the intended requirement.

**Lesson:** before a change, state the defect, smallest repair and evidence that would establish it. Review requirements as well as code. “No new framework” should prevent unnecessary infrastructure, not excuse a functionally serial distributed runner. Conversely, observed failures do not by themselves justify replacing everything with a general cluster platform.

## What is worth retaining

- Accepted-prefix reuse and immutable durable outputs genuinely preserved work.
- One canonical acceptance path preserved scientific checks during raw salvage.
- Explicit ownership, exact cloud versions, finite deadlines and conservative ambiguity handling avoided silent duplicate execution.
- Failure containment let independent work advance without relaxing stage/freeze barriers.
- Offline pinned runtimes, native host builds, private bounded inputs and production/test separation remain useful.
- Concrete read-only review plus focused regressions prevented several integration failures.

These assets argue against a reflexive rewrite. Their presence also must not be used to dismiss the integrated defects.

## Questions for next steps — not investigation authorization

Follow-up discovery: [standalone execution baseline and Dask assessment](distributed-execution-discovery.md) captures the operator's subsequent requirements. It now lives in the separate Hermes repository and remains documentation-only; the [handoff](README.md) records later direction favoring narrow custom infrastructure.

1. **Workload:** which future calculations actually benefit from a GPU, and at what task/batch size after preparation, transfer and collection? GPU hardware does not accelerate unchanged CPU-only code automatically.
2. **Task boundary:** what is the smallest useful, independently durable unit that amortizes startup/reuse while fitting memory and shutdown limits? Preserve scientific histories and acceptance requirements.
3. **Controller cost:** where does real wall time go, particularly after restart? Which repeated reads/serialization/checks can existing immutable acceptance already eliminate?
4. **Resource policy:** what normal concurrency and exceptional allocation fit the observed workload? For GPU work, separate host RAM, device VRAM, device ownership and host process limits.
5. **Platform contract:** what does each environment actually support for launch, identity, progress, termination and artifact delivery? Native Windows versus WSL is an unresolved choice; POSIX remote-shell/process-group assumptions are not portable guarantees.
6. **Runtime:** what explicit source/runtime artifact runs on each OS/architecture/device, with what numerical equivalence? Supported native sources under Perry's `scratch/` are a packaging/ownership concern identified by cleanup work, not permission for an immediate migration.
7. **Operating model:** what must function without Pi or an awake coordinator machine? How should unavailable hosts, remote expiry, rate limits and partial publication resolve within the authorized run?
8. **Build or adopt:** only after those requirements are explicit, compare the existing narrow mechanisms with a mature execution tool on the concrete capabilities they would replace. This history does not settle that choice.

The immediate output is a set of lessons and discriminating questions, not a platform selection or implementation release.

## Primary documentary evidence

These optional historical links require the sibling Perry checkout described above.

- [Original freeze/handoff](../../perry/root/docs/research/wp05d-r5-handoff-20261113.md): relocation, false readiness, source-size and production-admission defects.
- [Same-run readiness](../../perry/root/docs/research/wp05d-run-readiness.md): dated repairs, measured memory trial, ordinary/exclusive allocation, HTTP429 containment and remaining limitations.
- [Local preparation](../../perry/root/docs/research/wp05d-local-package-preparation-plan.md): implementation boundary, inconclusive comparison and useful-work check.
- [Overnight windows](../../perry/root/docs/research/wp05d-overnight-windows.md): absolute cutoff and superseding run instructions; these remain run-specific.
- [Kaggle recovery investigation](../../perry/root/docs/research/wp05d-kaggle-recovery-investigation-20261120.md): confirmed pagination behavior versus qualified finalization hypothesis.
- [Controlled raw recovery](../../perry/root/docs/research/wp05d-kaggle-recovery-20261121.md): 34+20 records, ordinary acceptance, wrapper defect and actual stop/resume/import evidence.
- [01:30 check](../../perry/root/docs/research/wp05d-0130-check-20261121.md): scheduled-agent outcome correction.
- [Repository organization design](../../perry/root/docs/repository-organization-design.md): separate proposed source/document ownership cleanup, not an implemented compute redesign.
