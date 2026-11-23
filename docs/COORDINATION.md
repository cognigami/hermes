# Coordinating Hermes implementation

Guidance for a coordinator implementing the [design](design.md) through the [implementation plan](implementation-plan.md). Project instructions and the operator's actual release remain authoritative; a plan is not permission for remote deployment, cloud submissions or application/data changes.

## 1. Delegate execution; retain context and decisions

The coordinator owns the overall context, sequencing, interfaces, task boundaries, integration, review assignments and decisions returned to the operator. Synthesize evidence rather than merely forwarding messages. Keep the architectural intent and current decisions available across worker handoffs.

**Delegate as much substantial implementation, investigation and review as can proceed independently.** Give workers concrete bounded tasks while the coordinator continues integration, interface decisions, planning or other useful work. Keep trivial edits and tightly coupled immediate decisions local. Do not create agents merely to mirror headings, duplicate ongoing work, or delegate tasks that require continuous supervision.

Concurrent writers need disjoint files and responsibilities. Agree shared interfaces first, serialize shared-file changes, or give one owner the coupled work. The coordinator normally owns agent creation, shared contracts, integration and authoritative documentation. Delegation does not transfer responsibility for correctness or completion.

Use a compact task/status table in the implementation plan for temporary progress. Keep durable behavior and decisions in the design or relevant subject documentation, not transcripts or the README. Existing source control provides ordinary history; do not add a registry, dashboard or coordination framework.

## 2. Pi launch and Herdr placement

The operator launches the coordinator in a Herdr workspace. Every Pi agent it launches, including workers and reviewers, must receive:

```text
--presets:preset eng
```

The task brief supplies the role. Use tabs in the coordinator's own workspace, not splits in its tab or the workspace currently displayed by another client. Each tab's returned root terminal pane is the correct place to start its agent.

Use the provided Herdr tool. At startup inspect `HERDR_ENV` and the coordinator's own `HERDR_WORKSPACE_ID`; require `HERDR_ENV=1` before controlling Herdr. Never guess IDs or copy a previous session's workspace. Consult `herdr --skill` and relevant `--help` when needed; never run bare `herdr`. Respect sandbox denials and do not work around them.

Command shapes below describe the tool arguments; use the installed tool rather than shelling out. `$PWD` is the actual worker repository, and the workspace value comes from the coordinator's environment:

```sh
herdr tab create --workspace "$HERDR_WORKSPACE_ID" --cwd "$PWD" --label hermes-artifacts --no-focus
```

Read `result.root_pane.pane_id` from the creation result. Use that actual pane ID:

```sh
herdr agent start hermes-artifacts --kind pi --pane "$WORKER_PANE" -- --presets:preset eng
```

Keep `--no-focus` on background tab creation. For authorized sibling-project work, explicitly set that repository as cwd and include its local instructions. Do not use a convenience launcher unless it demonstrably preserves the own-workspace tab placement and required preset. Descendant launches obey the same rules.

Use Herdr for launch/placement and Pi session tools for assignments and reports. Find the live target with `session_list`, then use `session_send_message` with `report_back: true` when a response is needed, and `false` otherwise. Brief workers to send a completion report to the coordinator's session ID. Reports must distinguish completed work, failed checks, uncertainty and actual blockers.

Continue useful work after sending. If the next dependency is the reply and nothing independent remains, end the turn and resume when it arrives; do not sleep, poll or use Herdr wait commands as the normal coordination loop. For a blocked/stalled/timed-out launch or prompt, inspect the existing agent before resubmitting: timeout does not prove nondelivery. Do not answer approval dialogs, close unrelated tabs or stop the Herdr server for the operator.

## 3. Review purposes and triggers

| Review | Trigger | Focus | Not its job |
|---|---|---|---|
| Correctness | A meaningful completed slice will become a downstream dependency | Contract fidelity, state transitions, identity/durability, failures, concurrency, tests and integration assumptions | Redesigning Hermes or inventing generic defensive machinery |
| Engineering | Major work is integrated, or a cross-module change needs an architectural check | Simplicity, comprehensibility, ownership, unnecessary dependencies/abstractions and duplicated mechanisms | Requiring architecture for hypothetical needs or reviewing every small metadata edit |
| Application/domain | An integration changes an application's calculation/result boundary or claims compatibility | Application-owned identities, validation, initialization, dependency/freeze semantics and retained results | Giving infrastructure authority to change scientific rules or access data |

Correctness review should prevent defects compounding without becoming a panel for every edit. One fresh reviewer may cover a small assembled slice; reuse that reviewer for focused follow-up. Distinct review purposes do not require permanently running agents. Core infrastructure work does not need a statistical-finance review merely because Perry is its first consumer.

Reviewers are read-only by default, including no mutating lint commands. Supply the exact design/task, changed files, checks, result and execution restrictions. Prefer independent reasoning or an independently checked result over repeating a worker's claims. Findings must identify a concrete defect, consequence and minimum correction.

Evaluate recommendations as evidence, not new requirements. Fix confirmed defects and request focused re-review. Engineering review examines integrated code, not just isolated patches. Domain review belongs with the application owner when actually needed; no reviewer gains source-data access, remote execution or study authority by assignment.

## Cross-project contact

Ask the operator before contacting an agent in another project unless that communication is explicitly authorized. A Perry integration in this plan does not authorize directing Perry's active coordinators, inspecting running studies, changing their jobs or contacting other sibling projects. Raise genuine cross-project dependencies without creating routine operator gates for local engineering decisions.

## 4. Assignment and integration minimum

Every task brief states:

- Objective, authoritative specification and expected result.
- Exact cwd and allowed write scope, or **read-only**.
- Established dependencies/interfaces and other agents' ownership.
- Execution/data authority, relevant checks and bounded effort.
- Required completion report, review role and genuine escalation conditions.

Workers use the provided build/lint/test/dependency and VC tools. The first implementation slice establishes documented workflows; until then, do not invent runnable project commands. Reviews and integration use the same supported workflows.

The coordinator retains context and decisions by updating authoritative subject documentation alongside changes, keeping the plan's short status table current and including necessary decisions in subsequent briefs. Preserve unit identities, first-valid acceptance, automatic recovery and unattended continuation through all handoffs. Resolve routine implementation choices directly; escalate only actual scope/contract changes, licensing choices with project consequences, unavailable required access or unresolved application semantics.

Do not silently turn tests into deployment authority. No production run is a smoke-test fixture. Existing Perry procedures are integration references, not permission to operate its machines, datasets or Kaggle account.

## 5. Implementation-plan requirements

Each plan provides scope/exclusions, ordered verifiable slices, dependencies, concrete modules and disjoint write scopes, proportional checks/reviews and a copyable coordinator start brief. Separate current behavioral contracts from proposed work and temporary progress.

Revise a slice if a real prerequisite is unresolved. Do not stop unrelated useful work or request approval for every routine engineering decision. Before handing a slice to the next owner, integrate its contracts/docs and report what was actually verified.
