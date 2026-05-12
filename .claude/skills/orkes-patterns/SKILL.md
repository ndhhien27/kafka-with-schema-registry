---
name: orkes-patterns
description: Architectural pattern selection for Orkes Conductor in this repo — Sequential Pipeline, Decision-Driven Flow, Human-in-the-Loop with SLA escalation, Saga / Compensation, Scatter-Gather (Parallel Fan-Out), Iterative Loop, Sub-Workflow Composition, Schedulers, Webhooks. Pick the right pattern, compose multiple patterns into one workflow, and avoid the anti-patterns called out in the BTH training material. Use during workflow design, technical-architect review, or when classifying an existing process for orchestration.
---

# Orkes architectural patterns

Pattern-level guidance for designing Orkes workflows. Drawn from the
"Architectural Patterns" section of [docs/orkes_training.md](docs/orkes_training.md)
plus the 8-stage vendor-onboarding example in
[docs/codelab.md](docs/codelab.md). For task-type specifics, use
`orkes-tasks`. For workflow JSON shape, use `orkes-workflows`. For Event
Handlers (the front door for most patterns), use `orkes-event-handlers`.

## When to use

User is:
- Designing a new workflow and needs to pick the right pattern.
- Classifying an existing process to decide which Orkes pattern fits
  (Step 3 of the training's "Activate Your Pilot" workflow).
- Composing multiple patterns in a single workflow (e.g. fork-join inside a
  do-while inside a saga).
- Reviewing a Technical Architect deliverable.

## The six patterns at a glance

| # | Pattern | Trigger | Conductor features |
|---|---|---|---|
| 1 | Sequential Pipeline | Fixed steps, each feeds the next | `SIMPLE`/`EVENT` tasks with retry + timeout |
| 2 | Decision-Driven Flow | Routing by data | `SWITCH` (`evaluatorType: javascript` or `value-param`), `INLINE` for derived inputs |
| 3 | Human-in-the-Loop | Manual approval/review with SLA | `HUMAN` task with `__humanTaskDefinition`, escalation tiers, async completion |
| 4 | Saga / Compensation | Multi-step transaction needing rollback | `failureWorkflow` + `SET_VARIABLE` for rollback context |
| 5 | Scatter-Gather (Parallel Fan-Out) | Multiple independent checks/calls | `FORK_JOIN` (static) or `FORK_JOIN_DYNAMIC` (runtime count) + `JOIN` |
| 6 | Iterative Loop | Repeat until condition | `DO_WHILE` with `loopCondition` + `loopOver` |

Plus three composition primitives:

| Composition | When |
|---|---|
| **Sub-Workflow Composition** | Repeating series of tasks across multiple workflows — extract to `SUB_WORKFLOW` |
| **Scheduler** | Run a workflow on a cron cadence |
| **Webhook (Wait For Webhook)** | Pause for an external HTTPS callback (third-party services) |

## Pattern 1 — Sequential Pipeline

**Use when**: a fixed sequence of automated steps, each feeding the next.
The engine retries transient failures and enforces timeouts at every step.

**Example** (training): export booking — validate cargo details → check
vessel capacity → confirm rate → issue booking confirmation → dispatch
terminal instructions.

**Tasks**: `SIMPLE` / `EVENT` / `HTTP` with `retryCount`, `retryLogic:
EXPONENTIAL_BACKOFF`, `timeoutSeconds`, `responseTimeoutSeconds` on each.

**Reminder**: in Chorus, prefer `EVENT` (with `asyncComplete: true`) over
polling `SIMPLE` — see `orkes-event-handlers`.

## Pattern 2 — Decision-Driven Flow

**Use when**: the process routes to different paths based on data — an
amount, a risk score, a customer tier, a policy rule.

**Example** (training): container release routing — by cargo type, route
dangerous goods to mandatory inspection, reefer to temperature verification,
standard dry to auto-release.

**Tasks**:

```
upstream INLINE (compute the routing input)
    ↓
SWITCH (evaluatorType: javascript)
    ├── case "auto_release":         [tasks ...]
    ├── case "temperature_check":    [tasks ...]
    ├── case "mandatory_inspection": [tasks ...]
    └── defaultCase:                 [tasks ...]
```

**Always define `defaultCase`** — unmatched values fall through silently
otherwise.

**Codelab example**: the vendor-onboarding `route_by_risk` SWITCH branches
on a 0-100 score:
- 0-30 (LOW)  → AUTO-APPROVE (~2 min)
- 31-60 (MEDIUM) → COMPLIANCE REVIEW (~5 min)
- 61-100 (HIGH) → MANAGER REVIEW (~5-8 min, with SLA escalation)

## Pattern 3 — Human-in-the-Loop

**Use when**: the workflow pauses for a human to review, approve, reject, or
provide input. The engine enforces SLA deadlines and escalates automatically.

**Example** (training): dangerous goods approval — automated hazmat class
validation, then operations supervisor review with 4h SLA, escalate to
safety officer if breached.

**Tasks**: `HUMAN` task with `__humanTaskDefinition`:
- `assignments`: ordered list, each with `assignee` (user or group),
  `assigneeType` (`EXTERNAL_USER` / `EXTERNAL_GROUP`), and `slaMinutes`.
- `assignmentCompletionStrategy`: typically `TERMINATE_ON_COMPLETE`.
- `userFormTemplate`: **always `null` in Chorus** — forms live in the CHORUS
  product UI per the strict UI rule.

**Backstops**:
- Set `timeoutSeconds` ≥ 2× total SLA as a hard backstop.
- Use **async completion API** for programmatic approve/reject from the
  CHORUS UI — not Orkes drag-and-drop forms.

**Auto-escalation example**:

```json
"humanTaskDefinition": {
  "assignmentCompletionStrategy": "TERMINATE_ON_COMPLETE",
  "userFormTemplate": null,
  "assignments": [
    { "assignee": "OPS Supervisor - GHQ", "assigneeType": "EXTERNAL_GROUP", "slaMinutes": 240 },
    { "assignee": "safety-officer@one-line.com", "assigneeType": "EXTERNAL_USER", "slaMinutes": 60 }
  ]
}
```

After 4h with no action by the OPS Supervisor group, the task escalates
automatically to the safety officer; if they don't act within 1h, the
`timeoutSeconds` backstop fires and the workflow's `failureWorkflow`
triggers (if defined).

> **Pascal Case `taskReferenceName`** — renders as the task name in the "My
> Task" UI. Use verb-noun form (`ApproveDangerousGoods`, `ReviewDocuments`).

## Pattern 4 — Saga / Compensation

**Use when**: a sequence of steps where each step has a corresponding "undo"
action. If step N fails, the workflow automatically compensates steps
N-1...1 in reverse order.

**Example** (training): export booking — reserve vessel slot → collect
surcharges → issue bill of lading. If B/L issuance fails, reverse surcharge
collection → release vessel slot.

**Mechanism**:

1. Define a `failureWorkflow` on the main workflow.
2. As the main workflow progresses, `SET_VARIABLE` records rollback context
   (e.g. `vesselSlotReservationId`, `paymentId`).
3. On uncaught failure, Orkes triggers the `failureWorkflow` with the
   failed run's input + variables.
4. The compensation workflow reads `${workflow.variables.X}` and runs the
   inverse operations in reverse order.

```json
{
  "name": "exp_{team}_export_booking",
  "failureWorkflow": "exp_{team}_export_booking_compensation",
  "variables": {
    "vesselSlotReservationId": null,
    "surchargeTransactionId":  null,
    "blDraftId":               null
  },
  "tasks": [
    { /* reserve_vessel_slot ... */ },
    { /* SET_VARIABLE: vesselSlotReservationId = ... */ },
    { /* collect_surcharges ... */ },
    { /* SET_VARIABLE: surchargeTransactionId = ... */ },
    { /* issue_bill_of_lading ... */ }
  ]
}
```

```json
// exp_{team}_export_booking_compensation.json
{
  "tasks": [
    { /* if surchargeTransactionId, reverse_surcharge ... */ },
    { /* if vesselSlotReservationId, release_vessel_slot ... */ }
  ]
}
```

**Per training**: the compensation workflow **must be designed and created
alongside** the primary workflow — not bolted on later.

## Pattern 5 — Scatter-Gather (Parallel Fan-Out)

**Use when**: multiple independent tasks run in parallel; aggregate results
when all (or a subset) complete.

**Example** (training): trade compliance screening — run sanctions check,
commodity embargo check, export licence validation simultaneously, then
aggregate results for booking approval.

**Tasks**:

| Variant | When | Task |
|---|---|---|
| Static count | Branches known at design time | `FORK_JOIN` + `JOIN` |
| Dynamic count | Branches determined at runtime (e.g. one per document) | `FORK_JOIN_DYNAMIC` + `JOIN` |

**JOIN semantics**: lists the **last task ref name in each branch** in
`joinOn`. Completes when all listed tasks complete (success or failure).

**Codelab example**: vendor-onboarding `parallel_compliance_checks`
fork-joins three checks (sanctions, insurance, licence) in parallel.

**Composition tip**: drop a `FORK_JOIN` inside a `DO_WHILE` to fan out per
iteration (e.g. for each document, run all checks in parallel).

## Pattern 6 — Iterative Loop

**Use when**: a block of tasks repeats until a condition is met — a document
is approved, retries are exhausted, or a threshold is reached.

**Example** (training): BL amendment — shipper submits correction → system
validates → if invalid or further changes requested, notify and wait for
re-submission → repeat until approved or max attempts reached.

**Tasks**: `DO_WHILE` with `loopCondition` (JS) and `loopOver` (the body).

```json
{
  "name": "verify_documents_loop",
  "taskReferenceName": "verify_documents_loop",
  "type": "DO_WHILE",
  "inputParameters": { "documents": "${normalize.output.result.documents}" },
  "loopCondition": "if ($.verify_documents_loop['iteration'] < $.documents.length) { true; } else { false; }",
  "loopOver": [
    /* per-iteration tasks: e.g. publish doc-verify EVENT (asyncComplete) */
  ]
}
```

**Iteration index**: `$.[taskRefName].iteration` is available inside both
`loopCondition` and the body's task `inputParameters`.

**Per-iteration outputs**: each body task's outputs are arrays indexed by
iteration: `${verify_doc.output.status[iterationIndex]}`.

## Composition primitive — Sub-Workflow

**Use when**: the same series of tasks repeats across multiple workflows
(extract for reuse) **or** a workflow grows beyond ~15 tasks (extract for
readability).

**Tasks**: `SUB_WORKFLOW` invoking another registered workflow definition.
The sub-workflow's `outputParameters` becomes
`${run_compliance_screening.output.X}` in the parent.

**Practical examples**:
- Extract "compliance screening" (sanctions + insurance + licence checks +
  scoring) into a sub-workflow shared by vendor onboarding, customer
  onboarding, and counterparty review.
- Extract "human approval with N-tier escalation" into a parameterized
  sub-workflow.

## Composition primitive — Scheduler

**Use when**: a workflow must run on a cadence (nightly, hourly, weekly).

**Mechanism**: configure a Scheduler in Orkes UI with a Cron expression
(e.g. `0 0 * ? * *` for nightly midnight). The scheduler triggers a workflow
start with predefined input.

Don't confuse with Event Handlers — schedulers are time-driven, handlers are
event-driven.

## Composition primitive — Webhook (Wait For Webhook)

**Use when**: pausing for an external HTTPS callback from a third-party
service (Sendgrid, Twilio, GitHub, custom services).

**Difference from Event Handlers**: Event Handlers actively listen on a
queue; Webhooks actively wait for an HTTP POST. Implementation: pair a
globally-defined Webhook with a `Wait For Webhook` task inside the workflow.

**Capabilities**: configurable timeouts, payload authentication, can
dynamically spawn a new workflow on receipt.

## How patterns compose (the codelab walkthrough)

The vendor-onboarding workflow in [docs/codelab.md](docs/codelab.md) chains
multiple patterns:

```
[Event Handler — Pattern 0 (entry)]
    │
    ▼
JSON_JQ_TRANSFORM (normalize)               <- Sequential Pipeline
    │
    ▼
EVENT publish workflow_started              <- Pattern 0 echo (notify)
    │
    ▼
DO_WHILE (verify_documents_loop)            <- Pattern 6 Iterative Loop
    │   └── EVENT publish doc_verify (asyncComplete)
    │       paired with completion handler
    │
    ▼
FORK_JOIN (parallel_compliance_checks)      <- Pattern 5 Scatter-Gather
    ├── EVENT publish sanctions_screen
    ├── EVENT publish insurance_validate
    └── EVENT publish license_verify
    │
    ▼
INLINE (calculate_risk_score)               <- inline computation
    │
    ▼
SWITCH (route_by_risk)                      <- Pattern 2 Decision-Driven
    ├── auto_approve  → EVENT publish notify
    ├── compliance_review → HUMAN ApproveByCompliance, then EVENT
    └── manager_review    → HUMAN ApproveByManager (with SLA escalation)  <- Pattern 3
    │
    ▼
EVENT publish final_notification            <- Pattern 0 echo (close)
    │
    ▼
TERMINATE
```

If any of the EVENT publishes fails irrecoverably and a `failureWorkflow`
were defined, **Pattern 4 (Saga)** would kick in for compensation.

## Anti-patterns (Chorus standard)

| Anti-pattern | Why | Use instead |
|---|---|---|
| Job-worker polling with `SIMPLE` task | Network overhead, misaligns with event-driven arch | `EVENT` + `Wait` + Event Handler chain (Pub/Sub model) |
| `JDBC` task for direct DB access | Violates strict DB isolation principle | Backend service via `EVENT` task |
| Orkes drag-and-drop User Forms in production | Strictly prohibited | Forms in CHORUS Design System; complete via API |
| `SWITCH` without `defaultCase` | Silent fall-through on unmatched values | Always define `defaultCase` |
| `EVENT` `asyncComplete: true` without paired Event Handler | Task hangs `IN_PROGRESS` forever | Pair every async-complete EVENT with a `complete_task` handler |
| `failureWorkflow` added after the fact | Compensation is hard to retrofit | Design saga + compensation **together** |
| Polling for HUMAN task completion | Defeats the durable-state benefit | Async completion API on UI submit |
| One handler with overly broad `condition` | Multiple flows accidentally collide | Team-prefix `metadata.type` and match exactly |

## Pattern selection — quick decision tree

```
What's the dominant shape?
│
├── Linear, fixed steps?                          → Pattern 1 (Sequential Pipeline)
├── Branches by data?                             → Pattern 2 (Decision-Driven Flow)
├── Needs a human approval?                       → Pattern 3 (Human-in-the-Loop)
├── Multi-step transaction needing rollback?      → Pattern 4 (Saga / Compensation)
├── Multiple independent things in parallel?      → Pattern 5 (Scatter-Gather)
└── Repeat until condition / per-item processing? → Pattern 6 (Iterative Loop)

Then ask:
- Is part of this reusable across workflows? → Extract a SUB_WORKFLOW
- Should this run on a cadence?              → Add a Scheduler trigger
- Need to pause for an HTTP callback?        → Add a Wait For Webhook task
```

Most non-trivial workflows compose **3-5 patterns**. The codelab combines
Patterns 1, 2, 3, 5, 6 plus the asyncComplete request/response chain.

## Capability ownership (RACI from training)

| Capability | PPO | Tech Architect | Developer |
|---|---|---|---|
| Identify orchestration-fit processes | Owns | Reviews | Aware |
| Translate policies/SLAs to workflows | Owns | Reviews | Builds |
| Model complex workflows + evaluate patterns | Aware | Owns | Builds |
| Implement NestJS + Kafka workers | — | Reviews | Builds |
| Configure retry/timeout/compensation | — | Reviews | Builds |
| Use operational dashboards | Owns | Reviews | Aware |

The Technical Architect owns pattern selection; this skill is the reference
they consult during design.

## Pre-design checklist

- [ ] Process exhibits 3+ orchestration signals (multi-service, human, long
      duration, compensation needs, audit trail, SLA, branching, sub-process
      reuse) — see `orkes` master skill's "When NOT to orchestrate"
- [ ] Picked one or more of the six patterns above
- [ ] If saga: `failureWorkflow` and `SET_VARIABLE` calls planned
- [ ] If human task: SLA tiers + `timeoutSeconds` backstop both planned
- [ ] If parallel: `joinOn` references known and non-`optional`
- [ ] If looping: `loopCondition` exit criterion explicit (no infinite loop)
- [ ] Sub-workflow extraction considered for reusable blocks
- [ ] Anti-patterns explicitly avoided (no polling SIMPLE, no JDBC, no
      Orkes-drag-drop forms in production)

## Cross-refs

- For task-type specifics (HUMAN, SWITCH, FORK_JOIN, DO_WHILE, etc.): use
  `orkes-tasks`.
- For workflow JSON shape (`failureWorkflow`, `variables`, `version`): use
  `orkes-workflows`.
- For Event Handlers (Pattern 0 entry + asyncComplete contract): use
  `orkes-event-handlers`.
- For the Kafka↔Orkes specifics (`_schema` etc.): use `kafka-orkes`.
- For project hard rules: [.claude/rules/orkes-standards.md](.claude/rules/orkes-standards.md).
- Authoritative source: [docs/orkes_training.md](docs/orkes_training.md)
  (Architectural Patterns 1/6 through 6/6, plus composition primitives);
  [docs/codelab.md](docs/codelab.md) (worked example combining 5 patterns).
