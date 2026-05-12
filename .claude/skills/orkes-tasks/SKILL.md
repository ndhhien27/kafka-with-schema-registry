---
name: orkes-tasks
description: Task type catalog for Orkes Conductor in this repo — when to use SIMPLE / INLINE / EVENT / HTTP / JSON_JQ_TRANSFORM / SWITCH / DO_WHILE / FORK_JOIN / FORK_JOIN_DYNAMIC / SUB_WORKFLOW / WAIT / HUMAN / TERMINATE / SET_VARIABLE / BUSINESS_RULE / UPDATE_TASK, plus their config schemas, retry/timeout fields, and the AVOID list (JDBC, polling SIMPLE, Orkes drag-and-drop forms). Use when picking a task type, configuring task fields, or reviewing a workflow's task graph.
---

# Orkes task catalog

Complete task-type reference for this repo. Most workflows compose 4-8 task
types; the codelab vendor-onboarding example uses 7. For the surrounding
workflow JSON shape, use `orkes-workflows`. For pattern-level guidance, use
`orkes-patterns`.

## When to use

User is:
- Picking the right task type for a step.
- Configuring a specific task's fields (`retryCount`, `timeoutSeconds`,
  `evaluatorType`, `loopCondition`, `joinOn`, etc.).
- Debugging a stuck `IN_PROGRESS` task or a task that fires the wrong branch.
- Reviewing a workflow PR for task choices.

If you're choosing between Orkes and "no orchestration", use the `orkes`
master skill's "When NOT to orchestrate" matrix first.

## Quick decision matrix

| Need | Task type |
|---|---|
| Embed JS computation inline (small, no I/O) | `INLINE` |
| Reshape JSON (jq) without code | `JSON_JQ_TRANSFORM` |
| Call an external HTTPS endpoint | `HTTP` |
| Publish to Kafka and pause for response | `EVENT` (with `asyncComplete: true`) |
| Branch by data | `SWITCH` |
| Repeat a block until a condition | `DO_WHILE` |
| Run N tasks in parallel (static) | `FORK_JOIN` + `JOIN` |
| Run N tasks in parallel (count known at runtime) | `FORK_JOIN_DYNAMIC` |
| Compose another workflow | `SUB_WORKFLOW` |
| Pause for an external signal | `WAIT` |
| Pause for human approval | `HUMAN` |
| Pause for an HTTP webhook | `WAIT` (Wait For Webhook variant) |
| Mutate a workflow variable | `SET_VARIABLE` |
| End the workflow with status | `TERMINATE` |
| Evaluate a tabular rules sheet | `BUSINESS_RULE` |
| Update another task's status programmatically | `UPDATE_TASK` |
| Long-running worker polling Orkes for work | `SIMPLE` ⚠️ avoid in Chorus |
| Direct DB query from Orkes | `JDBC` ❌ prohibited |

## AVOID list (Chorus standard)

| Task type | Why prohibited / discouraged |
|---|---|
| `JDBC` | Strict DB isolation rule — all DB access goes through backend services. |
| `SIMPLE` (polling worker) | Constant network overhead; misaligns with event-driven arch. Use the EVENT/Wait/Event-Handler chain instead. |
| Orkes drag-and-drop User Forms | Strictly prohibited in production. Build forms in CHORUS Design System. |

## Common task fields (every task type)

| Field | Purpose |
|---|---|
| `name` | Task definition name (matches a registered task or builtin). |
| `taskReferenceName` | Unique within this workflow; used in `${ref.output.X}` references. |
| `type` | Task type literal. |
| `inputParameters` | Map of input bindings; can interpolate `${workflow.input.X}` and `${otherTask.output.X}`. |
| `optional` | If `true`, task failure doesn't fail the workflow. |
| `retryCount` | Per-task retry attempts on failure. |
| `retryLogic` | `FIXED` / `EXPONENTIAL_BACKOFF`. |
| `retryDelaySeconds` | Initial delay; doubled per retry under `EXPONENTIAL_BACKOFF`. |
| `timeoutSeconds` | Hard timeout for the task. |
| `responseTimeoutSeconds` | Max time the task can be `IN_PROGRESS` before being marked timed out. |

For long-running tasks (anything > 30s), set both `timeoutSeconds` and
`responseTimeoutSeconds` explicitly — defaults are aggressive.

## Task types — catalog

### `INLINE` (JavaScript)

Embed small business logic without a worker. The expression returns a value
that becomes `${this.output.result}`.

```json
{
  "name": "calculate_risk_score",
  "taskReferenceName": "calculate_risk_score",
  "type": "INLINE",
  "inputParameters": {
    "evaluatorType": "javascript",
    "documents":  "${verify_documents.output.documents}",
    "sanctions":  "${parallel_compliance_checks.output.sanctions}",
    "expression": "function score() { var s = 0; if ($.sanctions.hit) s += 50; s += (10 - $.documents.length) * 5; return { score: Math.max(0, Math.min(100, s)) }; } score();"
  }
}
```

Rules:
- Keep expressions **short** — multi-page JS belongs in a backend service via EVENT/HTTP.
- `$.X` references the task's `inputParameters` (not `workflow.input`).
- Output is whatever the expression returns, available as `${this.taskRef.output.result}`.

### `JSON_JQ_TRANSFORM`

Reshape JSON without writing JS. Use for normalization/projection.

```json
{
  "name": "normalize_registration",
  "taskReferenceName": "normalize_registration",
  "type": "JSON_JQ_TRANSFORM",
  "inputParameters": {
    "registration":  "${workflow.input.registration}",
    "queryExpression": ".registration | { vendorId: .id, companyName: .name, documents: (.docs // []) }"
  }
}
```

Output is at `${this.output.result}`.

### `HTTP`

Call an external HTTPS endpoint synchronously. **Prefer `EVENT` for backend
calls** (per Chorus pub/sub rule); use `HTTP` only for true third parties.

```json
{
  "name": "geocode_address",
  "taskReferenceName": "geocode_address",
  "type": "HTTP",
  "inputParameters": {
    "http_request": {
      "uri":     "https://geocoder.example.com/v1/lookup",
      "method":  "POST",
      "headers": { "Authorization": "Bearer ${workflow.input.geocoderToken}" },
      "body":    { "address": "${normalize.output.result.address}" },
      "connectionTimeOut": 5000,
      "readTimeOut":       10000
    }
  },
  "retryCount": 2,
  "retryLogic": "EXPONENTIAL_BACKOFF"
}
```

### `EVENT` (Kafka publish)

Publish to a Kafka topic via the Apache Kafka Integration. The bread-and-butter
of Chorus's pub/sub model.

```json
{
  "name": "publish_workflow_started",
  "taskReferenceName": "publish_workflow_started",
  "type": "EVENT",
  "sink": "kafka:one-om-dev-training:one-om-dev-training-message-in-private",
  "asyncComplete": false,
  "inputParameters": {
    "metadata": {
      "type":          "{team}.vendor.workflow.started",
      "team":          "{team}",
      "correlationId": "${workflow.input.correlationId}",
      "timestamp":     "${workflow.startTime}"
    },
    "payload": {
      "workflowId":   "${workflow.workflowId}",
      "vendorId":     "${normalize.output.result.vendorId}"
    }
  }
}
```

Two `asyncComplete` modes:

| `asyncComplete` | Behavior |
|---|---|
| `false` (fire-and-forget) | Task completes immediately after publish. Use for notifications. |
| `true` (request/response) | Task pauses in `IN_PROGRESS`. **Requires** a matching Event Handler that runs `complete_task` on the response. See `orkes-event-handlers` (asyncComplete contract). |

For BTH/Chorus Avro topics with the `_schema` subject parameter, see
`kafka-orkes` instead.

### `SWITCH`

Branch by data. Always define `defaultCase`.

```json
{
  "name": "route_by_risk",
  "taskReferenceName": "route_by_risk",
  "type": "SWITCH",
  "evaluatorType": "javascript",
  "expression": "function r() { var s = $.score.score; if (s <= 30) return 'auto_approve'; if (s <= 60) return 'compliance_review'; return 'manager_review'; } r();",
  "inputParameters": { "score": "${calculate_risk_score.output.result}" },
  "decisionCases": {
    "auto_approve":       [ /* tasks ... */ ],
    "compliance_review":  [ /* tasks ... */ ],
    "manager_review":     [ /* tasks ... */ ]
  },
  "defaultCase": [ /* tasks for unmatched ... */ ]
}
```

Alternative: `evaluatorType: "value-param"` with `expression: "fieldName"` —
matches against a single key in `inputParameters`.

### `DO_WHILE`

Loop a block until `loopCondition` evaluates falsy.

```json
{
  "name": "verify_documents_loop",
  "taskReferenceName": "verify_documents_loop",
  "type": "DO_WHILE",
  "inputParameters": {
    "documents": "${normalize.output.result.documents}"
  },
  "loopCondition": "if ($.verify_documents_loop['iteration'] < $.documents.length) { true; } else { false; }",
  "loopOver": [ /* tasks executed each iteration ... */ ]
}
```

Rules:
- `loopOver` body can contain any task types (including FORK_JOIN, SWITCH).
- The current iteration index is available as `$.[taskRefName].iteration`.
- Each iteration's task outputs are accessible as
  `${[bodyTaskRef].output.X[iterationIndex]}`.

### `FORK_JOIN` + `JOIN`

Static parallel fan-out — number of branches known at design time.

```json
{
  "name": "parallel_compliance_checks",
  "taskReferenceName": "parallel_compliance_checks",
  "type": "FORK_JOIN",
  "forkTasks": [
    [ /* branch 1: sanctions check tasks ... */ ],
    [ /* branch 2: insurance validation tasks ... */ ],
    [ /* branch 3: license verification tasks ... */ ]
  ]
},
{
  "name": "join_compliance_checks",
  "taskReferenceName": "join_compliance_checks",
  "type": "JOIN",
  "joinOn": ["sanctions_check", "insurance_validation", "license_verification"]
}
```

`joinOn` lists the **last task ref name in each branch**. The `JOIN`
completes when all listed tasks complete (success or failure).

### `FORK_JOIN_DYNAMIC`

Same idea but the branch count is determined at runtime (e.g. one branch per
document).

```json
{
  "name": "verify_documents_dynamic",
  "taskReferenceName": "verify_documents_dynamic",
  "type": "FORK_JOIN_DYNAMIC",
  "inputParameters": {
    "documents": "${normalize.output.result.documents}",
    "dynamicTasks":      "${prepare_doc_tasks.output.tasks}",
    "dynamicTasksInput": "${prepare_doc_tasks.output.inputs}"
  },
  "dynamicForkTasksParam":      "dynamicTasks",
  "dynamicForkTasksInputParamName": "dynamicTasksInput"
},
{
  "name": "join_doc_verification",
  "taskReferenceName": "join_doc_verification",
  "type": "JOIN"
}
```

You typically prepare `dynamicTasks` (array of task definitions) and
`dynamicTasksInput` (map of inputs) in an upstream `INLINE` or
`JSON_JQ_TRANSFORM` task.

### `SUB_WORKFLOW`

Compose another registered workflow as a step.

```json
{
  "name": "run_compliance_screening",
  "taskReferenceName": "run_compliance_screening",
  "type": "SUB_WORKFLOW",
  "subWorkflowParam": {
    "name":    "exp_{team}_compliance_screening",
    "version": 1
  },
  "inputParameters": {
    "vendorId":      "${normalize.output.result.vendorId}",
    "correlationId": "${workflow.input.correlationId}"
  }
}
```

Sub-workflow's `outputParameters` becomes
`${run_compliance_screening.output.X}`.

### `WAIT`

Pause until manually completed via the Orkes API, until a duration elapses,
or until a webhook fires.

```json
{
  "name": "wait_for_callback",
  "taskReferenceName": "wait_for_callback",
  "type": "WAIT",
  "inputParameters": { "duration": "30m" }
}
```

For HTTP webhooks: use the `Wait For Webhook` variant — pair with a globally
defined Webhook in Orkes settings.

### `HUMAN`

Pause for human approval. Renders in the "My Task" UI to the assignee.

```json
{
  "name": "ApproveRegistration",
  "taskReferenceName": "ApproveRegistration",
  "type": "HUMAN",
  "inputParameters": {
    "vendorId":     "${normalize.output.result.vendorId}",
    "riskScore":    "${calculate_risk_score.output.result.score}",
    "displayName":  "Approve vendor ${normalize.output.result.companyName}"
  },
  "humanTaskDefinition": {
    "assignmentCompletionStrategy": "TERMINATE_ON_COMPLETE",
    "userFormTemplate":  null,
    "assignments": [
      { "assignee": "BPM Group - GHQ", "assigneeType": "EXTERNAL_GROUP", "slaMinutes": 240 },
      { "assignee": "safety-officer@one-line.com", "assigneeType": "EXTERNAL_USER", "slaMinutes": 60 }
    ]
  },
  "timeoutSeconds": 86400
}
```

Rules (per training):
- **Pascal Case `taskReferenceName`** — renders as the task name in the UI.
- **Always set `timeoutSeconds`** as a hard backstop **beyond** the SLA.
- **Use SLA-based escalation** by listing assignments in priority order with
  ascending `slaMinutes`.
- **Don't use Orkes drag-and-drop forms in production** — the `userFormTemplate`
  field stays `null`; the form lives in the CHORUS product UI.
- **Async completion**: complete the task programmatically via the Orkes API
  when the user submits in the CHORUS UI. Don't poll.

Assignee semantics:
- **Assignee**: the pool/group the task is initially assigned to.
- **Claimant**: the actual user who claims the task in the UI; may differ
  from assignee.

### `TERMINATE`

End the workflow with a final status, optionally setting `outputParameters`.

```json
{
  "name": "complete_workflow",
  "taskReferenceName": "complete_workflow",
  "type": "TERMINATE",
  "inputParameters": {
    "terminationStatus": "COMPLETED",
    "workflowOutput": {
      "vendorId":  "${normalize.output.result.vendorId}",
      "decision":  "${ApproveRegistration.output.decision}"
    }
  }
}
```

`terminationStatus`: `COMPLETED` / `FAILED` / `TERMINATED`. Setting
`FAILED` triggers the `failureWorkflow` if defined.

### `SET_VARIABLE`

Mutate a workflow variable. Used for saga rollback context, accumulators.

```json
{
  "name": "record_reservation",
  "taskReferenceName": "record_reservation",
  "type": "SET_VARIABLE",
  "inputParameters": {
    "vesselSlotReservationId": "${reserve_vessel_slot.output.reservationId}"
  }
}
```

Read with `${workflow.variables.vesselSlotReservationId}` in any downstream
task or in the `failureWorkflow`.

### `BUSINESS_RULE`

Evaluate a tabular rules sheet (CSV/XLS/XLSX). Use for clean condensation of
nested if-else logic.

```json
{
  "name": "tariff_classification",
  "taskReferenceName": "tariff_classification",
  "type": "BUSINESS_RULE",
  "inputParameters": {
    "ruleFileName": "tariff_rules.xlsx",
    "input":        "${shipment_details.output}"
  }
}
```

Note: Chorus rule "Strict Database Isolation" still applies — the rule file
itself can be loaded, but the `BUSINESS_RULE` task should not query a DB.

### `UPDATE_TASK`

Modify the status/output of another running task without restarting the
workflow. Use for completing long-running tasks programmatically or breaking
out of a stuck loop.

```json
{
  "name": "force_complete_review",
  "taskReferenceName": "force_complete_review",
  "type": "UPDATE_TASK",
  "inputParameters": {
    "workflowId":  "${workflow.workflowId}",
    "taskRefName": "ReviewDocuments",
    "status":      "COMPLETED",
    "output":      { "decision": "auto_complete_after_grace" }
  }
}
```

## Retry/timeout matrix

| Use case | `retryCount` | `retryLogic` | `timeoutSeconds` | `responseTimeoutSeconds` |
|---|---|---|---|---|
| HTTP call to flaky third party | 2-3 | `EXPONENTIAL_BACKOFF` | 30 | 60 |
| EVENT publish (no async) | 0 | — | 10 | 30 |
| EVENT publish (asyncComplete) | 0 | — | 600 (or however long the backend can take) | depends on backend |
| HUMAN task | 0 | — | `slaMinutes * 60 * 2` (2× SLA backstop) | same |
| INLINE / JSON_JQ_TRANSFORM | 0 | — | 5 | 10 |

## Common errors and fixes

| Symptom | Cause | Fix |
|---|---|---|
| Task stuck `IN_PROGRESS` forever | `EVENT` with `asyncComplete: true` and no matching Event Handler | Add the Event Handler with `complete_task` action, OR set `asyncComplete: false` |
| `JOIN` never completes | A `forkTasks` branch's last task is `optional: true` and silently skipped | List the actually-running last task in `joinOn` |
| `SWITCH` always falls to `defaultCase` | `evaluatorType: "javascript"` returns wrong type | Ensure the JS function explicitly returns a string matching a `decisionCases` key |
| `DO_WHILE` infinite loop | `loopCondition` references a missing field; defaults to truthy | Use `if ($.X) { true; } else { false; }` to make falsy explicit |
| `INLINE` task fails with "Cannot read properties" | Forgot to bind input via `inputParameters` | All `$.X` references must have a matching key in `inputParameters` |
| `HUMAN` task expires before SLA | `timeoutSeconds` lower than SLA in minutes × 60 | Set `timeoutSeconds` ≥ 2× total SLA |
| `SUB_WORKFLOW` runs old task graph | Sub-workflow has v2 but parent pinned `version: 1` | Update parent's `subWorkflowParam.version`, or omit version |

## Pre-merge checklist

- [ ] Each task has a unique `taskReferenceName`
- [ ] All `${X.output.Y}` references point to real upstream task outputs
- [ ] `EVENT` tasks: `asyncComplete: true` paired with a matching Event Handler
- [ ] `SWITCH` always has `defaultCase`
- [ ] `DO_WHILE` `loopCondition` explicitly returns boolean
- [ ] `JOIN`'s `joinOn` lists the actual last task in each branch
- [ ] `HUMAN` tasks: Pascal Case names, SLA + timeout both set, no `userFormTemplate`
- [ ] `BUSINESS_RULE` / `JDBC` / `SIMPLE` chosen only with explicit justification
- [ ] Long-running tasks have `timeoutSeconds` AND `responseTimeoutSeconds`
- [ ] No PII in `inputParameters`

## Cross-refs

- For workflow JSON shape (the surrounding container): use `orkes-workflows`.
- For Event Handlers (asyncComplete contract, `complete_task`): use
  `orkes-event-handlers`.
- For pattern composition (saga, fork-join, human-in-loop): use
  `orkes-patterns`.
- For Kafka `EVENT` task with Avro `_schema`: use `kafka-orkes`.
- Project hard rules: [.claude/rules/orkes-standards.md](.claude/rules/orkes-standards.md).
- Authoritative material: [docs/orkes_training.md](docs/orkes_training.md),
  [docs/codelab.md](docs/codelab.md).
