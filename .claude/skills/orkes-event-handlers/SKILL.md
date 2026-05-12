---
name: orkes-event-handlers
description: Authoring and debugging Orkes Event Handlers in this repo — JSON shape (name, event source, condition, actions), the four action types (start_workflow / complete_task / update_task / terminate_workflow), the asyncComplete request/response contract, the Standardized Kafka Event Envelope, team-isolation conventions, and the OrkesBootstrapService upsert lifecycle. Use when authoring an Event Handler, debugging "task stuck IN_PROGRESS", "handler never fires", or wiring the request/response chain between a workflow and the NestJS backend.
---

# Orkes Event Handlers

How to author Event Handlers that listen to external queues (Kafka, SQS,
etc.) and trigger Orkes actions. Covers the **asyncComplete request/response
contract** that pairs every long-running EVENT task with a matching
`complete_task` handler. For the Kafka-specific Avro `_schema` mechanic on
the publisher side, use `kafka-orkes`.

## When to use

User is:
- Creating a new Event Handler JSON (under `orkes/event_handlers/`).
- Wiring an `EVENT` task that pauses (`asyncComplete: true`) — needs a
  matching handler.
- Debugging "task stuck `IN_PROGRESS` forever" or "handler never fires" or
  "handler fires but `${field}` is empty".
- Reviewing a handler PR for naming, condition correctness, action shape.

If the task is purely about **publishing** to Kafka (the EVENT task itself),
use `kafka-orkes`. If you're picking the right action type to use, see the
"Action types" section below.

## Event Handler JSON anatomy

```json
{
  "name":      "exp_{team}_vendor_doc_verified_handler",
  "event":     "kafka:one-om-dev-training:one-om-dev-training-message-out-private",
  "condition": "$.metadata.type == '{team}.vendor.document.verified'",
  "evaluatorType": "javascript",
  "active":    true,
  "actions": [
    {
      "action": "complete_task",
      "complete_task": {
        "workflowId":  "${payload.workflowId}",
        "taskRefName": "${payload.taskRefName}",
        "output": {
          "vendorId":  "${payload.vendorId}",
          "status":    "${payload.status}",
          "details":   "${payload.details}"
        }
      }
    }
  ]
}
```

### Required fields

| Field | Purpose |
|---|---|
| `name` | Unique identifier in the environment. Team-prefixed snake_case. |
| `event` | Source URI: `kafka:<integration>:<topic>` for Kafka. |
| `condition` | JS expression filtering messages. `$` is the entire message envelope. |
| `actions` | Array of actions to run when the condition matches. |
| `active` | `true` to enable; `false` to pause without deleting. |

### Recommended fields

| Field | Purpose |
|---|---|
| `evaluatorType` | `"javascript"` for `condition`. Default behavior is JS. |
| `description` | Human summary; appears in the Orkes UI. |

## Event source format

| Source | `event` value | Example |
|---|---|---|
| Kafka | `kafka:<integration>:<topic>` | `kafka:one-om-dev-training:one-om-dev-training-message-out-private` |
| AMQP | `amqp:<connection>:<queue>` | `amqp:rabbit-prod:vendor.events` |
| SQS | `sqs:<queue-name>` | `sqs:vendor-events-prod` |

The `<integration>` is the name configured in the Orkes UI's Integration
panel. Per [docs/codelab.md](docs/codelab.md), the team training environment
uses `one-om-dev-training`.

## Naming conventions (codelab)

| Resource | Pattern | Example |
|---|---|---|
| Trigger handler | `exp_{team}_{flow}_trigger` | `exp_spm_vendor_onboarding_trigger` |
| Completion handler | `exp_{team}_{flow}_{event}_handler` | `exp_spm_vendor_doc_verified_handler` |
| Generic handler | `exp_{team}_{purpose}_handler` | `exp_spm_compliance_alert_handler` |

## The Standardized Kafka Event Envelope

All Chorus messages follow this envelope (per [docs/orkes_training.md](docs/orkes_training.md)
and [docs/codelab.md](docs/codelab.md)):

```json
{
  "metadata": {
    "type":          "{team}.vendor.document.verified",
    "team":          "{team}",
    "correlationId": "abc-123",
    "timestamp":     "2026-03-07T12:00:00Z",
    "messageId":     "uuid",
    "eventType":     "...",
    "sourceSystem":  "...",
    "destinationSystem": "...",
    "requester":     "..."
  },
  "payload": {
    "workflowId":  "<orkes workflow run id>",
    "taskRefName": "<paired task reference name>",
    "vendorId":    "VND-123",
    "status":      "approved"
  }
}
```

Inside an Event Handler, `$` is the **entire envelope**:

| Expression | Resolves to |
|---|---|
| `$.metadata.type` | The team-prefixed event type (used in `condition`) |
| `${metadata.correlationId}` | End-to-end trace key, propagate to the workflow |
| `${payload}` | Whole business-data object |
| `${payload.workflowId}` | Pinned workflow run id (asyncComplete pairing) |
| `${payload.taskRefName}` | Pinned task ref (asyncComplete pairing) |

## Condition expressions

`condition` is a JavaScript expression evaluated against each incoming
message. Common patterns:

```js
// Single-type filter (most common)
$.metadata.type == 'spm.vendor.registration.submitted'

// Multi-type filter
$.metadata.type == 'spm.vendor.document.verified' ||
$.metadata.type == 'spm.vendor.document.rejected'

// Filter by payload field
$.metadata.type == 'spm.vendor.document.verified' && $.payload.status == 'approved'

// Always fire (rare; only when the topic is single-purpose)
true
```

> **Why team-prefix the type?** Per the codelab: all teams share the same
> Kafka topics. Embedding the team in the type
> (`spm.vendor.registration.submitted`) means the condition trivially
> isolates each team's traffic — no extra `metadata.team` filter needed.

## Action types

### 1. `start_workflow` — trigger handlers

Starts a new workflow instance when the condition matches.

```json
{
  "action": "start_workflow",
  "start_workflow": {
    "name":          "exp_{team}_vendor_onboarding",
    "version":       1,
    "correlationId": "${metadata.correlationId}",
    "input": {
      "registration":  "${payload}",
      "correlationId": "${metadata.correlationId}",
      "submittedAt":   "${metadata.timestamp}"
    }
  }
}
```

| Field | Purpose |
|---|---|
| `name` | Workflow definition to start |
| `version` | Pinned version (omit to use latest — risky for production) |
| `correlationId` | Links the workflow to the original request |
| `input` | Becomes `${workflow.input.X}` inside the workflow |

### 2. `complete_task` — completion handlers (asyncComplete pairing)

Completes a **waiting task** (one with `asyncComplete: true`) inside an
already-running workflow.

```json
{
  "action": "complete_task",
  "complete_task": {
    "workflowId":  "${payload.workflowId}",
    "taskRefName": "${payload.taskRefName}",
    "output": {
      "vendorId":  "${payload.vendorId}",
      "status":    "${payload.status}",
      "details":   "${payload.details}"
    }
  }
}
```

| Field | Purpose |
|---|---|
| `workflowId` | Identifies **which workflow instance** holds the waiting task |
| `taskRefName` | Identifies **which task** in that workflow to complete |
| `output` | Becomes that task's output (available to downstream tasks) |

### 3. `update_task` — programmatic status change

Modify a running task's status without completing it (e.g. mark `IN_PROGRESS`
→ `FAILED` to trigger compensation).

```json
{
  "action": "update_task",
  "update_task": {
    "workflowId":  "${payload.workflowId}",
    "taskRefName": "${payload.taskRefName}",
    "status":      "FAILED",
    "output":      { "reason": "${payload.reason}" }
  }
}
```

### 4. `terminate_workflow` — emergency stop

Forcibly terminate a workflow run. Use sparingly — typically wired to an
"emergency cancel" Kafka event.

```json
{
  "action": "terminate_workflow",
  "terminate_workflow": {
    "workflowId": "${payload.workflowId}",
    "reason":     "Cancelled by ${metadata.requester}: ${payload.reason}"
  }
}
```

## The asyncComplete contract — the most important pattern

This is the **request/response chain** that lets Orkes wait for the NestJS
backend without polling. Per the training material's "Pub/Sub over polling"
rule.

```
[Orkes workflow]                          [NestJS backend]
                                                   │
  publish_doc_verify (EVENT, asyncComplete: true)  │
        │                                          │
        ├──── Kafka OUT topic ─────────────────────▶  consumer picks up
        │     metadata.type=                       │  request, runs work
        │       'spm.vendor.document.verify'       │
        │     payload.workflowId    = ${workflow.workflowId}
        │     payload.taskRefName   = 'verify_doc' │
        │     payload.vendorId      = ...          │
        │                                          │
   (task pauses in IN_PROGRESS)                    │
                                                   │
        │◀──── Kafka IN topic ──────────────────────  publishes response
        │     metadata.type=                       │  (echoes workflowId
        │       'spm.vendor.document.verified'     │   and taskRefName!)
        │     payload.workflowId    = <same>       │
        │     payload.taskRefName   = 'verify_doc' │
        │     payload.status        = 'approved'   │
        │                                          │
   exp_spm_vendor_doc_verified_handler matches     │
   condition, runs complete_task with              │
   workflowId + taskRefName from payload           │
        │                                          │
   verify_doc.output = payload.* values            │
        │                                          │
   continue to next workflow task                  │
```

### Three rules

1. **Every `EVENT` task with `asyncComplete: true` requires a matching
   handler with `complete_task`** — without it, the task stays
   `IN_PROGRESS` forever and the workflow hangs.
2. **The workflow includes `workflowId` + `taskRefName` in the request
   message** — the backend echoes them back in the response so the handler
   knows exactly which workflow + task to complete.
3. **Match by message type, not by topic** — multiple workflows publish to
   the same OUT topic; the handler's `condition` is what picks the right
   handler for each response type.

### Workflow-side EVENT task example

```json
{
  "name": "publish_doc_verify",
  "taskReferenceName": "verify_doc",
  "type": "EVENT",
  "sink": "kafka:one-om-dev-training:one-om-dev-training-message-in-private",
  "asyncComplete": true,
  "inputParameters": {
    "metadata": {
      "type":          "{team}.vendor.document.verify",
      "correlationId": "${workflow.input.correlationId}",
      "timestamp":     "${workflow.startTime}"
    },
    "payload": {
      "workflowId":  "${workflow.workflowId}",
      "taskRefName": "verify_doc",
      "vendorId":    "${normalize.output.result.vendorId}",
      "documents":   "${normalize.output.result.documents}"
    }
  }
}
```

The companion completion handler matches `'{team}.vendor.document.verified'`
and calls `complete_task` with `${payload.workflowId}` and
`${payload.taskRefName}` — exactly the values echoed back by the backend.

## Auto-registration lifecycle

`OrkesBootstrapService` in
[src/orkes/orkes-bootstrap.service.ts](src/orkes/orkes-bootstrap.service.ts)
walks `orkes/event_handlers/*.json` at boot when `ORKES_AUTO_REGISTER=true`
and **upserts** by name:

```ts
try {
  await events.getEventHandlerByName(handler.name);
  await events.updateEventHandler(handler);
} catch {
  await events.addEventHandler(handler);
}
```

Implications:
- **Renaming a handler leaves the old one orphaned** (still active under the
  old name). Delete the old one via Orkes UI or the EventClient API before
  merging the rename.
- **Edits redeploy in place.** No version field — the latest JSON wins.
- **Set `active: false` to pause** without deleting (useful during schema
  rollouts touching the handler — see `kafka-evolution`).

## Common errors and fixes

| Symptom | Cause | Fix |
|---|---|---|
| Task stuck `IN_PROGRESS` forever | EVENT task has `asyncComplete: true` but no matching handler | Add the handler with `complete_task`, or set `asyncComplete: false` |
| Handler never fires | Wrong `event` prefix, integration not configured, `active: false`, or `condition` doesn't match | Verify each part of `kafka:<integration>:<topic>`; check Orkes integration panel; check condition against an actual sample message |
| Handler fires but `${field}` resolves to empty | Field name in action mismatches actual message | Inspect the actual message in Orkes "Workflow Executions" → "Event Logs"; field names are case-sensitive |
| `complete_task` fails with "task not found" | `taskRefName` from response doesn't match a task in the running workflow | Backend must echo back the **exact** `taskRefName` from the request; don't transform it |
| `complete_task` fails with "workflow not found" | `workflowId` lost or transformed in transit | Backend must echo `workflowId` verbatim |
| Multiple handlers fire for one message | Two handlers have overlapping `condition`s | Make conditions mutually exclusive (e.g. distinct message types) |
| Renamed handler — old one still firing | `OrkesBootstrapService` upserts by name, doesn't delete old | Delete the orphan via Orkes UI before merging the rename |

## Pre-merge checklist

- [ ] `name` follows team-prefixed snake_case
- [ ] `event` uses the integration name configured in Orkes UI
- [ ] `condition` filters by team-prefixed `metadata.type`
- [ ] If `complete_task`: workflow-side EVENT task includes `workflowId` + `taskRefName` in payload, and backend echoes them back
- [ ] Each `EVENT` task with `asyncComplete: true` has a matching handler in this PR
- [ ] `correlationId` propagated from `metadata` to `start_workflow.correlationId`
- [ ] No PII in `output` map (Orkes UI shows it)
- [ ] If renaming: old handler scheduled for deletion in the same PR
- [ ] File lives under `orkes/event_handlers/` so `OrkesBootstrapService` picks it up
- [ ] Tested with a real sample message in the Orkes "Event Logs" panel

## Cross-refs

- For the workflow-side EVENT task: use `orkes-tasks` (EVENT section).
- For workflow JSON shape: use `orkes-workflows`.
- For pattern composition: use `orkes-patterns`.
- For Kafka-specific Avro `_schema` on the publish side: use `kafka-orkes`.
- For the `OrkesBootstrapService` registration mechanics: use `orkes` (master).
- Project hard rules: [.claude/rules/orkes-standards.md](.claude/rules/orkes-standards.md).
- Authoritative material: [docs/codelab.md](docs/codelab.md) (full walkthrough),
  [docs/orkes_training.md](docs/orkes_training.md) (envelope + pub/sub rule).
