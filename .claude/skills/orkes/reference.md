# orkes — reference

Loaded on demand from `orkes/SKILL.md`. Holds long tables and the Orkes
glossary that don't belong inline in the master skill body.

## Glossary

| Term | Definition |
|---|---|
| **Workflow definition** | JSON document registered with Conductor; describes the task graph, inputs, outputs, lifecycle. Versioned per `name`. |
| **Workflow run / instance** | A live execution of a workflow definition. Pinned to the `version` it started under. Has a unique `workflowId`. |
| **Task** | A single step in a workflow. Multiple types — see `orkes-tasks`. |
| **Task reference name (`taskRefName`)** | Unique identifier for a task within one workflow. Used in `${ref.output.X}` references and in `complete_task` actions. |
| **Event Handler** | Listener on an external queue (Kafka, SQS, AMQP) that triggers Orkes actions when a message matches its `condition`. |
| **`asyncComplete`** | Boolean on `EVENT` / `WAIT` / `HUMAN` tasks. When `true`, the task pauses indefinitely until externally completed (typically by a `complete_task` Event Handler). |
| **`complete_task`** | Event Handler action that completes a paused (`asyncComplete: true`) task by `workflowId` + `taskRefName`. |
| **`failureWorkflow`** | Optional workflow name on the main definition; triggered automatically on uncaught failure. The saga compensation entry point. |
| **`SET_VARIABLE`** | Task type that mutates `workflow.variables.X` — used for accumulators and saga rollback context. |
| **Standardized envelope** | Chorus convention for Kafka messages: `{ metadata: { type, team, correlationId, ... }, payload: { ... } }`. See [docs/orkes_training.md](docs/orkes_training.md) and [docs/codelab.md](docs/codelab.md). |
| **`correlationId`** | UUID-shaped string that ties together every step of a long-running flow. Always propagated. |
| **Pub/Sub model** | The Chorus-recommended interaction pattern: `EVENT (asyncComplete: true)` → backend Kafka consumer → backend Kafka producer → Event Handler `complete_task`. Replaces polling `SIMPLE` workers. |
| **My Task screen** | The CHORUS UI surface that lists pending HUMAN tasks for the logged-in user. Powered by direct Orkes integration. |
| **Assignee** | The pool/group a HUMAN task is initially assigned to (`EXTERNAL_USER` or `EXTERNAL_GROUP`). |
| **Claimant** | The user who actually picks up a HUMAN task in the UI. May differ from the assignee (assign → claim → release lifecycle). |

## Repo file map

| File | Purpose |
|---|---|
| [src/orkes/orkes.module.ts](src/orkes/orkes.module.ts) | NestJS module; env-gated `OrkesClients` provider (returns `null` when disabled or misconfigured). |
| [src/orkes/orkes.tokens.ts](src/orkes/orkes.tokens.ts) | `ORKES_CLIENTS` Symbol DI token. |
| [src/orkes/orkes-bootstrap.service.ts](src/orkes/orkes-bootstrap.service.ts) | `OnApplicationBootstrap`; walks `orkes/workflows/*.json` and `orkes/event_handlers/*.json` and registers when `ORKES_AUTO_REGISTER=true`. |
| [src/orkes/orkes.controller.ts](src/orkes/orkes.controller.ts) | `POST /orkes/test-workflow`, `GET /orkes/workflow/:id`. Returns 503 when Orkes is disabled. |
| [orkes/workflows/kafka_demo_workflow.json](orkes/workflows/kafka_demo_workflow.json) | Demo: INLINE → EVENT publish (Kafka). |
| [orkes/workflows/kafka_demo_consumer_workflow.json](orkes/workflows/kafka_demo_consumer_workflow.json) | Demo: started by `order_placed_handler` after the message round-trips. |
| [orkes/event_handlers/order_placed_handler.json](orkes/event_handlers/order_placed_handler.json) | Listens on `kafka:<topic>:orkes-demo-handler`, action `start_workflow`. |

## Environment variables (full reference)

| Var | Default | Notes |
|---|---|---|
| `ORKES_ENABLED` | `false` | Master toggle. When `false`, `ORKES_CLIENTS` is `null` and the controller returns 503. |
| `ORKES_SERVER_URL` | (unset) | e.g. `https://developer.orkescloud.com/api` (cloud trial) or `https://one-dev.orkesconductor.io/` (team training env). |
| `ORKES_KEY` | (unset) | Application key id from the Orkes UI. |
| `ORKES_SECRET` | (unset) | Application key secret. |
| `ORKES_AUTO_REGISTER` | `false` | When `true`, walk `orkes/workflows/` and `orkes/event_handlers/` at boot and register everything. |

If any of `ORKES_SERVER_URL` / `ORKES_KEY` / `ORKES_SECRET` are unset while
`ORKES_ENABLED=true`, the module logs a warning and `ORKES_CLIENTS` resolves
to `null` — the app boots but Orkes endpoints respond 503.

## DI tokens

| Token | Type | Where |
|---|---|---|
| `ORKES_CLIENTS` | `Symbol` (`OrkesClients` from `@io-orkes/conductor-javascript`, or `null`) | [src/orkes/orkes.tokens.ts](src/orkes/orkes.tokens.ts) |

`OrkesClients` exposes:
- `getMetadataClient()` — workflow / task / event-handler definitions (used by `OrkesBootstrapService`).
- `getWorkflowClient()` — start / inspect / terminate workflow runs (used by `OrkesController`).
- `getEventClient()` — Event Handler CRUD (used by `OrkesBootstrapService`).
- `getTaskClient()` — task polling / updating (not used in the pub/sub model — included for reference).

## The Standardized Kafka Event Envelope (full schema)

Chorus convention for **all** Kafka messages between CHORUS backend and
Orkes. Source: [docs/orkes_training.md](docs/orkes_training.md) "The
Standardized Kafka Event Envelope" + [docs/codelab.md](docs/codelab.md)
"Message Envelope".

```json
{
  "metadata": {
    "type":              "{team}.vendor.registration.submitted",
    "team":              "{team}",
    "correlationId":     "abc-123",
    "timestamp":         "2026-03-07T12:00:00Z",
    "messageId":         "uuid-for-dedup",
    "eventType":         "...",
    "sourceSystem":      "...",
    "destinationSystem": "...",
    "requester":         "..."
  },
  "payload": {
    "vendorId":    "VND-123",
    "companyName": "Acme Shipping",
    "workflowId":  "<orkes workflow run id, when CHORUS responds>",
    "taskRefName": "<paired task ref, when responding to asyncComplete>"
  }
}
```

| Field | Required | Purpose |
|---|---|---|
| `metadata.type` | yes | Team-prefixed event type. Used in Event Handler `condition`. |
| `metadata.team` | yes | Team name (informational; routing uses `type` prefix). |
| `metadata.correlationId` | yes | End-to-end trace key. Propagate through every step. |
| `metadata.timestamp` | yes | ISO 8601 UTC. |
| `metadata.messageId` | recommended | UUID for deduplication / auditing. |
| `metadata.eventType` | optional | Coarse classification (e.g. `command`, `event`, `response`). |
| `metadata.sourceSystem` / `destinationSystem` | optional | For routing across CHORUS and external systems. |
| `metadata.requester` | optional | The user/system that initiated the original request. |
| `payload.workflowId` | conditional | **Required** on every CHORUS→external message; needed by Event Handlers to call `complete_task`. |
| `payload.taskRefName` | conditional | **Required** when responding to an `asyncComplete: true` task. |

**Inside an Event Handler, `$` is the entire envelope** — see
`orkes-event-handlers` for `${...}` reference syntax.

## Team-isolation conventions (codelab)

| Resource | Pattern | Example |
|---|---|---|
| Workflow name | `exp_{team}_{purpose}` | `exp_spm_vendor_onboarding` |
| Event handler name (trigger) | `exp_{team}_{flow}_trigger` | `exp_spm_vendor_onboarding_trigger` |
| Event handler name (completion) | `exp_{team}_{flow}_{event}_handler` | `exp_spm_vendor_doc_verified_handler` |
| Compensation workflow | `{name}_compensation` | `exp_spm_vendor_onboarding_compensation` |
| Kafka message type | `{team}.{domain}.{event}` | `spm.vendor.registration.submitted` |

**Why team-prefix the message type?** All teams share the same Kafka topics.
Embedding the team name in `metadata.type` means `condition`s naturally
isolate each team's traffic — no extra `metadata.team` filter needed.

## The Four Core Architectural Principles (Chorus standard)

Source: [docs/orkes_training.md](docs/orkes_training.md). Non-negotiable
framing for every Orkes change.

1. **Orchestration Layer Only.** Traceability for business processes — not
   a no-code/low-code replacement for backend systems. Business logic stays
   in NestJS workers.
2. **Event-Driven & Asynchronous.** Communication relies on Kafka. Expect
   delays; operations are fundamentally asynchronous.
3. **Strict Database Isolation.** Orkes has **zero** direct DB access. The
   `BUSINESS_RULE` task can load a rule file but must not query a DB. The
   `JDBC` task is on the AVOID list.
4. **Backend Operation Guardrails.** If using Orkes for backend operations,
   acknowledge the asynchronous nature; evaluate case-by-case.

## When NOT to orchestrate (Signal Exclusion Matrix)

| Don't orchestrate | Use instead |
|---|---|
| Simple request-response | Direct function calls |
| High-frequency events (>10K/sec) | Kafka Streams / Flink |
| Pure data transformations | ETL tools |
| Stateless notifications | Message queue + consumer |
| Basic CRUD | Standard APIs |

**Orchestrate only when the process exhibits 3+ of**: multi-service
coordination, human approval/intervention, minutes-to-days duration,
compensation needs, regulatory/audit decision trail, business-rule routing,
SLA enforcement, sub-process reuse.

## Useful commands

| What | Command |
|---|---|
| Trigger demo workflow | `curl -X POST http://localhost:3000/orkes/test-workflow -H 'Content-Type: application/json' -d '{"userId":"u-demo","amountCents":12345,"currency":"USD","sku":"SKU-DEMO"}'` |
| Inspect a run | `curl http://localhost:3000/orkes/workflow/<workflowId>` |
| Start a workflow via Orkes API directly | `curl -X POST "$ORKES_SERVER_URL/workflow/<name>?version=<n>" -H 'Content-Type: application/json' -H "x-authorization: <TOKEN>" -d '{...}'` |
| Get a workflow status (Orkes API) | `curl "$ORKES_SERVER_URL/workflow/<workflowId>?includeTasks=true" -H "x-authorization: <TOKEN>"` |
| Pause an Event Handler (Orkes API) | `curl -X PUT "$ORKES_SERVER_URL/event/<name>/active" -H "x-authorization: <TOKEN>" -d 'false'` |
| Cron expression for nightly midnight (Scheduler) | `0 0 * ? * *` |
