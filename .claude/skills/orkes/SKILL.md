---
name: orkes
description: Master/router skill for Orkes Conductor work in this repo — when to orchestrate, repo wiring (OrkesClients, OrkesBootstrapService, OrkesController, env vars), the Chorus four core architectural principles, team-isolation naming conventions, and routing to focused sub-skills. Activate any time the user mentions Orkes, Conductor, workflows, event handlers, HUMAN tasks, EVENT/INLINE/FORK_JOIN/DO_WHILE/SWITCH/SUB_WORKFLOW tasks, vendor onboarding patterns, or compensation/saga workflows — even if they don't say "skill".
---

# Orkes Conductor master skill

Entry point for **any** Orkes Conductor work in this repo. Routes to the
right sub-skill, surfaces the Chorus architectural principles, and lists the
repo wiring. For Kafka-specific Orkes integration, see the sibling
`kafka-orkes` skill (kept under that name for backwards compatibility).

## When to use

Activate any time the user mentions:

- Orkes / Conductor / workflow orchestration
- A workflow definition, task, or event handler
- Task types: `SIMPLE`, `INLINE`, `EVENT`, `HTTP`, `JSON_JQ_TRANSFORM`,
  `SWITCH`, `DO_WHILE`, `FORK_JOIN`, `FORK_JOIN_DYNAMIC`, `SUB_WORKFLOW`,
  `WAIT`, `HUMAN`, `TERMINATE`, `SET_VARIABLE`, `BUSINESS_RULE`
- Patterns: sequential pipeline, decision-driven, human-in-the-loop, saga,
  scatter-gather, iterative loop
- "Vendor onboarding", "BL amendment", "compliance screening", or any
  multi-step orchestrated process from the codelab
- This repo's `OrkesBootstrapService`, `OrkesController`, or files under
  `orkes/workflows/` and `orkes/event_handlers/`

## When NOT to orchestrate (per training material)

Per [docs/orkes_training.md](docs/orkes_training.md) "Signal Exclusion
Matrix": **don't** use Orkes for

| Don't orchestrate | Use instead |
|---|---|
| Simple request-response | Direct function calls |
| High-frequency events (>10K/sec) | Kafka Streams / Flink |
| Pure data transformations | ETL tools |
| Stateless notifications | Message queue + consumer |
| Basic CRUD | Standard APIs |

**Orchestrate only** when the process exhibits 3+ of: multi-service
coordination, human approval/intervention, minutes-to-days duration,
compensation needs, regulatory/audit decision trail, business-rule routing,
SLA enforcement, or sub-process reuse.

## Decision tree — pick the sub-skill

| User intent | Sub-skill |
|---|---|
| Designing a workflow JSON (name/version/inputs/outputs/lifecycle) | `orkes-workflows` |
| Picking the right task type or configuring a specific task | `orkes-tasks` |
| Authoring or debugging an Event Handler (Kafka or otherwise) | `orkes-event-handlers` |
| Choosing or composing an architectural pattern (saga, fork-join, human-in-loop, etc.) | `orkes-patterns` |
| Wiring Kafka↔Orkes specifically (`_schema`, EVENT-task with Avro, BTH naming) | `kafka-orkes` |
| Authoring an Avro schema referenced by an Orkes EVENT task | `kafka-conventions` |
| Schema evolution that touches an Orkes-consumed `.avsc` (effective NONE) | `kafka-evolution` |

If the work spans two sub-skills, invoke each in turn rather than trying to
cover everything from this master skill.

## The Four Core Architectural Principles (Chorus standard)

From [docs/orkes_training.md](docs/orkes_training.md). These are the
non-negotiable framing for every Orkes change:

1. **Orchestration Layer Only.** Orkes provides traceability for business
   processes. It is **not** a no-code/low-code replacement for backend
   systems. Business logic stays in NestJS workers.
2. **Event-Driven & Asynchronous.** Communication relies on Kafka. Expect
   delays; operations are fundamentally asynchronous.
3. **Strict Database Isolation.** Orkes has **zero** direct DB access. All
   DB writes/reads go through backend services. The `BUSINESS_RULE` and
   `JDBC` task types are explicitly out of scope (`JDBC` is on the AVOID
   list).
4. **Backend Operation Guardrails.** If using Orkes for backend operations,
   acknowledge the asynchronous nature and evaluate case-by-case.

## Pub/Sub over polling (load-bearing rule)

**Avoid `SIMPLE` tasks with job-worker polling.** Per the training material,
polling creates constant network overhead and misaligns with the
event-driven architecture.

**Recommended chain** for any backend interaction:

```
[Orkes workflow]            [NestJS backend]
     │                            │
     ├── EVENT task ─────────────▶│  Kafka consumer picks up
     │   (publish to OUT topic)   │  request, does work
     │                            │
     ├── (asyncComplete: true,    │
     │    workflow pauses)        │
     │                            │
     │◀────────────── Event Handler│  publishes response to IN
     │  (complete_task)            │  topic; handler routes back
     │                            │
     ▼                            ▼
   continue                     done
```

The `EVENT` task publishes to an OUT topic, sets `asyncComplete: true`, and
pauses. The backend processes and publishes to an IN topic. An Event Handler
catches the response and runs `complete_task` to resume the workflow.

This pattern is documented in detail under `orkes-event-handlers`
(asyncComplete contract).

## Repo anchors (this codebase)

| Concern | File |
|---|---|
| Orkes module wiring + env-gated client construction | [src/orkes/orkes.module.ts](src/orkes/orkes.module.ts) |
| `ORKES_CLIENTS` DI token | [src/orkes/orkes.tokens.ts](src/orkes/orkes.tokens.ts) |
| Auto-registration on boot | [src/orkes/orkes-bootstrap.service.ts](src/orkes/orkes-bootstrap.service.ts) |
| HTTP entry points (`POST /orkes/test-workflow`, `GET /orkes/workflow/:id`) | [src/orkes/orkes.controller.ts](src/orkes/orkes.controller.ts) |
| Workflow definitions (auto-registered) | [orkes/workflows/](orkes/workflows/) |
| Event handler definitions (auto-registered) | [orkes/event_handlers/](orkes/event_handlers/) |
| Demo workflow (INLINE → EVENT publish) | [orkes/workflows/kafka_demo_workflow.json](orkes/workflows/kafka_demo_workflow.json) |
| Demo workflow (echo started by handler) | [orkes/workflows/kafka_demo_consumer_workflow.json](orkes/workflows/kafka_demo_consumer_workflow.json) |
| Demo event handler (Kafka → start_workflow) | [orkes/event_handlers/order_placed_handler.json](orkes/event_handlers/order_placed_handler.json) |

## Enabling the module

`OrkesModule` is gated by these env vars (see
[src/config/env.schema.ts](src/config/env.schema.ts)):

```env
ORKES_ENABLED=true
ORKES_SERVER_URL=https://developer.orkescloud.com/api   # or one-dev.orkesconductor.io for the team env
ORKES_KEY=<application key id>
ORKES_SECRET=<application key secret>
ORKES_AUTO_REGISTER=true     # walk orkes/ and register on boot
```

When disabled or misconfigured, `ORKES_CLIENTS` resolves to `null` and the
controller responds `503 Service Unavailable`. The app boots either way.

## Triggering the demo loop

```bash
# Start the demo workflow from the app
curl -X POST http://localhost:3000/orkes/test-workflow \
  -H 'Content-Type: application/json' \
  -d '{"userId":"u-demo","amountCents":12345,"currency":"USD","sku":"SKU-DEMO"}'
# -> { "status":"accepted", "workflowId":"<uuid>" }

# Inspect the run
curl http://localhost:3000/orkes/workflow/<workflowId>
```

Open the run in the Orkes UI to see the INLINE → EVENT publish chain, plus
the consumer workflow started by `order_placed_handler` after the message
round-trips through Kafka.

## Team-isolation naming (codelab convention)

Multiple teams share the same Orkes environment **and** the same Kafka
topics. Per [docs/codelab.md](docs/codelab.md):

- **Resource names** prefix the team: `exp_{team_name}_vendor_onboarding`,
  `exp_{team_name}_vendor_onboarding_trigger`.
- **Kafka message types** prefix the team:
  `{team_name}.vendor.registration.submitted`. This means Event Handler
  conditions only need `$.metadata.type == '{team}.X.Y'` to enforce
  isolation — no separate `metadata.team` filter required.
- **Why embed in the type, not just metadata?** All teams share topics; the
  prefix makes natural-collision-free routing trivial.
- **Workflow `version`** is bumped on any task-graph or I/O change. Don't
  mutate published versions in place.

## The Standardized Kafka Event Envelope (Chorus standard)

```json
{
  "metadata": {
    "type":          "{team_name}.vendor.registration.submitted",
    "team":          "{team_name}",
    "correlationId": "abc-123",
    "timestamp":     "2026-03-07T12:00:00Z",
    "messageId":     "uuid-for-dedup",
    "eventType":     "...",
    "sourceSystem":  "...",
    "destinationSystem": "...",
    "requester":     "..."
  },
  "payload": {
    "vendorId":      "VND-123",
    "companyName":   "Acme Shipping"
  }
}
```

Inside an Event Handler, `$` references the **whole envelope**:

| Expression | Resolves to |
|---|---|
| `$.metadata.type` | Used in the `condition` for routing |
| `${metadata.correlationId}` | End-to-end trace key, propagate through every step |
| `${payload}` | Full business-data object |
| `${payload.vendorId}` | A single field |

`workflowId` is required on **every** message from CHORUS to external
systems so the receiver can correlate back.

## Hard rules (full lists in `.claude/rules/orkes-standards.md`)

1. **Don't put PII in `inputParameters`.** Orkes UI shows them in the run
   history indefinitely.
2. **Bump `version`** when changing a workflow's task graph or I/O shape.
   Don't mutate published versions in place.
3. **Always propagate `correlationId`** across every task and every Kafka
   publish.
4. **Production: Orkes drag-and-drop User Forms are PROHIBITED.** All forms
   must be built in the product using the CHORUS Design System.
5. **Don't access the database directly from Orkes.** No `JDBC` task. Route
   through backend services.
6. **Polling worker `SIMPLE` tasks are anti-pattern** at the Chorus
   standard. Use the Event/Wait/Event-Handler pub/sub chain instead.

## Cross-references

- For workflow JSON shape + lifecycle: use `orkes-workflows`.
- For task type catalog: use `orkes-tasks`.
- For Event Handlers (any source): use `orkes-event-handlers`.
- For architectural patterns: use `orkes-patterns`.
- For Kafka↔Orkes specifics (`_schema`, Avro EVENT tasks): use `kafka-orkes`.
- Project hard rules: [.claude/rules/orkes-standards.md](.claude/rules/orkes-standards.md).
- Authoritative training material: [docs/orkes_training.md](docs/orkes_training.md).
- End-to-end vendor-onboarding walkthrough: [docs/codelab.md](docs/codelab.md).
- BTH-specific Kafka spec: [docs/kafka.md](docs/kafka.md).
