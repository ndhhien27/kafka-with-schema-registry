---
name: kafka-orkes
description: Patterns for wiring Kafka through Orkes Conductor in this repo — Apache Kafka Integration setup, EVENT tasks (produce) with `_schema`, Event Handlers (consume), the effective-NONE compatibility caveat, OrkesBootstrapService auto-registration. Use whenever authoring or reviewing an Orkes workflow that reads from or writes to Kafka, or wiring the Orkes Cloud integration to a local broker.
---

# Kafka through Orkes Conductor

This skill covers Orkes-side Kafka wiring: the `EVENT` task that publishes,
the Event Handler that consumes, the `_schema` parameter that ties to the
Schema Registry subject, and the **effective-NONE compatibility caveat**
(Orkes always uses the latest schema version, with no version pinning).

## When to use

User is:
- Adding a Kafka `EVENT` task to an Orkes workflow.
- Adding an Orkes Event Handler that reacts to a Kafka topic.
- Configuring the Apache Kafka Integration in the Orkes UI.
- Debugging why an Orkes workflow's Kafka publish is dropping messages or
  failing schema validation.

If the work is purely **app-side** (NestJS producer/consumer) with no Orkes
involvement, use `kafka-producer` / `kafka-consumer` instead. If the change
touches **schema shape**, use `kafka-evolution` first to sequence the rollout
(Orkes makes this harder — see the caveat below).

## Big-picture mental model

| Side | App-only | Orkes |
|---|---|---|
| Produce | `ClientKafka.emit(topic, ...)` + `AvroSerializer` | `EVENT` task with `sink: kafka:<topic>` and `_schema` input |
| Consume | `@EventPattern(topic)` controller method | Event Handler with `event: kafka:<topic>:<group>` and `start_workflow` / `complete_task` action |
| Schema version pinning | Pin via `schemaId` or use latest | **Always latest** — no version control in Orkes |
| Compatibility (effective) | Whatever the subject is set to | **NONE** — uploads take effect immediately for Orkes paths |

The `kafka-evolution` skill explains why Orkes is effectively `NONE` and what
that means for rollouts.

## Apache Kafka Integration (Orkes UI setup)

When configuring the integration in the Orkes Conductor UI ("Apache Kafka
Integration"), pick:

| Setting | Value |
|---|---|
| Sending Protocol | `AVRO` |
| Connection Security | `SASL_SSL` (BTH guideline default; this repo uses `PLAINTEXT` against the local broker — adjust per env) |
| Schema Registry Auth Type | `Schema Registry User Info (Key/Password)` |
| Value Subject Name Strategy | `io.confluent.kafka.serializers.subject.TopicNameStrategy` |
| Bootstrap servers | The broker URL Orkes can reach (use a tunnel like ngrok / cloudflared / Tailscale Funnel for local dev) |

The integration **name** chosen in the UI must match the
`kafka:<integration>:<topic>` prefix you use in workflow `EVENT` task `sink`
fields. The reference workflow in
[orkes/workflows/kafka_demo_workflow.json](orkes/workflows/kafka_demo_workflow.json)
uses `kafka:<topic>` (no integration prefix) — that works when there's only
one Kafka integration configured.

## Producer side: the `EVENT` task with `_schema`

Per BTH guideline §Producer in Orkes: include `_schema` in the Event Task's
`inputParameters` to tell the Avro serializer which subject to use.

```json
{
  "name": "publish_order_placed",
  "taskReferenceName": "publish_order_placed",
  "type": "EVENT",
  "sink": "kafka:one-bth-dev-order-placed-in-private",
  "asyncComplete": false,
  "inputParameters": {
    "_schema": "one-bth-dev-order-placed-in-private-value",
    "eventId":     "${build_payload.output.result.eventId}",
    "occurredAt":  "${build_payload.output.result.occurredAt}",
    "orderId":     "${build_payload.output.result.orderId}",
    "userId":      "${build_payload.output.result.userId}",
    "amountCents": "${build_payload.output.result.amountCents}",
    "currency":    "${build_payload.output.result.currency}",
    "items":       "${build_payload.output.result.items}"
  }
}
```

Rules:
1. `_schema` value = `<topic>-value` (TopicNameStrategy subject). **Always.**
2. Field names + types in `inputParameters` must match the registered Avro
   schema 1:1 — Orkes gives no client-side `Type.isValid`. The first you'll
   know about a mismatch is a Confluent serializer error in the workflow run
   output.
3. `occurredAt` is Avro `timestamp-millis`, so emit `new Date().getTime()`
   (epoch milliseconds) inside an `INLINE` task to populate it.
4. Build the payload in an upstream `INLINE` / `HTTP` / `SIMPLE` task, then
   thread its output into `inputParameters`.

This repo's reference workflow lives at
[orkes/workflows/kafka_demo_workflow.json](orkes/workflows/kafka_demo_workflow.json).

## Consumer side: Event Handler

Per BTH guideline §Consumer in Orkes: an Event Handler listens on
`kafka:<topic>:<consumer-group>` and runs actions on each event.

```json
{
  "name": "order_placed_handler",
  "event": "kafka:one-bth-dev-order-placed-in-private:orkes-demo-handler",
  "condition": "true",
  "active": true,
  "actions": [
    {
      "action": "start_workflow",
      "start_workflow": {
        "name": "kafka_demo_consumer_workflow",
        "version": 1,
        "input": {
          "eventId":     "${eventId}",
          "orderId":     "${orderId}",
          "userId":      "${userId}",
          "amountCents": "${amountCents}",
          "currency":    "${currency}"
        }
      }
    }
  ]
}
```

Rules:
1. The `event` string is `kafka:<topic>:<consumer-group>`. Pick a unique
   consumer group **per handler** so you can scale + monitor independently.
2. `${field}` references inside `start_workflow.input` extract from the
   **decoded** Avro payload (Orkes does the deserialization).
3. Actions: `start_workflow` (most common), `complete_task` (close a `WAIT`
   task in another running workflow), `update_task`, `terminate_workflow`.
4. `condition` is a SpEL/JQ expression — evaluate `true` to fire on every
   event, or filter (`"$.amountCents > 0"`).

Reference handler:
[orkes/event_handlers/order_placed_handler.json](orkes/event_handlers/order_placed_handler.json).

## Auto-registration on boot

This repo's `OrkesBootstrapService`
([src/orkes/orkes-bootstrap.service.ts](src/orkes/orkes-bootstrap.service.ts))
walks `orkes/workflows/*.json` and `orkes/event_handlers/*.json` at app boot
when `ORKES_AUTO_REGISTER=true`:

- `metadataClient.registerWorkflowDef(def, true)` — `overwrite=true` so
  updates redeploy in place. Bump `version` in the JSON to keep history.
- Event handlers: `getEventHandlerByName(name)` → `updateEventHandler` if
  exists, else `addEventHandler`.

To trigger from inside the app:

```bash
curl -X POST http://localhost:3000/orkes/test-workflow \
  -H 'Content-Type: application/json' \
  -d '{"userId":"u-demo","amountCents":12345,"currency":"USD","sku":"SKU-DEMO"}'
```

Inspect a run:

```bash
curl http://localhost:3000/orkes/workflow/<workflowId>
```

The HTTP endpoints live in
[src/orkes/orkes.controller.ts](src/orkes/orkes.controller.ts).

## Enabling the Orkes module

`OrkesModule` is gated behind these env vars (see
[src/config/env.schema.ts](src/config/env.schema.ts) and
[src/orkes/orkes.module.ts](src/orkes/orkes.module.ts)):

```env
ORKES_ENABLED=true
ORKES_SERVER_URL=https://developer.orkescloud.com/api
ORKES_KEY=<application key id>
ORKES_SECRET=<application key secret>
ORKES_AUTO_REGISTER=true     # walk orkes/ and register on boot
```

If `ORKES_ENABLED=false` or any of `ORKES_SERVER_URL` / `ORKES_KEY` /
`ORKES_SECRET` are unset, the `ORKES_CLIENTS` provider returns `null` and
the `OrkesController` responds with `503 Service Unavailable`. This is by
design — the demo can boot without an Orkes account.

## Hard rules (full list in `.claude/rules/orkes-standards.md`)

1. **Always** include `_schema: "<topic>-value"` on `EVENT` tasks. Without
   it, the Avro serializer can't resolve the subject and the publish fails.
2. **Never** rely on Orkes for schema-version pinning — Orkes always uses
   latest. If you need pinning, do the publish from the app side (see
   `kafka-producer`).
3. **Treat Orkes-touched schemas as effective `NONE`.** Any `.avsc` change
   immediately affects Orkes flows; coordinate rollouts manually.
4. **Use a unique consumer group per Event Handler.** Sharing groups across
   handlers leads to messages being silently load-balanced between unrelated
   subscribers.
5. **Configure the integration in the Orkes UI before referencing it** in
   `sink: kafka:<topic>`. Otherwise the workflow registration succeeds but
   the run fails silently at task execution.
6. **Don't put PII in `inputParameters`.** Orkes UI shows them in the run
   history indefinitely.

## Rollout caveat — the effective-`NONE` problem

Per BTH guideline §Type 2 (Producer or Consumer is Orkes):

> Orkes can only reference the latest schema version, so it is correct to
> consider its Compatibility Mode to be NONE. Not gracefully transitionable.

Practical implication: when an `.avsc` is consumed by an Orkes workflow, the
**only safe change without coordination** is "add an optional field that
older messages don't include" — and even that needs the Orkes side updated
first to read the new field.

For deletes / required-field adds / type changes / renames touched by Orkes,
the rollout is an incident:

1. Pause the Orkes workflow (set `active: false` on relevant Event Handlers).
2. Push schema v2.
3. Update workflow `inputParameters` to match v2.
4. Re-enable the workflow.
5. Burn down the Kafka backlog with the new shape.

See `kafka-evolution` for the underlying compatibility matrix.

## Common errors and fixes

| Symptom | Likely cause | Fix |
|---|---|---|
| `EVENT` task fails with `Subject not found` | `_schema` value typo, or schema not yet registered | Verify `<topic>-value` exact match; check Confluent UI |
| `EVENT` task fails with `Invalid Avro encoding` | `inputParameters` field shape doesn't match registered schema | Diff the workflow JSON's keys against the `.avsc` field list |
| Event Handler never fires | Wrong `event` prefix, integration not configured, or `active: false` | Check `kafka:<integration-name>:<topic>:<group>` parts; verify integration in UI |
| Handler fires but `${field}` resolves to empty | Field name in handler input mismatches Avro field | Avro is case-sensitive; verify exact spelling |
| Workflow stuck after `EVENT` publish | `asyncComplete: true` and no caller closed the wait | Set `asyncComplete: false` for fire-and-forget, or add a corresponding `complete_task` action elsewhere |
| `OrkesController` returns 503 | `ORKES_ENABLED=false` or missing `KEY`/`SECRET`/`SERVER_URL` | Set the env vars; restart the app |
| `OrkesBootstrapService` logs `No workflow JSON under <dir>` | Working directory mismatch | Run from repo root, or fix the candidate paths in `resolveOrkesDir` |

## Pre-merge checklist (Orkes JSON change)

- [ ] `_schema` matches `<topic>-value` exactly
- [ ] Every `inputParameters` key has a matching field in the registered `.avsc`
- [ ] `event` string uses the integration name configured in the Orkes UI
- [ ] Consumer group on the Event Handler is unique to this flow
- [ ] If schema is shared with app-side producer/consumer, rollout sequencing called out in PR (see `kafka-evolution`)
- [ ] Workflow run tested in the Orkes sandbox before merge
- [ ] `version` bumped if task graph or I/O shape changed
- [ ] No PII in `inputParameters` or workflow `outputParameters`

## Cross-refs

- For `<topic>-value` derivation: use `kafka-conventions`.
- For the app-side equivalent (when you need version pinning): use
  `kafka-producer`.
- For the app-side equivalent (when you need DLQ + retry semantics): use
  `kafka-consumer`.
- For the effective-NONE caveat and rollout flows: use `kafka-evolution`.
- Project hard rules: [.claude/rules/orkes-standards.md](.claude/rules/orkes-standards.md).
- Authoritative spec: [docs/kafka.md](docs/kafka.md) ("Producer in Orkes" /
  "Consumer in Orkes" sections); [docs/codelab.md](docs/codelab.md)
  end-to-end vendor-onboarding walkthrough; [docs/orkes_training.md](docs/orkes_training.md)
  training material.
