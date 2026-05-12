# Orkes standards (project-scoped, hard rules)

These rules apply whenever a workflow definition, event handler definition, or
the `OrkesBootstrapService` is touched. They encode the BTH guideline's
"Producer in Orkes" / "Consumer in Orkes" sections plus this repo's
auto-registration mechanics.

The most important thing to internalize: **Orkes always references the latest
schema version** — there is no version pinning. Practically that makes Orkes
behave like compatibility mode `NONE` for any schema it touches. Plan rollouts
accordingly.

## Workflow + handler authoring

1. **Always include `_schema` in every Kafka `EVENT` task's `inputParameters`.**
   Value is `<topic>-value` (the TopicNameStrategy subject). Without it the
   Orkes Avro serializer cannot resolve the subject and the publish fails at
   task execution.
2. **Field names + types in `inputParameters` must match the registered Avro
   schema 1:1.** Orkes does **not** run client-side `Type.isValid` — the first
   you'll know about a mismatch is a Confluent serializer error in the workflow
   run output.
3. **Build payloads in an upstream task, then thread output into the EVENT
   task.** Use `INLINE` (JavaScript), `HTTP`, `JSON_JQ_TRANSFORM`, or `SIMPLE`
   tasks for payload assembly — see `orkes/workflows/kafka_demo_workflow.json`
   for the canonical pattern.
4. **`occurredAt` is `timestamp-millis`** — emit `new Date().getTime()` (epoch
   ms) inside the upstream `INLINE` task. Don't ISO-format.
5. **Workflow names are `lower_snake_case`** matching the JSON filename
   (e.g. `kafka_demo_workflow` ↔ `kafka_demo_workflow.json`). The bootstrap
   service registers by file basename order.
6. **Bump `version` when changing a workflow's task graph or input/output
   shape.** Don't mutate published versions in place — running workflows are
   pinned to the version they started under.

## Event Handler authoring

7. **`event` string format is `kafka:<topic>:<consumer-group>`.** Pick a
   **unique consumer group per handler** so independent handlers don't
   silently load-balance messages between each other.
8. **Use `${field}` interpolation against the decoded Avro payload** — Orkes
   does the deserialization. Field names are case-sensitive.
9. **`condition` defaults to `"true"` (fire on every event).** Filter with
   SpEL/JQ expressions when you only want a subset (e.g. `"$.amountCents > 0"`).
10. **Pick the right `action`** — `start_workflow` is the most common;
    `complete_task` closes a `WAIT` task in another running workflow;
    `update_task` and `terminate_workflow` exist for advanced patterns.

## Orkes Apache Kafka Integration (UI-side)

The integration must be configured **before** referencing it in `sink:
kafka:<topic>` — otherwise the workflow registration succeeds but task
execution silently fails.

11. **Sending Protocol = `AVRO`.** Required for Schema Registry interop.
12. **Value Subject Name Strategy = `io.confluent.kafka.serializers.subject.TopicNameStrategy`.**
    Matches this repo's app-side default.
13. **Schema Registry Auth Type = `Schema Registry User Info (Key/Password)`** —
    paste the same key/password as `SCHEMA_REGISTRY_USER` / `_PASS` in
    `.env`. Production uses `SASL_SSL`; this fork uses `PLAINTEXT` against the
    local broker for the demo loop.
14. **Bootstrap servers must be reachable from Orkes Cloud.** For local dev,
    use a tunnel (ngrok / cloudflared / Tailscale Funnel) and point the
    integration at the tunnel URL.
15. **The integration name in the Orkes UI must match the prefix** used in
    `sink: kafka:<integration>:<topic>` (or `sink: kafka:<topic>` if you only
    have one Kafka integration). The reference workflow in
    `orkes/workflows/kafka_demo_workflow.json` uses `kafka:<topic>` — adjust
    if you configure multiple integrations.

## Auto-registration

16. **Workflows live under `orkes/workflows/`, handlers under
    `orkes/event_handlers/`.** `OrkesBootstrapService.onApplicationBootstrap`
    in [src/orkes/orkes-bootstrap.service.ts](src/orkes/orkes-bootstrap.service.ts)
    walks both directories at boot when `ORKES_AUTO_REGISTER=true`.
17. **Workflow registration uses `overwrite=true`.** Pushing a new
    `kafka_demo_workflow.json` redeploys in place, replacing the latest
    version. Bump `version` in the JSON to keep history.
18. **Handler upsert is `getEventHandlerByName` → update if exists, else add**
    — see `OrkesBootstrapService.upsertEventHandler`. Renaming a handler
    without removing the old one leaves an orphan; clean up via the Orkes UI
    or the EventClient.
19. **Don't auto-register in production unless the bundle is the source of
    truth.** Set `ORKES_AUTO_REGISTER=false` and use Orkes' own deploy
    pipeline if multiple services share the workflow inventory.

## Privacy + observability

20. **Don't put PII in `inputParameters`.** Orkes UI shows them in the run
    history indefinitely. Hash, tokenize, or omit before publishing.
21. **Workflow run IDs are useful trace anchors** — log them when you start
    workflows from inside the app (`OrkesController.startTestWorkflow` does
    this). Include them in any related Kafka headers if you cross-correlate
    workflow runs with downstream events.

## Schema evolution caveat (effective `NONE`)

22. **Treat any `.avsc` change touched by an Orkes path as breaking** — Orkes
    pulls latest immediately, regardless of subject compatibility setting. The
    only safe change without downtime is "add an optional field that older
    messages don't include", and even that needs the Orkes side updated first
    to read the new field.
23. **For deletes, required-field adds, type changes, or renames touched by
    Orkes**: pause the relevant Event Handlers (`active: false`), push the
    schema, update workflow `inputParameters`, re-enable handlers, then burn
    down any Kafka backlog with the new shape. See `kafka-evolution` skill for
    the underlying matrix.

## Pre-merge checklist (Orkes JSON change)

- [ ] `_schema` matches `<topic>-value` exactly
- [ ] Every `inputParameters` key has a matching field in the registered `.avsc`
- [ ] `event` string uses the integration name configured in the Orkes UI
- [ ] Consumer group on the Event Handler is unique to this flow
- [ ] If schema is shared with app-side producer/consumer, rollout sequencing called out in PR
- [ ] Workflow run tested in the Orkes sandbox before merge
- [ ] `version` bumped if task graph or I/O shape changed
- [ ] No PII in `inputParameters` or workflow `outputParameters`
