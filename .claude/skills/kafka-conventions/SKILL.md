---
name: kafka-conventions
description: BTH naming conventions and Avro authoring rules for this repo — topic names, subjects, .avsc filenames, record/namespace patterns, optional/required fields, logical types. Use whenever creating or renaming a topic / subject / .avsc, authoring a new Avro schema, or reviewing a schema PR.
---

# Kafka conventions

This skill covers the **naming and authoring rules** for Kafka topics,
Schema Registry subjects, and Avro `.avsc` files in this repo. Activate it
whenever you're creating, renaming, or reviewing any of these.

For the full Avro types table, branch/cluster mapping, and detailed enum/map
syntax, see [reference.md](reference.md). For complete worked `.avsc`
examples, see [examples.md](examples.md).

## When to use

User wants to:
- Pick a name for a new topic / subject / `.avsc` file.
- Author a new Avro schema (record, enum, fixed, etc.).
- Rename or restructure existing schemas.
- Review whether a schema PR follows BTH naming.

If the work is changing the **shape** of an existing published schema (add /
delete / rename a field), use `kafka-evolution` instead — it covers the
rollout sequencing rules.

## Topic naming convention

Format: `{ORG}-{APP}-{ENV}-{FEATURE}-{TYPE}` — all dashes, lowercase.

| Slot | Source | Example |
|---|---|---|
| `ORG` | Organization | `one` |
| `APP` | Application / domain prefix | `bth` (or `om_com` per BTH spreadsheet) |
| `ENV` | Cluster environment | `dev` / `test` / `stage` / `prod` |
| `FEATURE` | Business event domain | `error-report`, `user-created`, `ian-notification` |
| `TYPE` | Direction + visibility | `in-private`, `out-private`, `in-public` |

Examples:
- `one-bth-dev-user-created-in-private`
- `one-om_com-dev-error-report-in-private`
- `one-om_com-stage-ian-notification-out-private`

This repo's `topics.build(feature, type)` helper in
[src/config/app-config.service.ts:52](src/config/app-config.service.ts) wires
`ORG` / `APP` / `ENV` from env vars (`KAFKA_TOPIC_ORG`, `KAFKA_TOPIC_APP`,
`KAFKA_TOPIC_ENV`). Use it instead of string-concatenating in production code.

> **Caveat**: the existing `*-events.types.ts` files **hardcode** topic names
> as `one-bth-dev-*` strings rather than calling `topics.build`. Don't follow
> that shortcut for new code that needs to deploy to non-dev clusters.

## Subject naming (TopicNameStrategy)

Subject = `<topic>-value` for the value schema. Append `-key` only if you
also register a key schema (rare in this repo).

- Topic `one-bth-dev-user-created-in-private` → subject
  `one-bth-dev-user-created-in-private-value`.
- Strategy class on the broker / Orkes side:
  `io.confluent.kafka.serializers.subject.TopicNameStrategy`.

## `.avsc` filename convention

Filename equals the subject (without `-value`) with **dashes replaced by
underscores**:

- Subject `one-bth-{env}-user-created-in-private-value` → filename
  `one_bth_user_created_in_private.avsc` (the upstream BTH repo drops `{env}`
  so the same file is reused across clusters).
- This repo embeds the env in the filename for dev simplicity:
  `one_bth_dev_user_created_in_private.avsc`.

The mapping `filename → topic → subject` is enforced by `subjectForFile` in
[src/schema-registry/schema-registry.service.ts:152](src/schema-registry/schema-registry.service.ts):

```
chorus/users/profile/one_bth_dev_user_created_in_private.avsc
  → topic   one-bth-dev-user-created-in-private
  → subject one-bth-dev-user-created-in-private-value
```

## Avro `name` field convention

Inside the `.avsc`, the record `name` is the **filename without extension**,
already in snake_case (since filenames use underscores).

```json
{
  "namespace": "chorus.com.ian",
  "type": "record",
  "name": "one_om_com_ian_notification_in_private",
  "fields": [...]
}
```

`namespace` mirrors the Java-style folder path under `schemas/chorus/...`
(dots, not slashes).

> **Demo deviation**: this repo's shipped `.avsc` files use friendly names
> (`UserCreated`, `OrderPlaced`) and namespace `com.example.events` instead
> of the BTH-spec snake-case names. New schemas going to BTH-governed
> registries should follow the snake-case convention.

## Folder structure (BTH `om-schema-registry` mirror)

```
schemas/
  chorus/
    {domain}/             # users, orders, ian, error-report, ...
      {module}/           # profile, checkout, notification, ...
        {topic_with_underscores}.avsc

# In the upstream BTH repo only — skipped in this fork:
docs/
  chorus/
    {domain}/
      {module}/
        asyncapi-{topic}.yaml
```

## Avro authoring rules

### Required vs optional fields

```jsonc
// Required (no default)
{ "name": "title", "type": "string" }

// Optional — MUST use union with null + default null
{ "name": "options", "type": ["null", "string"], "default": null }
```

Never write `{"type": "string", "optional": true}` — Avro has no such
concept. Without `default null`, adding the field later breaks BACKWARD/FULL
compatibility.

### Primitive types (most common)

| JSON-shaped type | Avro |
|---|---|
| `string` | `"string"` |
| 32-bit signed int | `"int"` |
| 64-bit signed int | `"long"` |
| float / decimal | `"double"` (or use logical-type `decimal` for currency) |
| boolean | `"boolean"` |
| binary | `"bytes"` |

> **Money rule**: prefer `"long"` minor units (cents) over `"double"`. See
> `OrderPlacedEvent.amountCents` in
> [schemas/chorus/orders/checkout/one_bth_dev_order_placed_in_private.avsc](schemas/chorus/orders/checkout/one_bth_dev_order_placed_in_private.avsc).

### Date / time logical types

| Wire shape | Avro |
|---|---|
| ISO date (`YYYY-MM-DD`) | `{ "type": "int", "logicalType": "date" }` |
| ISO datetime / epoch ms | `{ "type": "long", "logicalType": "timestamp-millis" }` |
| Epoch micros | `{ "type": "long", "logicalType": "timestamp-micros" }` |

This repo uses `timestamp-millis` for all `occurredAt` fields — populate via
`Date.now()` in the producer.

### Required attributes per record

| Field | Required? | Notes |
|---|---|---|
| `type` | yes | Almost always `"record"` at the top level |
| `name` | yes | Matches filename pattern (snake_case) |
| `namespace` | recommended | Mirrors `schemas/chorus/...` path |
| `fields` | yes | At least one field |
| `doc` | recommended | Both at record + field level |
| `aliases` | optional | For renames (see `kafka-evolution`) |

For complex types (arrays, enums, maps, unions), see [reference.md](reference.md).

## Common mistakes (auto-flag in review)

- Using `.` (dot) in a topic name. Confluent uses `.` as a metric separator
  and silently corrupts metrics that contain it.
- Bare `"type": "string"` for a field that's nullable in your domain — must
  be `["null", "string"]` with `"default": null`.
- Naming a record `UserCreatedEvent` (PascalCase) when the BTH spec calls
  for `one_bth_user_created_in_private` (snake_case mirroring topic). Note
  this repo's demo schemas deliberately deviate; new production schemas
  should follow the spec.
- `autoRegisterSchemas: true` in producer config — bypasses governance.
- Missing `default null` on a `["null", T]` union — breaks add-field on
  FULL/BACKWARD compatibility.
- Choosing `"double"` for money — float precision loses cents over time.

## Pre-merge checklist (schema authoring PR)

- [ ] Filename matches `{topic_with_underscores}.avsc`
- [ ] `name` field matches filename (or follows the documented friendly-name
      deviation, with PR rationale)
- [ ] `namespace` mirrors the `schemas/chorus/...` folder path
- [ ] All optional fields use `["null", T]` + `"default": null`
- [ ] All required fields have **no** default
- [ ] Money uses `"long"` minor units, not `"double"`
- [ ] Timestamps use `{"type":"long","logicalType":"timestamp-millis"}`
- [ ] `doc` strings present at record + field level
- [ ] No `.` in topic name; `_` only in filename + record `name`

## Cross-refs

- For schema **changes** (add/delete/rename): use `kafka-evolution`.
- For producer wiring once the `.avsc` is authored: use `kafka-producer`.
- For consumer wiring: use `kafka-consumer`.
- For the Orkes-side `_schema` reference (`<topic>-value`): use `kafka-orkes`.
- Project rule file: [.claude/rules/kafka-standards.md](.claude/rules/kafka-standards.md).
- Authoritative spec: [docs/kafka.md](docs/kafka.md) ("Naming Convention",
  "Avro format Guide" sections).
