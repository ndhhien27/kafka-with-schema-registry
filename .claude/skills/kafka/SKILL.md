---
name: kafka
description: Master/router skill for Kafka, Avro, Schema Registry, and Orkes Conductor work in this repo. Activate whenever the user mentions Kafka topics, Avro schemas, producers, consumers, Schema Registry, BTH/Chorus, or Orkes — even if they don't say "skill". Routes to the focused sub-skill that actually handles the change.
---

# Kafka master skill

This is the entry point for **any** Kafka, Avro, Schema Registry, or Orkes
Conductor work in this NestJS reference repo. Read this skill first to find the
right sub-skill, the canonical mental model, and the repo anchors. For deep
dives on file paths and tokens, see [reference.md](reference.md). For
end-to-end walkthroughs, see [examples.md](examples.md).

## When to use

Activate any time the user mentions:

- BTH / Chorus / `om-schema-registry`
- A Kafka topic, Avro schema, Schema Registry, Confluent broker
- This repo's `producer.service.ts`, `avro.serializer.ts`,
  `schema-registry.service.ts`, `schemas/chorus/...`
- Orkes Conductor with Kafka (Event Task, Event Handler, `_schema`)
- Concepts like `@EventPattern`, `ClientKafka`, `kafkajs`, `KafkaContext`,
  `@KafkaRetry`, DLQ

## Mental model — data-as-a-contract

Kafka itself is just bytes. **Avro + Confluent Schema Registry add the
contract**:

- **Avro** is schema-first binary serialization. The schema lives in `.avsc`
  (JSON syntax), the wire format is compact binary.
- **Schema Registry** is the single source of truth — versioned per subject,
  enforces compatibility on each register.
- **TopicNameStrategy** subject is `<topic>-value` for value schemas (always,
  in this repo).
- Producers and consumers fail-fast on shape violations rather than corrupting
  downstream state.
- **Wire format**: 5-byte Schema-Registry frame (magic `0x00` + 4-byte schema
  id, big-endian) followed by Avro-binary payload. Read the schema id with
  `buf.readUInt32BE(1)`.

## Decision tree — pick the sub-skill

| User intent | Sub-skill |
|---|---|
| Naming a topic / subject / `.avsc` file; authoring an Avro schema | `kafka-conventions` |
| Implementing a producer (encode + send) | `kafka-producer` |
| Implementing a consumer (subscribe + decode) | `kafka-consumer` |
| Changing an existing schema (add/delete/rename a field, change type) | `kafka-evolution` |
| Wiring Kafka through Orkes Conductor (workflows, event handlers) | `kafka-orkes` |
| Pure debugging without changes | the generic `debug-issue` skill |
| Pure code-review of a change | the generic `review-changes` skill |

If the user's intent spans two sub-skills (e.g. "add a new event" = conventions
+ producer + consumer + Orkes), invoke the relevant sub-skills in turn rather
than trying to cover everything from this master skill.

## Repo anchors (this codebase)

| Concern | File |
|---|---|
| `Type.isValid` pre-encode + AvroSerializer | [src/kafka/serdes/avro.serializer.ts](src/kafka/serdes/avro.serializer.ts) |
| AvroDeserializer (SR-framed → JSON) | [src/kafka/serdes/avro.deserializer.ts](src/kafka/serdes/avro.deserializer.ts) |
| SR error taxonomy | [src/kafka/serdes/sr-errors.ts](src/kafka/serdes/sr-errors.ts) |
| Schema auto-registration + FULL compatibility | [src/schema-registry/schema-registry.service.ts](src/schema-registry/schema-registry.service.ts) |
| Topic-naming env helper (`topics.build`) | [src/config/app-config.service.ts](src/config/app-config.service.ts) |
| ClientKafka + AvroSerializer wiring | [src/kafka/kafka.module.ts](src/kafka/kafka.module.ts) |
| ServerKafka (consumer) wiring | [src/main.ts](src/main.ts) |
| Producer example | [src/features/users/user-created.producer.ts](src/features/users/user-created.producer.ts) |
| Consumer example (no retry) | [src/features/users/user-created.consumer.ts](src/features/users/user-created.consumer.ts) |
| Consumer example (with retry) | [src/features/orders/order-placed.consumer.ts](src/features/orders/order-placed.consumer.ts) |
| DLQ filter | [src/kafka/filters/kafka-dlq.filter.ts](src/kafka/filters/kafka-dlq.filter.ts) |
| Schema-validation HTTP filter | [src/kafka/filters/schema-validation.filter.ts](src/kafka/filters/schema-validation.filter.ts) |
| Retry interceptor + decorator | [src/kafka/interceptors/kafka-retry.interceptor.ts](src/kafka/interceptors/kafka-retry.interceptor.ts), [src/kafka/decorators/kafka-retry.decorator.ts](src/kafka/decorators/kafka-retry.decorator.ts) |
| Orkes module + bootstrap | [src/orkes/orkes.module.ts](src/orkes/orkes.module.ts), [src/orkes/orkes-bootstrap.service.ts](src/orkes/orkes-bootstrap.service.ts) |
| Orkes workflows | [orkes/workflows/](orkes/workflows/) |
| Orkes event handlers | [orkes/event_handlers/](orkes/event_handlers/) |
| Demo schemas | [schemas/chorus/users/profile/](schemas/chorus/users/profile/), [schemas/chorus/orders/checkout/](schemas/chorus/orders/checkout/) |

For the full file map and a glossary, see [reference.md](reference.md).

## Quickstart (golden path)

```bash
docker compose up -d                       # Kafka KRaft + Schema Registry
pnpm install
cp .env.example .env
pnpm start:dev

# Produce one event
curl -X POST http://localhost:3000/users \
  -H 'Content-Type: application/json' \
  -d '{"userId":"u-1","email":"alice@example.com"}'

# Watch the consumer log line:
#   UserCreated received id=... user=u-1 email=alice@example.com partition=0 offset=...

# Health + lag
curl http://localhost:3000/health
curl http://localhost:3000/metrics | grep kafka_consumer_lag
```

For an "add a new event end-to-end" walkthrough (avsc + topic constant +
producer + consumer + test), see [examples.md](examples.md).

## Hard rules (do not violate — full list in `.claude/rules/kafka-standards.md`)

1. **Never send raw JSON** to a topic that has a registered Avro subject —
   the consumer treats it as a poison pill (no leading `0x00` magic byte).
2. **Never set `autoRegisterSchemas: true`** in production — the registry is
   governance-controlled.
3. **Optional fields use `["null", T]` with `"default": null`** — never bare
   nullable types. Avro has no `optional` keyword.
4. **Default subject compatibility = `FULL`** unless you have an explicit
   rollout plan (see `kafka-evolution`).
5. **Topics use dashes, filenames use underscores** —
   `one_bth_dev_user_created_in_private.avsc` ↔ `one-bth-dev-user-created-in-private`.
6. **Subject = `<topic>-value`** under TopicNameStrategy. No exceptions.
7. **Always populate `eventId` (UUID) and `occurredAt` (epoch millis)** on
   every domain event.

## Project caveats (read before editing)

- `KafkaDlqFilter` is currently **commented out** in
  [src/kafka/kafka.module.ts](src/kafka/kafka.module.ts) — DLQ won't fire
  until you re-enable the `APP_FILTER` provider.
- `KafkaRetryInterceptor` defaults `maxAttempts` to **1** (no retry) —
  handlers must opt in via `@KafkaRetry`.
- Topic constants in `src/features/*/[feature]-events.types.ts` are
  **hardcoded** to `one-bth-dev-*` instead of built via
  `AppConfigService.topics.build(...)`. Don't propagate this shortcut to
  production code.
- Demo `.avsc` files use `name: "UserCreated"` / `namespace:
  "com.example.events"` — **not** the BTH-spec snake-case form. New schemas
  should follow `kafka-conventions`.

## Cross-references

- BTH guideline source: [docs/kafka.md](docs/kafka.md) — the transcribed
  authoritative spec.
- Architecture diagrams: [docs/architecture.md](docs/architecture.md),
  [docs/produce-consume-flow.md](docs/produce-consume-flow.md).
- Codelab walkthrough (vendor onboarding): [docs/codelab.md](docs/codelab.md).
- Orkes training material: [docs/orkes_training.md](docs/orkes_training.md).
- README quickstart: [README.md](README.md).
- Project hard rules: [.claude/rules/kafka-standards.md](.claude/rules/kafka-standards.md),
  [.claude/rules/orkes-standards.md](.claude/rules/orkes-standards.md),
  [.claude/rules/observability.md](.claude/rules/observability.md).
