---
name: kafka-producer
description: Patterns for implementing a BTH-compliant Kafka producer in this NestJS repo — ClientKafka via @nestjs/microservices, AvroSerializer with Type.isValid pre-encode validation, idempotent kafkajs producer, two-stage validation, error taxonomy. Use whenever wiring a new producer or debugging a SerializationError / SchemaPayloadInvalidError.
---

# Kafka producer

Patterns for **sending Avro-encoded messages** through the
`@nestjs/microservices` `ClientKafka` provided by this repo, including the
two-stage validation that fails fast before bytes hit the broker.

## When to use

User is:
- Adding a new producer for a topic.
- Replacing a raw-JSON producer with Avro + Schema Registry.
- Debugging `SerializationError` / `SchemaPayloadInvalidError` from
  `AvroSerializer`.
- Reviewing PR changes to anything under `src/features/*/[feature]-*.producer.ts`
  or [src/kafka/serdes/avro.serializer.ts](src/kafka/serdes/avro.serializer.ts).

If the task is **defining the topic / authoring the `.avsc`**, use
`kafka-conventions` first. If the task is **changing an existing schema**,
use `kafka-evolution`.

## AS-IS to TO-BE (per BTH guideline)

| Aspect | AS-IS (raw JSON) | TO-BE (this repo) |
|---|---|---|
| Format | JSON text | Avro binary + 5-byte SR frame (`0x00` + 4-byte schema id) |
| Serialization | `JSON.stringify` | `AvroSerializer.serialize(topic, payload)` |
| Validation | None | Client-side `Type.isValid` then SR encode (two-stage) |
| Evolution | Free | Producer-first only on add-field (FORWARD) — see `kafka-evolution` |
| Dependency | None | `@confluentinc/schemaregistry` + `avsc` |

## Two-stage validation contract

1. **Client-side** (`AvroSerializer.assertPayloadShape` in
   [src/kafka/serdes/avro.serializer.ts:86](src/kafka/serdes/avro.serializer.ts)):
   - Loads `.avsc` text via `SchemaRegistryService.getSchemaTextForTopic(topic)`.
   - Parses once, caches as `avsc.Type`.
   - Calls `type.isValid(value, { errorHook })` collecting field paths.
   - Throws `SchemaPayloadInvalidError(topic, paths)` on miss — **never reaches
     the broker**.
2. **Schema Registry-side** (Confluent `AvroSerializer`):
   - Re-validates against the registered schema.
   - Throws `SerializationError` with `Invalid message at <path>...` if shape
     still wrong.
   - Both paths are caught by `SchemaValidationExceptionFilter` and mapped to
     a 4xx HTTP response when triggered from a controller.

> If the local `.avsc` is unavailable (mocked in tests, or topic registered
> elsewhere), stage 1 is **skipped silently** and stage 2 carries the
> validation. Don't rely on stage 1 in tests without ensuring the schema
> service is wired with real `.avsc` text.

## Wiring pattern (this repo)

The `ClientKafka` provider is registered globally in
[src/kafka/kafka.module.ts](src/kafka/kafka.module.ts) — features just inject it.

```ts
// src/features/users/user-created.producer.ts (canonical pattern)
@Injectable()
export class UserCreatedProducer implements OnModuleInit, OnApplicationShutdown {
  constructor(@Inject(KAFKA_CLIENT_PRODUCER) private readonly client: ClientKafka) {}

  async onModuleInit() { await this.client.connect(); }
  async onApplicationShutdown() { await this.client.close(); }

  async emit(input: CreateUserInput): Promise<UserCreatedEvent> {
    const event: UserCreatedEvent = {
      eventId: randomUUID(),
      occurredAt: Date.now(),
      userId: input.userId,
      email: input.email,
      displayName: input.displayName ?? null,
    };
    await firstValueFrom(
      this.client.emit(USER_CREATED_TOPIC, { key: input.userId, value: event }),
    );
    return event;
  }
}
```

Three things to notice:
- `@Inject(KAFKA_CLIENT_PRODUCER)` — string token from
  [src/kafka/kafka.tokens.ts](src/kafka/kafka.tokens.ts).
- `firstValueFrom(client.emit(...))` — `emit` returns an `Observable`; await
  it explicitly so errors surface as rejected promises.
- Producer **populates `eventId` and `occurredAt`** — never delegate this
  to the caller. They're the traceability quartet anchors.

## Two producer paths in this repo

| Path | When | How |
|---|---|---|
| **`ClientKafka` (`KAFKA_CLIENT_PRODUCER`)** | Normal app emits — domain events from controllers, services | `client.emit(topic, { key, value })` — runs through `AvroSerializer` |
| **`ProducerService` (`KAFKA_CLIENT`)** | DLQ raw-byte publishing only | `producer.produce({ topic, raw: true, value: originalBuffer, headers })` — bypasses Avro |

`ProducerService` lives at
[src/kafka/producer.service.ts](src/kafka/producer.service.ts). Only the
`KafkaDlqFilter` should use the `raw: true` path; everything else routes
through the SR-aware `ClientKafka`.

## Schema-ID resolution

Pick one — both honor the SR client config in
[src/schema-registry/schema-registry.service.ts:36-43](src/schema-registry/schema-registry.service.ts)
(`useLatestVersion: true, autoRegisterSchemas: false,
subjectNameStrategyType: TOPIC`):

| Strategy | When | How |
|---|---|---|
| **By latest version** (default) | Most cases | `serializer.serialize(topic, payload)` — SR fetches latest schema for `<topic>-value` |
| **By explicit schema ID** | Locked rollouts, A/B versions | Pre-fetch with `client.getBySubjectAndId(subject, schemaId)`, then encode against that ID. See BTH guideline `encodeMessage` example in [docs/kafka.md](docs/kafka.md). |

Pin schema IDs via env (`SCHEMA_REGISTRY_VERSION.<EVENT>`) when the team
requires deterministic encoding across deploys.

## Hard rules (full list in `.claude/rules/kafka-standards.md`)

1. **Never** `autoRegisterSchemas: true` in production. Schemas live in
   `schemas/` and are governance-controlled.
2. **Never** mix raw JSON and Avro on the same topic — Avro consumers will
   treat raw bytes as poison pill (no `0x00` magic byte).
3. **Always** treat optional fields as `["null", T]` with `default: null`.
   Bare `T` becomes required.
4. **Always** carry `eventId`, `occurredAt` on every domain event for
   idempotency + provenance.
5. **Never** put PII in topic names. Topics are exposed in metrics dashboards.
6. **Always** log the schema ID extracted from the SR-framed buffer
   (`buf.readUInt32BE(1)`) at producer-side `INFO` for traceability — the
   `AvroSerializer` already does this.

## Common errors and fixes

| Error | Likely cause | Fix |
|---|---|---|
| `SchemaPayloadInvalidError: Payload failed Type.isValid for topic=... paths=[email]` | Field type mismatch client-side | Inspect `paths`; correct field type or default in the call site |
| `SerializationError: Invalid message at <field>` from SR | Local schema cache stale, or schema missing optional default | Restart app, sync `.avsc` with registry |
| `SerializationError: Unknown magic byte` (consumer side) | Producer sent raw JSON or wrong topic | Confirm `AvroSerializer` is wired into `ClientsModule.registerAsync` options |
| `RestError: Subject not found` | Topic not registered yet, or wrong subject name | Check `subjectForFile` in `SchemaRegistryService` matches `<topic>-value` |
| `ECONNREFUSED` on SR | Wrong `SCHEMA_REGISTRY_URL` or auth | Verify `.env` and that the SR container is up (`docker compose ps`) |

## DLQ contract (relevant for producers)

- A producer-side `SchemaPayloadInvalidError` thrown pre-encode never reaches
  the broker. The HTTP caller gets a `400 Bad Request` via
  `SchemaValidationExceptionFilter` in
  [src/kafka/filters/schema-validation.filter.ts](src/kafka/filters/schema-validation.filter.ts).
- DLQ messages preserve the **original SR-framed bytes** (no re-encode) so
  they can be replayed via `x-original-topic` header. The DLQ producer uses
  `ProducerService.produce({ raw: true })`.
- `KafkaDlqFilter` is currently **commented out** in
  [src/kafka/kafka.module.ts:65-68](src/kafka/kafka.module.ts) — re-enable
  before relying on DLQ in non-dev.

## Pre-merge checklist

- [ ] `.avsc` exists under `schemas/chorus/<domain>/<module>/`
- [ ] Topic constant in `*-events.types.ts` matches the env-derived name
- [ ] Producer connects in `onModuleInit`, closes in `onApplicationShutdown`
- [ ] `eventId` (UUID) + `occurredAt` (epoch ms) populated by the producer (not the caller)
- [ ] Caller uses `firstValueFrom(client.emit(...))` to await
- [ ] Unit test mocks `SchemaRegistryService.encode` and asserts the topic + payload
- [ ] Integration test (Testcontainers) round-trips one event through the broker
- [ ] No raw `console.log`; uses `Logger`

## Cross-refs

- For naming + Avro authoring: use `kafka-conventions`.
- For the consumer side: use `kafka-consumer`.
- Before any schema change: use `kafka-evolution`.
- For Orkes Event Tasks (different `_schema` mechanic): use `kafka-orkes`.
- Project hard rules: [.claude/rules/kafka-standards.md](.claude/rules/kafka-standards.md).
- Authoritative spec: [docs/kafka.md](docs/kafka.md) ("Producer Development Guide" section).
