---
name: kafka-consumer
description: Patterns for implementing a BTH-compliant Kafka consumer in this NestJS repo — @EventPattern handlers via @nestjs/microservices, AvroDeserializer, KafkaContext API, @KafkaRetry decorator, retry interceptor + DLQ filter contract. Use whenever wiring a new consumer or debugging a decode failure / poison pill / HandlerExhaustedError.
---

# Kafka consumer

Patterns for **receiving and processing Avro-encoded messages** through
NestJS `@EventPattern` handlers in this repo, including retry + DLQ contracts.

## When to use

User is:
- Adding an `@EventPattern` handler for a topic.
- Replacing a raw-JSON consumer with the SR-aware `AvroDeserializer`.
- Debugging a `SerializationError` on decode, a poison-pill DLQ message, or
  `HandlerExhaustedError`.
- Reviewing PR changes under `src/features/*/[feature]-*.consumer.ts` or
  [src/kafka/serdes/avro.deserializer.ts](src/kafka/serdes/avro.deserializer.ts).

If the task is **publishing** (rather than consuming), use `kafka-producer`.
If the change touches schema **shape**, use `kafka-evolution`.

## AS-IS to TO-BE (per BTH guideline)

| Aspect | AS-IS (raw JSON) | TO-BE (this repo) |
|---|---|---|
| Format | JSON text | Avro binary, schema id read from payload |
| Deserialization | `JSON.parse` | `AvroDeserializer.deserialize(rawMessage, { channel: topic })` |
| Evolution | Free | Consumer-first only on delete-field (BACKWARD) — see `kafka-evolution` |
| Group identity | Manual `groupId` | NestJS `consumer: { groupId }` from `KAFKA_GROUP_ID` env |
| Compatibility | None | Honors registered subject's `compatibility` (default `FULL`) |

## Wiring pattern (this repo)

The `ServerKafka` microservice is wired once in
[src/main.ts](src/main.ts) — every `@EventPattern` handler in the app
shares the same consumer group.

```ts
// src/features/users/user-created.consumer.ts (canonical pattern, no retry)
@Controller()
export class UserCreatedConsumer {
  private readonly logger = new Logger(UserCreatedConsumer.name);

  @EventPattern(USER_CREATED_TOPIC)
  async handle(
    @Payload() value: UserCreatedEvent,
    @Ctx() ctx: KafkaContext,
  ): Promise<void> {
    const partition = ctx.getPartition();
    const offset = ctx.getMessage().offset;
    this.logger.log(
      `UserCreated received id=${value.eventId} user=${value.userId}` +
        ` email=${value.email} partition=${partition} offset=${offset}`,
    );
    // domain logic — must be idempotent on value.eventId
  }
}
```

```ts
// src/features/orders/order-placed.consumer.ts (with retry)
@Controller()
export class OrderPlacedConsumer {
  @EventPattern(ORDER_PLACED_TOPIC)
  @KafkaRetry({ maxAttempts: 3, backoffMs: [100, 300, 800] })
  async handle(@Payload() value: OrderPlacedEvent, @Ctx() _ctx: KafkaContext): Promise<void> {
    if (value.amountCents < 0) throw new Error(`invalid amountCents=${value.amountCents}`);
    // ...
  }
}
```

Register the consumer class as a `@Controller()` and list it under
`controllers: []` of its feature module — Nest discovers `@EventPattern`
handlers at boot.

## Decode flow (`AvroDeserializer`)

1. NestJS's `KafkaParser` inspects the leading byte. If it's `0x00`, the buffer
   is preserved as-is and handed to the deserializer.
2. [`AvroDeserializer.deserialize`](src/kafka/serdes/avro.deserializer.ts):
   - Reads schema ID from bytes 1–4 (`readUInt32BE(1)`).
   - Calls `SchemaRegistryService.decode(channel, value)`.
   - Returns `{ pattern, data }` for the dispatcher.
3. `@Payload() value` in the handler receives the decoded record.
4. `KafkaContext.getMessage().value` still holds the **original SR-framed
   Buffer** so the DLQ filter can replay raw bytes without re-encoding.

## Hard rules (full list in `.claude/rules/kafka-standards.md`)

1. **One Nest app = one consumer group.** All `@EventPattern` handlers share
   `cfg.kafka.groupId`. Need per-feature scaling? Split into separate Nest
   microservice processes.
2. **Never** `JSON.parse` the raw value — bypasses Avro and breaks schema
   evolution.
3. **Never** swallow exceptions in handlers. Throw → retry interceptor → DLQ
   filter. The partition advances automatically on retries; nothing else
   commits the offset.
4. **Always** declare retry policy explicitly with `@KafkaRetry({
   maxAttempts, backoffMs })` for handlers with retryable failures (network,
   downstream throttling). Skip it for pure idempotent compute. Without it,
   `maxAttempts` defaults to **1** (no retry).
5. **Always** make handlers idempotent — Kafka guarantees at-least-once. Use
   `eventId` for de-duplication.
6. **Always** log `topic`, `partition`, `offset`, `eventId` on entry — these
   are the **traceability quartet**.

## Retry + DLQ contract

```ts
@KafkaRetry({ maxAttempts: 3, backoffMs: [100, 300, 800] })
@EventPattern(ORDER_PLACED_TOPIC)
async handle(@Payload() value: OrderPlacedEvent) {
  if (value.amountCents < 0) throw new Error(`invalid amountCents=${value.amountCents}`);
  // ... business logic ...
}
```

Failure flow:
1. Handler throws on attempts 1, 2, 3.
2. After `maxAttempts`, [`KafkaRetryInterceptor`](src/kafka/interceptors/kafka-retry.interceptor.ts)
   raises `HandlerExhaustedError(attempts)`.
3. [`KafkaDlqFilter`](src/kafka/filters/kafka-dlq.filter.ts) catches it and
   produces the **original SR-framed bytes** to `<topic>.DLQ` with these
   headers:
   - `x-original-topic`, `x-original-partition`, `x-original-offset`
   - `x-error-name: HandlerExhaustedError` (or `PoisonPillError` on decode failure)
   - `x-error-message` (first 512 chars)
   - `x-attempts`, `x-dlq-at`
4. Offset is committed; partition is unblocked.

For decode failures (poison pills), `AvroDeserializer` throws →
`KafkaDlqFilter` routes to DLQ with `x-error-name: PoisonPillError`. **No
handler attempts are consumed.**

> **Caveat**: `KafkaDlqFilter` is currently **commented out** in
> [src/kafka/kafka.module.ts:65-68](src/kafka/kafka.module.ts). Re-enable
> before relying on the DLQ flow in any non-dev environment. Without it,
> uncaught handler exceptions surface as unhandled rejections and the
> partition may stall.

## Default retry behavior

Defaults from
[src/kafka/interceptors/kafka-retry.interceptor.ts:17-18](src/kafka/interceptors/kafka-retry.interceptor.ts):

| Setting | Default | Override |
|---|---|---|
| `maxAttempts` | `1` (no retry) | `@KafkaRetry({ maxAttempts: N })` |
| `backoffMs` | `[100, 500, 2000]` (last value repeats if attempts > array length) | `@KafkaRetry({ backoffMs: [...] })` |
| `dlqTopic` | `<topic>.DLQ` | `@KafkaRetry({ dlqTopic: '...' })` |

So a handler **without** `@KafkaRetry` will run exactly once. If it throws,
the DLQ filter (when enabled) routes the message and commits the offset.

## Common errors and fixes

| Symptom | Likely cause | Fix |
|---|---|---|
| `Unknown magic byte` | Producer wrote raw JSON to an Avro topic | Verify producer wires `AvroSerializer`; check upstream services |
| `Schema not found in cache, id=12345` | Schema registered after consumer connected to SR; consumer cache stale | Restart consumer or call `client.getSchema(id)` to refresh |
| Stuck partition (no new messages processed) | Handler is `await`-ing a stuck downstream call without timeout | Add timeout + `@KafkaRetry` so it surfaces to DLQ |
| `RestError: 401` from SR | Wrong `SCHEMA_REGISTRY_USER`/`PASS` | Re-issue API key from Confluent UI |
| `eachMessage` runs but nothing logs | Wrong `groupId` (committed offsets ahead of expectation) or `subscribe.fromBeginning: false` and no new traffic | Use a fresh group id or seek to start |

## eachMessage vs eachBatch

This repo defaults to `eachMessage` (one decode per message). Use `eachBatch`
only when:
- You need to commit offsets manually after a batch operation (e.g. bulk DB write).
- You're hitting throughput ceilings on `eachMessage`.

Switching to `eachBatch` requires bypassing `@EventPattern` and registering a
custom kafkajs consumer — the BTH guideline ([docs/kafka.md](docs/kafka.md)
→ "Consumer Development Guide") has the sketch.

## Pre-merge checklist

- [ ] Consumer class is `@Controller()` and listed under feature module's `controllers: []`
- [ ] Handler logs the **traceability quartet** (`topic`/`partition`/`offset`/`eventId`) on entry
- [ ] `@KafkaRetry` declared if downstream calls can fail transiently
- [ ] Handler is idempotent (uses `eventId` for de-dup if it has side effects)
- [ ] Unit test exercises both happy path and a thrown error → DLQ assertion
- [ ] Integration test (Testcontainers) round-trips one event end-to-end
- [ ] No raw `console.log`; uses `Logger`

## Cross-refs

- For the producer side: use `kafka-producer`.
- For naming + Avro authoring: use `kafka-conventions`.
- Before any schema change: use `kafka-evolution`.
- For Orkes-side Event Handlers (different mechanic): use `kafka-orkes`.
- Project hard rules: [.claude/rules/kafka-standards.md](.claude/rules/kafka-standards.md).
- Authoritative spec: [docs/kafka.md](docs/kafka.md) ("Consumer Development Guide" section).
