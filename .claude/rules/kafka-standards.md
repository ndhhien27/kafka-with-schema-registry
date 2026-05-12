# Kafka standards (project-scoped, hard rules)

These are non-negotiable rules for **any code that produces or consumes Kafka
messages in this repo**. They encode BTH guideline mandates plus this repo's
operational reality. Violating them tends to corrupt downstream state, bypass
governance, or block partitions in production.

When the rule says "never" or "always", treat it as `BLOCK` severity in code
review. When you genuinely need to violate one, document the rationale in the
PR and link to a follow-up issue.

## Serialization

1. **Never send raw JSON to a topic that has a registered Avro subject.** Avro
   consumers identify Schema-Registry-framed bytes by leading magic byte `0x00`
   — raw JSON triggers a `PoisonPillError` and routes to `<topic>.DLQ`.
2. **Never set `autoRegisterSchemas: true` in production.** Schemas live under
   `schemas/chorus/...` and are governance-controlled. The serializer in
   [src/schema-registry/schema-registry.service.ts](src/schema-registry/schema-registry.service.ts)
   uses `autoRegisterSchemas: false, useLatestVersion: true` — keep it that way.
3. **Always go through `AvroSerializer`** (configured in
   [src/kafka/kafka.module.ts](src/kafka/kafka.module.ts) on `ClientKafka`)
   for outbound messages on Avro topics. The only exception is the DLQ path,
   which uses `ProducerService.produce({ raw: true })` to preserve original bytes.
4. **Always go through `AvroDeserializer`** for inbound messages — wired on the
   `ServerKafka` microservice in [src/main.ts](src/main.ts). Don't `JSON.parse`
   raw values inside handlers.

## Naming

5. **Topics use dashes, filenames use underscores.** Topic
   `one-bth-dev-user-created-in-private` ↔ filename
   `one_bth_dev_user_created_in_private.avsc`. The mapping is enforced by
   `subjectForFile` in [src/schema-registry/schema-registry.service.ts:152](src/schema-registry/schema-registry.service.ts).
6. **Subjects under TopicNameStrategy** are always `<topic>-value`. Append
   `-key` only if you also register a key schema (rare — not used in this repo today).
7. **Topic format is `{ORG}-{APP}-{ENV}-{FEATURE}-{TYPE}`** per BTH guideline.
   `ORG`, `APP`, `ENV` come from `KAFKA_TOPIC_*` env vars; build via
   `AppConfigService.topics.build(feature, type)` in
   [src/config/app-config.service.ts:52](src/config/app-config.service.ts).
8. **Never put PII in topic names** — topics are exposed in metrics dashboards,
   logs, and the Confluent UI.
9. **Never use `.` (dot) in a topic name.** Confluent uses `.` as a metric
   separator and silently corrupts metrics that contain it.

## Schema authoring

10. **Optional fields use `["null", T]` with `"default": null`** — never bare
    nullable types. Avro has no `optional` keyword. Without `default null`,
    adding the field later breaks BACKWARD/FULL compatibility.
11. **Required fields have no default.** Adding a required field to an existing
    schema is a breaking change under FULL — see the `kafka-evolution` skill.
12. **Default subject compatibility is `FULL`** (set per subject after
    registration in [src/schema-registry/schema-registry.service.ts:111](src/schema-registry/schema-registry.service.ts)).
    Override only as part of a coordinated rollout.
13. **Money is `long` minor units, not `double`.** Floats lose precision; always
    serialize amounts in cents/satoshi/etc. (`amountCents` in `OrderPlacedEvent`).
14. **Timestamps use `{"type":"long","logicalType":"timestamp-millis"}`** —
    populate via `Date.now()` in the producer. Don't store ISO strings.

## Producer side

15. **Producers must populate `eventId` (UUID) and `occurredAt` (epoch millis)
    on every domain event** — see `UserCreatedProducer.emit` for the shape.
    These fields enable consumer-side idempotency and traceability.
16. **`ClientKafka.connect()` in `onModuleInit`, `close()` in
    `onApplicationShutdown`.** Skipping shutdown leaks the underlying kafkajs
    producer and prevents `transactionTimeout` cleanup.
17. **Use `firstValueFrom(client.emit(...))`** to await the produce — `emit`
    returns an `Observable<RecordMetadata[]>`. Forgetting to await means errors
    surface as unhandled rejections.
18. **Producer is idempotent by default.** `producer: { idempotent: true }` is
    set in `kafka.module.ts`. Don't disable it — it's how we get exactly-once
    semantics within a single producer session.

## Consumer side

19. **Handlers must be idempotent.** Kafka guarantees at-least-once delivery;
    use `eventId` as a de-duplication key when storing side-effects.
20. **Always log `topic`, `partition`, `offset`, `eventId` on handler entry.**
    These four fields are the traceability quartet — `KafkaContext.getTopic()`,
    `getPartition()`, `getMessage().offset`, plus the decoded event.
21. **Never swallow exceptions in handlers.** Throwing surfaces to
    `KafkaRetryInterceptor` (which retries per `@KafkaRetry`) and then
    `KafkaDlqFilter` (DLQ + commit). Catching internally hides poison messages.
22. **Use `@KafkaRetry({ maxAttempts, backoffMs })` for transient failures** —
    network calls, downstream throttling. Skip it for pure compute. Without
    `@KafkaRetry`, `maxAttempts` defaults to 1 (no retry).
23. **One Nest app = one consumer group.** All `@EventPattern` handlers share
    `cfg.kafka.groupId`. Need per-feature scaling? Split into separate Nest
    microservice processes.

## DLQ contract

24. **DLQ topic is `<topic>.DLQ`** unless overridden via `@KafkaRetry({
    dlqTopic: '...' })`. The filter in
    [src/kafka/filters/kafka-dlq.filter.ts](src/kafka/filters/kafka-dlq.filter.ts)
    publishes original SR-framed bytes (no re-encode) so messages can be replayed.
25. **DLQ messages carry these headers** (don't change without coordinating
    downstream replay tooling):

    | Header | Meaning |
    |---|---|
    | `x-original-topic` | Source topic |
    | `x-original-partition` | Source partition |
    | `x-original-offset` | Source offset |
    | `x-error-name` | `PoisonPillError` / `HandlerExhaustedError` / `SchemaPayloadInvalidError` |
    | `x-error-message` | First 512 chars of error |
    | `x-attempts` | Handler attempts consumed |
    | `x-dlq-at` | ISO timestamp |

26. **`KafkaDlqFilter` registration is currently commented out** in
    [src/kafka/kafka.module.ts:65-68](src/kafka/kafka.module.ts) —
    re-enable it before relying on DLQ behavior in any non-dev environment.

## Pre-merge checklist (every Kafka PR)

- [ ] `.avsc` exists under `schemas/chorus/<domain>/<module>/`
- [ ] Topic constant defined in `*-events.types.ts`
- [ ] If schema is new: subject compatibility set to `FULL` after first register
- [ ] If schema is changed: relevant `kafka-evolution` decision tree row noted in PR
- [ ] Producer connects in `onModuleInit`, closes in `onApplicationShutdown`
- [ ] Handler logs the traceability quartet (`topic/partition/offset/eventId`)
- [ ] Handler is idempotent (uses `eventId` for de-dup if it has side effects)
- [ ] Unit test exercises happy path and at least one error path
- [ ] Integration test (Testcontainers) round-trips the event end-to-end
- [ ] No raw `console.log`; uses `Logger` or `nestjs-pino`
