# Kafka master skill — reference

Detailed reference loaded only when SKILL.md tells you to consult it. Keeps the
master skill body short while making everything you need lookable.

## Glossary

| Term | Definition |
|---|---|
| **Avro** | Schema-first binary serialization. Schema in JSON (`.avsc`), payload in compact binary. |
| **Schema Registry (SR)** | Centralized service that stores versioned schemas per subject and enforces compatibility on register. Confluent's reference impl. |
| **Subject** | A namespace under which schemas are versioned. Default strategy: `<topic>-value`. |
| **TopicNameStrategy** | Subject = `<topic>-value` (or `<topic>-key` for keys). The class on the broker side is `io.confluent.kafka.serializers.subject.TopicNameStrategy`. |
| **SR-framed bytes** | 5-byte prefix (`0x00` magic + 4-byte BE schema id) + Avro-binary payload. |
| **Compatibility mode** | Per-subject policy — `BACKWARD` / `FORWARD` / `FULL` / `NONE` (and their `_TRANSITIVE` variants). Determines what schema changes are accepted. |
| **DLQ** | Dead-letter queue. This repo uses `<topic>.DLQ` for failed messages with original SR-framed bytes preserved. |
| **Poison pill** | A message that cannot be deserialized (bad magic byte, unknown schema id). Routed to DLQ without consuming retry attempts. |
| **BTH** | The internal team / spec that this repo implements. Naming, compatibility defaults, and Orkes integration follow the BTH Kafka Development Guideline (transcribed in [docs/kafka.md](docs/kafka.md)). |
| **Chorus** | The schema namespace tree (`schemas/chorus/<domain>/<module>/`) — mirrors the upstream `om-schema-registry` repo. |
| **`@EventPattern`** | NestJS microservices decorator for Kafka event consumers. Subscribed at app boot. |
| **`@KafkaRetry`** | This repo's per-handler retry policy decorator (`maxAttempts`, `backoffMs`, `dlqTopic`). |
| **Orkes Conductor** | Workflow orchestration engine. Reads/writes Kafka via Apache Kafka Integration. Always uses **latest** schema version. |

## File map by concern

### Configuration

| File | Purpose |
|---|---|
| [src/config/env.schema.ts](src/config/env.schema.ts) | Zod schema for all env vars; `validateEnv` throws on bad config. |
| [src/config/app-config.service.ts](src/config/app-config.service.ts) | Typed accessors: `cfg.kafka`, `cfg.topics`, `cfg.schemaRegistry`, `cfg.orkes`, etc. |
| [src/config/config.module.ts](src/config/config.module.ts) | Wires Nest's `ConfigModule` with the validator. |
| [.env.example](.env.example) | Reference values for every env var. |

### Schema Registry

| File | Purpose |
|---|---|
| [src/schema-registry/schema-registry.service.ts](src/schema-registry/schema-registry.service.ts) | Recursive `.avsc` walk → `client.register` → `updateCompatibility(FULL)`. Provides `encode`/`decode`/`getSchemaTextForTopic`. |
| [src/schema-registry/schema-registry.module.ts](src/schema-registry/schema-registry.module.ts) | Provides the `@confluentinc/schemaregistry` `Client` and the service. |
| [scripts/register-schemas.ts](scripts/register-schemas.ts) | One-shot CLI: `pnpm register:schemas`. Use for CD when you don't want boot-time registration. |

### Kafka transport

| File | Purpose |
|---|---|
| [src/kafka/kafka.module.ts](src/kafka/kafka.module.ts) | `ClientsModule.registerAsync` for `KAFKA_CLIENT_PRODUCER`; binds `KafkaRetryInterceptor` and `SchemaValidationExceptionFilter` globally. |
| [src/kafka/kafka.tokens.ts](src/kafka/kafka.tokens.ts) | DI tokens: `KAFKA_CLIENT` (Symbol), `KAFKA_CLIENT_PRODUCER` (string). |
| [src/kafka/kafka.client.ts](src/kafka/kafka.client.ts) | Factory for the raw `kafkajs` `Kafka` instance — used by `ProducerService` and `KafkaAdminService`. |
| [src/kafka/producer.service.ts](src/kafka/producer.service.ts) | Raw kafkajs producer with `.produce({ raw: true })` for the DLQ path. Idempotent + GZIP compressed. |
| [src/kafka/admin.service.ts](src/kafka/admin.service.ts) | Wraps kafkajs `Admin` for `fetchOffsets` / `describeGroups` (used by lag gauge + health). |
| [src/main.ts](src/main.ts) | `connectMicroservice<MicroserviceOptions>` with `Transport.KAFKA`, plus the AvroSerializer/Deserializer pair. |

### Serdes

| File | Purpose |
|---|---|
| [src/kafka/serdes/avro.serializer.ts](src/kafka/serdes/avro.serializer.ts) | `Type.isValid` pre-encode → SR encode → log schema id + bytes. |
| [src/kafka/serdes/avro.deserializer.ts](src/kafka/serdes/avro.deserializer.ts) | Reads `0x00` magic, calls `sr.decode`, returns `{ pattern, data }` packet. |
| [src/kafka/serdes/sr-errors.ts](src/kafka/serdes/sr-errors.ts) | `isSchemaRegistryError`, `isSchemaRegistryValidationError`, `extractValidationPaths`, `SchemaPayloadInvalidError`. |

### Reliability

| File | Purpose |
|---|---|
| [src/kafka/decorators/kafka-retry.decorator.ts](src/kafka/decorators/kafka-retry.decorator.ts) | `@KafkaRetry({ maxAttempts, backoffMs, dlqTopic })` — sets metadata. |
| [src/kafka/interceptors/kafka-retry.interceptor.ts](src/kafka/interceptors/kafka-retry.interceptor.ts) | Reads metadata, retries with backoff, throws `HandlerExhaustedError`. Default `maxAttempts=1`. |
| [src/kafka/filters/kafka-dlq.filter.ts](src/kafka/filters/kafka-dlq.filter.ts) | Catches handler exceptions → publishes raw bytes to `<topic>.DLQ` with structured headers. **Currently commented out in `kafka.module.ts`.** |
| [src/kafka/filters/schema-validation.filter.ts](src/kafka/filters/schema-validation.filter.ts) | HTTP-side: maps SR `SerializationError` to `400 Bad Request` with `paths`. |
| [src/kafka/errors/poison-pill.error.ts](src/kafka/errors/poison-pill.error.ts) | `PoisonPillError`, `HandlerExhaustedError`. |

### Features (reference implementations)

| File | Purpose |
|---|---|
| [src/features/users/user-events.types.ts](src/features/users/user-events.types.ts) | `USER_CREATED_TOPIC` constant + `UserCreatedEvent` interface. |
| [src/features/users/user-created.producer.ts](src/features/users/user-created.producer.ts) | `ClientKafka.emit` + `firstValueFrom` pattern. Generates `eventId` + `occurredAt`. |
| [src/features/users/user-created.consumer.ts](src/features/users/user-created.consumer.ts) | Plain `@EventPattern` consumer (no retry, no side effects). |
| [src/features/users/users.controller.ts](src/features/users/users.controller.ts) | REST `POST /users` → producer.emit → returns `{eventId}`. |
| [src/features/users/users.module.ts](src/features/users/users.module.ts) | Wires controller + producer + consumer. |
| [src/features/orders/order-events.types.ts](src/features/orders/order-events.types.ts) | `ORDER_PLACED_TOPIC` + `OrderPlacedEvent` (with `items[]`). |
| [src/features/orders/order-placed.consumer.ts](src/features/orders/order-placed.consumer.ts) | Consumer with `@KafkaRetry({ maxAttempts: 3, backoffMs: [100, 300, 800] })` and a deliberate failure path. |

### Orkes

| File | Purpose |
|---|---|
| [src/orkes/orkes.module.ts](src/orkes/orkes.module.ts) | Builds `OrkesClients` from env (returns `null` when disabled or misconfigured). |
| [src/orkes/orkes.tokens.ts](src/orkes/orkes.tokens.ts) | `ORKES_CLIENTS` Symbol token. |
| [src/orkes/orkes-bootstrap.service.ts](src/orkes/orkes-bootstrap.service.ts) | Reads `orkes/workflows/*.json` and `orkes/event_handlers/*.json`, registers via `metadata.registerWorkflowDef(def, true)` and upsert-by-name for handlers. |
| [src/orkes/orkes.controller.ts](src/orkes/orkes.controller.ts) | `POST /orkes/test-workflow`, `GET /orkes/workflow/:id`. |
| [orkes/workflows/kafka_demo_workflow.json](orkes/workflows/kafka_demo_workflow.json) | Producer-side workflow: INLINE → EVENT publish. |
| [orkes/workflows/kafka_demo_consumer_workflow.json](orkes/workflows/kafka_demo_consumer_workflow.json) | Consumer-side workflow: echo INLINE task. |
| [orkes/event_handlers/order_placed_handler.json](orkes/event_handlers/order_placed_handler.json) | Listens on `kafka:<topic>:orkes-demo-handler`, starts the consumer workflow. |

### Observability

| File | Purpose |
|---|---|
| [src/observability/tracing.ts](src/observability/tracing.ts) | OTel SDK bootstrap — must be imported first in `main.ts`. |
| [src/observability/logger.module.ts](src/observability/logger.module.ts) | `nestjs-pino` setup. |
| [src/observability/metrics/](src/observability/metrics/) | Prometheus registry + `kafka_consumer_lag` gauge. |
| [src/health/](src/health/) | Terminus indicators for Kafka + SR. |

## DI tokens

| Token | Type | Where |
|---|---|---|
| `KAFKA_CLIENT` | `Symbol` (raw kafkajs `Kafka`) | [src/kafka/kafka.tokens.ts](src/kafka/kafka.tokens.ts) |
| `KAFKA_CLIENT_PRODUCER` | `string` (NestJS `ClientKafka`) | [src/kafka/kafka.tokens.ts](src/kafka/kafka.tokens.ts) |
| `SCHEMA_REGISTRY_CLIENT` | `Symbol` (`@confluentinc/schemaregistry` Client) | [src/schema-registry/schema-registry.service.ts:14](src/schema-registry/schema-registry.service.ts) |
| `ORKES_CLIENTS` | `Symbol` (`OrkesClients` or `null`) | [src/orkes/orkes.tokens.ts](src/orkes/orkes.tokens.ts) |
| `KAFKA_RETRY_METADATA` | `string` metadata key for `@KafkaRetry` | [src/kafka/decorators/kafka-retry.decorator.ts](src/kafka/decorators/kafka-retry.decorator.ts) |

## Environment variables (full reference)

Defined in [src/config/env.schema.ts](src/config/env.schema.ts).

| Var | Default | Notes |
|---|---|---|
| `NODE_ENV` | `development` | `development` / `test` / `production` |
| `HTTP_PORT` | `3000` | REST + `/metrics` + `/health` |
| `LOG_LEVEL` | `info` | pino levels |
| `KAFKA_BROKERS` | (required) | Comma-separated broker list |
| `KAFKA_CLIENT_ID` | `nestjs-kafka-avro` | Suffixed with `-producer` / `-microservice` per role |
| `KAFKA_GROUP_ID` | `nestjs-kafka-avro-group` | Shared by every `@EventPattern` in this app |
| `KAFKA_SSL` | `false` | Boolean parsed from string |
| `KAFKA_SASL_MECHANISM` / `_USERNAME` / `_PASSWORD` | (unset) | **Dormant** — not wired into the kafkajs client today |
| `KAFKA_TOPIC_ORG` / `_APP` / `_ENV` | `one` / `bth` / `dev` | Threaded into `topics.build(feature, type)` |
| `SCHEMA_REGISTRY_URL` | (required) | e.g. `http://localhost:8081` |
| `SCHEMA_REGISTRY_USER` / `_PASS` | (unset) | Basic auth for SR |
| `SCHEMA_COMPATIBILITY` | `FULL` | Applied to every subject after register |
| `OTEL_ENABLED` | `false` | Toggle export only — patching always runs |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318` | OTLP/HTTP |
| `OTEL_SERVICE_NAME` | `nestjs-kafka-avro` | Stable across envs |
| `METRICS_PORT` | `9464` | Reserved for future split; today `/metrics` is on `HTTP_PORT` |
| `CONSUMER_LAG_POLL_MS` | `15000` | Poll interval for the lag gauge |
| `ORKES_ENABLED` | `false` | Master toggle for the Orkes module |
| `ORKES_SERVER_URL` | (unset) | e.g. `https://developer.orkescloud.com/api` |
| `ORKES_KEY` / `ORKES_SECRET` | (unset) | Application key/secret from Orkes UI |
| `ORKES_AUTO_REGISTER` | `false` | Walk `orkes/` and register workflows + handlers at boot |

## DLQ headers (full reference)

Set by [src/kafka/filters/kafka-dlq.filter.ts](src/kafka/filters/kafka-dlq.filter.ts).
Don't change without coordinating with downstream replay tooling.

| Header | Type | Example |
|---|---|---|
| `x-original-topic` | string | `one-bth-dev-order-placed-in-private` |
| `x-original-partition` | string | `2` |
| `x-original-offset` | string | `4815162342` |
| `x-error-name` | string | `PoisonPillError` / `HandlerExhaustedError` / `SchemaPayloadInvalidError` |
| `x-error-message` | string | First 512 chars of `error.message` |
| `x-attempts` | string | Handler attempts consumed (1 if retry not configured) |
| `x-dlq-at` | string | ISO 8601 timestamp |

## Useful commands

| What | Command |
|---|---|
| Start infra (slim) | `docker compose up -d` |
| Start infra (full Confluent stack) | `docker compose -f docker-compose.full.yml up -d` |
| Dev server | `pnpm start:dev` |
| One-shot schema register | `pnpm register:schemas` |
| Unit tests | `pnpm test` |
| Coverage | `pnpm test:cov` |
| Integration (Testcontainers) | `pnpm test:int` |
| E2E | `pnpm test:e2e` |
| List topics | `docker compose exec kafka kafka-topics --list --bootstrap-server kafka:9092` |
| Tail a topic | `docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic one-bth-dev-user-created-in-private --from-beginning` |
| Inspect a subject | `curl http://localhost:8081/subjects/one-bth-dev-user-created-in-private-value/versions/latest \| jq` |
| Set a compatibility mode | `curl -X PUT http://localhost:8081/config/<subject> -H 'Content-Type: application/vnd.schemaregistry.v1+json' -d '{"compatibility":"FORWARD"}'` |
