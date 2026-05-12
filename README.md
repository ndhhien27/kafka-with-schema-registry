# NestJS + Kafka + Schema Registry (Avro) + Orkes Conductor

Reference repo aligned with the BTH Kafka Development Guideline:
NestJS 10 microservices · `@confluentinc/schemaregistry` · Avro `Type.isValid`
pre-encode validation · `FULL` compatibility default · `{ORG}-{APP}-{ENV}-{FEATURE}-{TYPE}`
topic naming · Orkes Conductor cloud-trial integration · pino · OpenTelemetry · Prometheus
· Testcontainers.

## Features

- Hybrid Nest app — HTTP gateway + `Transport.KAFKA` microservice in one process
- `@EventPattern(topic)` consumers discovered automatically as Nest controllers
- `ClientKafka` producer wired through `ClientsModule.registerAsync` with a custom `AvroSerializer`
- Schema Registry auto-registration of every `.avsc` under `schemas/` at startup, walked recursively
- **Client-side `Type.isValid` pre-encode validation** — payloads that don't match the local Avro schema fail fast with field paths (BTH guideline: "App-side validation during serialization")
- **`FULL` compatibility** applied to every subject after registration (configurable via `SCHEMA_COMPATIBILITY`)
- **BTH topic naming** `{ORG}-{APP}-{ENV}-{FEATURE}-{TYPE}` driven by env (`KAFKA_TOPIC_ORG`, `KAFKA_TOPIC_APP`, `KAFKA_TOPIC_ENV`)
- **Orkes Conductor integration** — workflow + event-handler definitions in `orkes/`, REST endpoints to start/inspect, gated by `ORKES_ENABLED`
- Global `KafkaDlqFilter` routes any uncaught handler error to `<topic>.DLQ` preserving original SR-framed bytes
- Global `KafkaRetryInterceptor` honors per-handler `@KafkaRetry({ maxAttempts, backoffMs })`
- Graceful shutdown: HTTP + microservice + producer/admin drain on `SIGTERM`
- pino structured logging, OTel auto-instrumentation, Prometheus metrics, Terminus health
- Unit tests (mocked), integration tests (Testcontainers), e2e health test

## Quickstart

```bash
# 1. Boot infra (Kafka KRaft + Schema Registry)
docker compose up -d

# 2. Install
pnpm install

# 3. Configure
cp .env.example .env
# Optional: set ORKES_ENABLED=true and fill in ORKES_SERVER_URL / ORKES_KEY / ORKES_SECRET

# 4. Run
pnpm start:dev

# 5. Produce an event via REST (publishes to `one-bth-dev-user-created-in-private`)
curl -X POST http://localhost:3000/users \
  -H 'Content-Type: application/json' \
  -d '{"userId":"u-1","email":"alice@example.com","displayName":"Alice"}'

# 6. Watch the consumer log:
#    UserCreated received id=... user=u-1 email=alice@example.com ...

# 7. Verify health + metrics
curl http://localhost:3000/health
curl http://localhost:3000/metrics | grep kafka_consumer_lag
```

> The full Confluent stack (Connect, ksqlDB, Control Center, Flink, REST Proxy, Prometheus,
> Alertmanager) is preserved in `docker-compose.full.yml`. Use
> `docker compose -f docker-compose.full.yml up -d` when you need it.

## Scripts

| Command | What |
|---|---|
| `pnpm start:dev` | Watch-mode dev server |
| `pnpm build` | Compile TS |
| `pnpm test` | Unit tests |
| `pnpm test:cov` | Unit tests with coverage |
| `pnpm test:int` | Integration tests via Testcontainers |
| `pnpm test:e2e` | End-to-end |
| `pnpm register:schemas` | Pre-register `.avsc` files in Schema Registry (one-shot) |

## Project Layout

```
src/
  config/          # Zod-validated env + AppConfigService (incl. topics.build helper)
  schema-registry/ # SchemaRegistryService — recursive .avsc walk, FULL compatibility default
  kafka/
    serdes/        # AvroSerializer (with Type.isValid pre-encode) + AvroDeserializer
    filters/       # KafkaDlqFilter, SchemaValidationExceptionFilter
    interceptors/  # KafkaRetryInterceptor
    decorators/    # @KafkaRetry({ maxAttempts, backoffMs, dlqTopic })
    errors/        # PoisonPillError, HandlerExhaustedError
  orkes/           # OrkesModule, OrkesBootstrapService, OrkesController
  observability/
    tracing.ts     # OTel SDK bootstrap — imported first in main.ts
    logger.module.ts
    metrics/       # Prometheus + consumer-lag gauge
  health/          # /health (Kafka + SR indicators)
  features/
    users/  user-created producer + consumer + REST controller
    orders/ order-placed consumer (DLQ + retry demo)
schemas/
  chorus/
    users/profile/one_bth_dev_user_created_in_private.avsc
    orders/checkout/one_bth_dev_order_placed_in_private.avsc
orkes/
  workflows/
    kafka_demo_workflow.json           # INLINE -> EVENT (publish) workflow
    kafka_demo_consumer_workflow.json  # echo workflow started by event handler
  event_handlers/
    order_placed_handler.json          # Kafka event handler that starts the consumer workflow
test/
  unit/          # no containers needed
  integration/   # kafka.containers.ts spins KRaft Kafka + SR
  e2e/           # full-app health e2e
```

## Adding a new event

1. Drop `<topic_with_underscores>.avsc` under `schemas/chorus/<domain>/<module>/`. Filename
   `one_bth_dev_user_created_in_private.avsc` -> topic `one-bth-dev-user-created-in-private`
   -> subject `one-bth-dev-user-created-in-private-value`.
2. Type the payload under `src/features/<your-feature>/`. Define a topic constant in `*-events.types.ts`.
3. **Producer:** inject `@Inject(KAFKA_CLIENT_PRODUCER) client: ClientKafka` and call
   `client.emit(topic, { key, value })`. The `AvroSerializer` runs `Type.isValid` and SR encode.
4. **Consumer:** create a `@Controller()` with a method decorated `@EventPattern('<topic>')`.
   List it under `controllers: []` of your feature module. Optionally add
   `@KafkaRetry({ maxAttempts: 3, backoffMs: [100, 300, 800] })` for retry policy.
   On exhaustion the message lands on `<topic>.DLQ`.

## Topic naming convention (BTH guideline)

```
{ORG}-{APP}-{ENV}-{FEATURE}-{TYPE}
```

Example: `one-bth-dev-user-created-in-private` (defaults bake `ORG=one`, `APP=bth`, `ENV=dev`).
Override per environment via `KAFKA_TOPIC_ORG`, `KAFKA_TOPIC_APP`, `KAFKA_TOPIC_ENV`.

The `.avsc` filename mirrors the topic with underscores instead of dashes
(`one_bth_dev_user_created_in_private.avsc`). `SchemaRegistryService` derives the subject
by stripping the path, replacing `_` with `-`, and appending `-value`.

## Schema compatibility

`SchemaRegistryService` calls `client.updateCompatibility(subject, level)` on every subject
after registration. Default level is `FULL` (BTH guideline: only optional add/delete is safe
without a coordinated rollout). Override with `SCHEMA_COMPATIBILITY`.

## DLQ contract

A message lands on `<topic>.DLQ` (no Schema Registry encoding — original bytes preserved) with these headers:

| Header | Meaning |
|---|---|
| `x-original-topic` | Source topic |
| `x-original-partition` | Source partition |
| `x-original-offset` | Source offset |
| `x-error-name` | `PoisonPillError`, `HandlerExhaustedError`, or `SchemaPayloadInvalidError` |
| `x-error-message` | First 512 chars of the error |
| `x-attempts` | Handler attempts consumed |
| `x-dlq-at` | ISO timestamp |

## Orkes Conductor (cloud trial)

1. Sign up at https://developer.orkescloud.com and create an Application key/secret.
2. Set in `.env`:
   ```
   ORKES_ENABLED=true
   ORKES_SERVER_URL=https://developer.orkescloud.com/api
   ORKES_KEY=<your key id>
   ORKES_SECRET=<your key secret>
   ORKES_AUTO_REGISTER=true
   ```
3. **Make your local Kafka reachable from Orkes Cloud.** Cloud Conductor needs to talk to your
   broker. Use a tunnel (ngrok / cloudflared / Tailscale Funnel) and configure a Confluent
   integration in the Orkes UI pointing at the tunnel URL. The integration name in the
   workflow's `EVENT` sink (`kafka:...`) must match the integration you configure on the Orkes
   side.
4. Boot the app — `OrkesBootstrapService` registers `kafka_demo_workflow`,
   `kafka_demo_consumer_workflow`, and `order_placed_handler` on startup.
5. Trigger the demo:
   ```bash
   curl -X POST http://localhost:3000/orkes/test-workflow \
     -H 'Content-Type: application/json' \
     -d '{"userId":"u-demo","amountCents":12345,"currency":"USD","sku":"SKU-DEMO"}'
   # -> { "status":"accepted", "workflowId":"<uuid>" }
   ```
6. Inspect the workflow:
   ```bash
   curl http://localhost:3000/orkes/workflow/<workflowId>
   ```
   Or open the run in the Orkes UI to see the INLINE -> EVENT chain.

> **Compatibility caveat**: per the BTH guideline §5, Orkes only resolves the latest schema
> version, so changes pushed via Orkes Event tasks effectively bypass `FULL` compatibility.
> Treat any schema change touched by an Orkes path with the same care as `NONE`.

## Hybrid mode and consumer groups

All `@EventPattern` handlers in this single Nest app share one consumer-group identity
(`KAFKA_GROUP_ID`). Split into separate Nest microservice processes if you need per-feature
group ids.

## Notes

- **OTel import order is load-bearing.** `src/main.ts` must `import './observability/tracing'`
  on its first line so instrumentation can patch kafkajs before it's loaded.
- SASL fields (`KAFKA_SASL_*`) are present in `env.schema.ts` but **not wired** into the
  kafkajs client — preserved so SASL_SSL can be re-enabled without an env-schema migration.
- Integration tests require Docker. On Apple Silicon, make sure Docker is using the Linux
  virtualization backend.

See `docs/architecture.md` and `docs/produce-consume-flow.md` for diagrams.
