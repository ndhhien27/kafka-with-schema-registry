# NestJS + Kafka + Schema Registry (Avro)

Production-shaped reference repo: NestJS 10 · kafkajs 2 · Confluent Schema Registry · Avro · pino · OpenTelemetry · Prometheus · Testcontainers.

## Features

- kafkajs client wrapped in a Nest `KafkaModule` (global)
- `@KafkaSubscribe(...)` decorator — methods become consumers discovered at boot
- Dead-letter topic routing with structured headers on poison pills and handler exhaustion
- Retry with configurable exponential backoff per handler
- Graceful shutdown: producer/consumers/admin drain on `SIGTERM`
- Schema Registry auto-registration of every `.avsc` under `schemas/` at startup (TopicNameStrategy)
- Avro encode/decode with cached schema IDs
- pino structured logging with redaction
- OpenTelemetry auto-instrumentation for kafkajs (opt-in via `OTEL_ENABLED=true`)
- Prometheus metrics at `/metrics` including custom `kafka_consumer_lag{topic,partition,group}` gauge
- `/health` endpoint with Kafka + Schema Registry indicators (Terminus)
- Unit tests (mocked), integration tests (Testcontainers), e2e health test
- GitHub Actions CI

## Quickstart

```bash
# 1. Boot infra (Kafka KRaft + Schema Registry + Connect + ksqlDB + REST Proxy + Flink)
docker compose up -d broker schema-registry

# 2. Install
pnpm install

# 3. Configure
cp .env.example .env

# 4. Run
pnpm start:dev

# 5. Produce an event via REST
curl -X POST http://localhost:3000/users \
  -H 'Content-Type: application/json' \
  -d '{"userId":"u-1","email":"alice@example.com","displayName":"Alice"}'

# 6. Watch the consumer log in the NestJS output:
#    UserCreated received id=... user=u-1 email=alice@example.com ...

# 7. Verify health + metrics
curl http://localhost:3000/health
curl http://localhost:3000/metrics | grep kafka_consumer_lag
```

## Scripts

| Command | What |
|---|---|
| `pnpm start:dev` | Watch-mode dev server |
| `pnpm build` | Compile TS |
| `pnpm test` | Unit tests |
| `pnpm test:cov` | Unit tests with coverage |
| `pnpm test:int` | Integration tests via Testcontainers |
| `pnpm test:e2e` | End-to-end (Testcontainers + full app) |
| `pnpm register:schemas` | Pre-register `.avsc` files in Schema Registry (CI) |

## Project Layout

```
src/
  config/          # Zod-validated env + AppConfigService
  schema-registry/ # SchemaRegistryService, auto-registration
  kafka/
    kafka.client.ts          # Kafka() factory (SASL/SSL aware)
    producer.service.ts      # idempotent producer, graceful shutdown
    consumer.registry.ts     # discovers @KafkaSubscribe, retry + DLQ
    admin.service.ts         # shared Admin client (used by lag gauge + health)
    decorators/              # @KafkaSubscribe
    interfaces/              # DecodedKafkaMessage, KafkaSubscribeOptions
    errors/                  # PoisonPillError, HandlerExhaustedError
  observability/
    tracing.ts               # OTel SDK bootstrap — imported first in main.ts
    logger.module.ts         # nestjs-pino
    metrics/                 # Prometheus + consumer-lag gauge
  health/                    # /health (Kafka + SR indicators)
  features/
    users/  user-created producer + consumer + REST controller
    orders/ order-placed consumer (DLQ demo)
schemas/
  user-created.avsc
  order-placed.avsc
test/
  unit/          # no containers needed
  integration/   # kafka.containers.ts spins KRaft Kafka + SR
  e2e/           # full-app health e2e
```

## Adding a new event

1. Drop `<topic-with-dashes>.avsc` in `schemas/`. Filename `foo-bar.avsc` → subject `foo.bar-value`.
2. Type the payload under `src/features/<your-feature>/`.
3. Producer: inject `ProducerService` and call `.produce<T>({ topic, key, value })`.
4. Consumer: add a method decorated with `@KafkaSubscribe({ topic, groupId })` on any provider — it's auto-discovered at boot.

## DLQ contract

A message lands on `<topic>.DLQ` (no Schema Registry encoding — original bytes preserved) with these headers:

| Header | Meaning |
|---|---|
| `x-original-topic` | Source topic |
| `x-original-partition` | Source partition |
| `x-original-offset` | Source offset |
| `x-error-name` | `PoisonPillError` or `HandlerExhaustedError` |
| `x-error-message` | First 512 chars of the error |
| `x-attempts` | Handler attempts consumed |
| `x-dlq-at` | ISO timestamp |

## Notes

- **OTel import order is load-bearing.** `src/main.ts` must `import './observability/tracing'` on its first line so instrumentation can patch kafkajs before it's loaded.
- The repo's `docker-compose.yml` includes extra Confluent services (Connect, ksqlDB, REST Proxy, Flink) — feel free to `docker compose up -d broker schema-registry` only.
- Integration tests require Docker. On Apple Silicon, make sure Docker is using the Linux virtualization backend.

See `docs/architecture.md` and `docs/produce-consume-flow.md` for diagrams.
