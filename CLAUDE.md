# CLAUDE.md — Project context for AI agents

This is a **NestJS 10 reference implementation** of the BTH Kafka Development
Guideline: Avro over Confluent Schema Registry, `Type.isValid` pre-encode
validation, FULL compatibility default, BTH topic naming, plus Orkes Conductor
integration. Use it as the canonical pattern for any Kafka producer / consumer /
schema / Orkes work in BTH services.

## Where to look first

| Need | Go to |
|---|---|
| **Domain rules / patterns** (Kafka, Orkes, schema evolution) | The `kafka-*` skills under `.claude/skills/` — start with `kafka` (router) |
| **Hard team rules** (must-not-violate) | `.claude/rules/` — `kafka-standards.md`, `orkes-standards.md`, `observability.md` |
| **Architecture diagrams** | [docs/architecture.md](docs/architecture.md), [docs/produce-consume-flow.md](docs/produce-consume-flow.md) |
| **Authoritative BTH spec** | [docs/kafka.md](docs/kafka.md) (transcribed BTH guideline) |
| **End-to-end Orkes walkthrough** | [docs/codelab.md](docs/codelab.md) (vendor onboarding codelab) |
| **Orkes training material** | [docs/orkes_training.md](docs/orkes_training.md) |
| **README / quickstart** | [README.md](README.md) |

## Skill router (when to invoke which skill)

Invoke the relevant skill **before** reading code or making changes:

| User intent | Skill |
|---|---|
| **Kafka / Avro / Schema Registry** | |
| Any Kafka/Avro/Schema work in this repo | `kafka` (master — routes to the right Kafka sub-skill) |
| Naming a topic, subject, `.avsc` file; authoring an Avro schema | `kafka-conventions` |
| Implementing or debugging a producer | `kafka-producer` |
| Implementing or debugging a consumer (`@EventPattern`) | `kafka-consumer` |
| Changing an existing schema (add/delete/rename field) | `kafka-evolution` |
| **Orkes Conductor** | |
| Any Orkes/Conductor/workflow work in this repo | `orkes` (master — routes to the right Orkes sub-skill) |
| Designing a workflow JSON (name, version, inputs, outputs, lifecycle) | `orkes-workflows` |
| Picking the right task type or configuring a specific task | `orkes-tasks` |
| Authoring or debugging an Event Handler (asyncComplete, `complete_task`, etc.) | `orkes-event-handlers` |
| Choosing or composing an architectural pattern (saga, fork-join, human-in-loop, etc.) | `orkes-patterns` |
| **Kafka ↔ Orkes integration** | |
| Wiring Kafka through Orkes (EVENT task with `_schema`, Apache Kafka Integration, BTH naming) | `kafka-orkes` |
| **Generic helpers** | |
| General codebase navigation | `explore-codebase` |
| Code review of unstaged changes | `review-changes` |
| Tracing a bug | `debug-issue` |
| Refactoring | `refactor-safely` |

Both master skills (`kafka` and `orkes`) include decision trees if you're
unsure which sub-skill applies. `kafka-orkes` is the specialized lens for
the **intersection** of Kafka and Orkes (Avro `_schema` on EVENT tasks); for
broader Orkes work, start with `orkes`.

## MCP Tools: code-review-graph

This project has a knowledge graph indexed by `code-review-graph`. **Use the
graph MCP tools BEFORE Grep/Glob/Read to explore the codebase** — they are
faster, cheaper, and give structural context (callers, dependents, test
coverage) that file scanning cannot.

| Use the graph for | Tool |
|---|---|
| Reviewing changes | `detect_changes` |
| Token-efficient code excerpts | `get_review_context` |
| Blast radius analysis | `get_impact_radius` |
| Affected execution paths | `get_affected_flows` |
| Tracing callers/callees/imports/tests | `query_graph` |
| Finding functions/classes by name or keyword | `semantic_search_nodes` |
| High-level structure | `get_architecture_overview` + `list_communities` |
| Planning renames or finding dead code | `refactor_tool` |

The graph auto-updates on file changes via the `PostToolUse` hook in
`.claude/settings.json`. Fall back to Grep/Glob/Read **only** when the graph
doesn't cover what you need.

### Token-efficient defaults

- Start any review/debug/refactor with `get_minimal_context(task="<your task>")`.
- Use `detail_level="minimal"` on graph calls; escalate to `"standard"` only when needed.
- Target: ≤5 graph calls and ≤800 output tokens for routine review/debug/refactor tasks.

## Quickstart

```bash
docker compose up -d                 # Kafka KRaft + Schema Registry
pnpm install
cp .env.example .env                 # set ORKES_* if you want the Orkes loop
pnpm start:dev

# Produce one UserCreated event
curl -X POST http://localhost:3000/users \
  -H 'Content-Type: application/json' \
  -d '{"userId":"u-1","email":"a@example.com","displayName":"Alice"}'

# Trigger the Orkes EVENT-publish workflow
curl -X POST http://localhost:3000/orkes/test-workflow \
  -H 'Content-Type: application/json' \
  -d '{"userId":"u-demo","amountCents":12345,"currency":"USD","sku":"SKU-DEMO"}'

# Health + lag metrics
curl http://localhost:3000/health
curl http://localhost:3000/metrics | grep kafka_consumer_lag
```

## Repo orientation

```
src/
  config/                # Zod-validated env + AppConfigService (incl. topics.build helper)
  schema-registry/       # SchemaRegistryService — recursive .avsc walk, FULL compatibility default
  kafka/
    serdes/              # AvroSerializer (Type.isValid pre-encode) + AvroDeserializer
    filters/             # KafkaDlqFilter, SchemaValidationExceptionFilter
    interceptors/        # KafkaRetryInterceptor
    decorators/          # @KafkaRetry({ maxAttempts, backoffMs, dlqTopic })
    errors/              # PoisonPillError, HandlerExhaustedError, SchemaPayloadInvalidError
    producer.service.ts  # raw kafkajs producer used by DLQ filter
    kafka.module.ts      # ClientKafka registration with AvroSerializer
  orkes/                 # OrkesModule, OrkesBootstrapService, OrkesController
  observability/         # tracing.ts (OTel — load order matters), pino logger, Prometheus
  health/                # /health (Kafka + SR indicators)
  features/
    users/               # UserCreated REST → producer → consumer
    orders/              # OrderPlaced consumer (with @KafkaRetry)
schemas/chorus/<domain>/<module>/  # .avsc files, auto-registered at boot
orkes/
  workflows/             # *.json — registered when ORKES_AUTO_REGISTER=true
  event_handlers/        # *.json — registered when ORKES_AUTO_REGISTER=true
test/{unit,integration,e2e}/
```

## Caveats — read before editing

1. **`KafkaDlqFilter` is currently commented out** in `src/kafka/kafka.module.ts`
   (only `SchemaValidationExceptionFilter` is registered). DLQ routing won't fire
   until you uncomment that `APP_FILTER`. Producer-side `SchemaPayloadInvalidError`
   still surfaces as HTTP 400 via the schema-validation filter.
2. **`KafkaRetryInterceptor` defaults to `maxAttempts: 1`** (no retry) — handlers
   only retry when they explicitly opt in via `@KafkaRetry({ maxAttempts: N, ... })`.
3. **Topic constants in `src/features/*/[feature]-events.types.ts` are hardcoded**
   to `one-bth-dev-*` strings rather than built via `AppConfigService.topics.build()`.
   That means changing `KAFKA_TOPIC_ENV` does **not** rename the topics those
   features subscribe to. Treat this as a known bootstrap shortcut to fix before
   any non-dev deploy.
4. **Demo `.avsc` files** under `schemas/chorus/...` use record names like
   `UserCreated` / namespace `com.example.events` — **not** the BTH-spec
   `one_bth_dev_user_created_in_private` snake-case form. This is a deliberate
   demo deviation; production schemas should follow the BTH conventions documented
   in the `kafka-conventions` skill.
5. **OTel import order is load-bearing.** `src/main.ts` must `import './observability/tracing'`
   on its first line so instrumentation can patch `kafkajs` before it's loaded.
6. **SASL fields** (`KAFKA_SASL_*` in `env.schema.ts`) are dormant — present in
   the schema but not wired into the kafkajs client today.
