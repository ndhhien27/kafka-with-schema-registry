# Kafka master skill — examples

End-to-end walkthroughs. Loaded only when SKILL.md says "see examples.md".

## Example 1 — Add a new event end-to-end (`PaymentSettled`)

This walks through the full sequence to add a new event without touching Orkes.
All file paths are relative to repo root. Imagine a `payments` feature.

### Step 1 — Author the `.avsc`

Create `schemas/chorus/payments/settlement/one_bth_dev_payment_settled_in_private.avsc`:

```json
{
  "type": "record",
  "name": "PaymentSettled",
  "namespace": "com.example.events",
  "doc": "Emitted when a payment is finalized. Topic: one-bth-dev-payment-settled-in-private.",
  "fields": [
    { "name": "eventId",     "type": "string",
      "doc": "UUID of this event." },
    { "name": "occurredAt",  "type": { "type": "long", "logicalType": "timestamp-millis" } },
    { "name": "paymentId",   "type": "string" },
    { "name": "userId",      "type": "string" },
    { "name": "amountCents", "type": "long" },
    { "name": "currency",    "type": "string", "default": "USD" },
    { "name": "memo",        "type": ["null", "string"], "default": null }
  ]
}
```

The filename → topic → subject derivation is automatic via
[`subjectForFile`](src/schema-registry/schema-registry.service.ts):

```
one_bth_dev_payment_settled_in_private.avsc
  → topic   one-bth-dev-payment-settled-in-private
  → subject one-bth-dev-payment-settled-in-private-value
```

> **Note** the demo schemas in this repo use `name: "PaymentSettled"` /
> `namespace: "com.example.events"` — for production schemas, follow the BTH
> convention (`name: "one_bth_payment_settled_in_private"`) — see
> `kafka-conventions`.

### Step 2 — Topic constant + types

Create `src/features/payments/payment-events.types.ts`:

```ts
export interface PaymentSettledEvent {
  eventId: string;
  occurredAt: number;
  paymentId: string;
  userId: string;
  amountCents: number;
  currency: string;
  memo: string | null;
}

export const PAYMENT_SETTLED_TOPIC = 'one-bth-dev-payment-settled-in-private';
```

### Step 3 — Producer

Create `src/features/payments/payment-settled.producer.ts`:

```ts
import { Inject, Injectable, OnApplicationShutdown, OnModuleInit } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { firstValueFrom } from 'rxjs';
import { randomUUID } from 'crypto';
import { KAFKA_CLIENT_PRODUCER } from '../../kafka/kafka.tokens';
import { PAYMENT_SETTLED_TOPIC, PaymentSettledEvent } from './payment-events.types';

export interface SettlePaymentInput {
  paymentId: string;
  userId: string;
  amountCents: number;
  currency?: string;
  memo?: string | null;
}

@Injectable()
export class PaymentSettledProducer implements OnModuleInit, OnApplicationShutdown {
  constructor(@Inject(KAFKA_CLIENT_PRODUCER) private readonly client: ClientKafka) {}

  async onModuleInit(): Promise<void> { await this.client.connect(); }
  async onApplicationShutdown(): Promise<void> { await this.client.close(); }

  async emit(input: SettlePaymentInput): Promise<PaymentSettledEvent> {
    const event: PaymentSettledEvent = {
      eventId: randomUUID(),
      occurredAt: Date.now(),
      paymentId: input.paymentId,
      userId: input.userId,
      amountCents: input.amountCents,
      currency: input.currency ?? 'USD',
      memo: input.memo ?? null,
    };
    await firstValueFrom(
      this.client.emit(PAYMENT_SETTLED_TOPIC, { key: input.paymentId, value: event }),
    );
    return event;
  }
}
```

### Step 4 — Consumer

Create `src/features/payments/payment-settled.consumer.ts`:

```ts
import { Controller, Logger } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { KafkaRetry } from '../../kafka/decorators/kafka-retry.decorator';
import { PAYMENT_SETTLED_TOPIC, PaymentSettledEvent } from './payment-events.types';

@Controller()
export class PaymentSettledConsumer {
  private readonly logger = new Logger(PaymentSettledConsumer.name);

  @EventPattern(PAYMENT_SETTLED_TOPIC)
  @KafkaRetry({ maxAttempts: 3, backoffMs: [200, 800, 2000] })
  async handle(@Payload() value: PaymentSettledEvent, @Ctx() ctx: KafkaContext): Promise<void> {
    const partition = ctx.getPartition();
    const offset = ctx.getMessage().offset;
    this.logger.log(
      `PaymentSettled id=${value.eventId} payment=${value.paymentId}` +
        ` user=${value.userId} amount=${value.amountCents} ${value.currency}` +
        ` topic=${PAYMENT_SETTLED_TOPIC} partition=${partition} offset=${offset}`,
    );
    // ... persist, charge ledger, etc. — must be idempotent on eventId
  }
}
```

### Step 5 — REST controller (optional)

If the new event is triggered by an HTTP request, add
`src/features/payments/payments.controller.ts`:

```ts
import { Body, Controller, HttpCode, Post } from '@nestjs/common';
import { PaymentSettledProducer, SettlePaymentInput } from './payment-settled.producer';

@Controller('payments')
export class PaymentsController {
  constructor(private readonly producer: PaymentSettledProducer) {}

  @Post('settle')
  @HttpCode(202)
  async settle(@Body() body: SettlePaymentInput) {
    const event = await this.producer.emit(body);
    return { status: 'accepted', eventId: event.eventId };
  }
}
```

### Step 6 — Module

Create `src/features/payments/payments.module.ts`:

```ts
import { Module } from '@nestjs/common';
import { PaymentsController } from './payments.controller';
import { PaymentSettledProducer } from './payment-settled.producer';
import { PaymentSettledConsumer } from './payment-settled.consumer';

@Module({
  controllers: [PaymentsController, PaymentSettledConsumer],
  providers: [PaymentSettledProducer],
  exports: [PaymentSettledProducer],
})
export class PaymentsModule {}
```

Add `PaymentsModule` to the `imports` array of [src/app.module.ts](src/app.module.ts).

### Step 7 — Verify the loop

```bash
pnpm start:dev
# Boot logs should include:
#   Registered chorus/payments/settlement/one_bth_dev_payment_settled_in_private.avsc
#     -> subject=one-bth-dev-payment-settled-in-private-value id=<n>
#   Set compatibility FULL on subject=one-bth-dev-payment-settled-in-private-value

# Trigger
curl -X POST http://localhost:3000/payments/settle \
  -H 'Content-Type: application/json' \
  -d '{"paymentId":"p-1","userId":"u-1","amountCents":1234}'

# Watch the consumer log:
#   PaymentSettled id=<uuid> payment=p-1 user=u-1 amount=1234 USD topic=... partition=... offset=...
```

### Step 8 — Tests

Add at minimum:
- A unit test for the producer that mocks `ClientKafka.emit` and asserts the
  topic + payload shape (see [test/unit/producer.service.spec.ts](test/unit/producer.service.spec.ts)
  for the pattern).
- An integration test (Testcontainers) that round-trips one event through
  Kafka + SR (see [test/integration/](test/integration/)).

## Example 2 — Verify the existing UserCreated loop

```bash
docker compose up -d
pnpm install && cp .env.example .env
pnpm start:dev

# In another shell
curl -X POST http://localhost:3000/users \
  -H 'Content-Type: application/json' \
  -d '{"userId":"u-1","email":"alice@example.com","displayName":"Alice"}'

# Expected console output (single line per side):
#   [AvroSerializer]  Avro serialize topic=one-bth-dev-user-created-in-private schemaId=<n> hasKey=true bytes=<n> ...
#   [AvroDeserializer] Avro deserialize topic=one-bth-dev-user-created-in-private schemaId=<n> framed=true hasKey=true ...
#   [UserCreatedConsumer] UserCreated received id=<uuid> user=u-1 email=alice@example.com partition=0 offset=<n>
```

## Example 3 — Trigger a DLQ via a poison pill

```bash
# Manually publish raw JSON to a topic that has an Avro subject
docker compose exec kafka kafka-console-producer \
  --bootstrap-server kafka:9092 \
  --topic one-bth-dev-order-placed-in-private \
< <(echo '{"not":"avro"}')

# Expected (when KafkaDlqFilter is uncommented in kafka.module.ts):
#   [AvroDeserializer] SR decode failed on topic=...: Unknown magic byte!
#   [KafkaDlqFilter]   Routed message to DLQ topic=one-bth-dev-order-placed-in-private.DLQ ...

# Tail the DLQ
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic one-bth-dev-order-placed-in-private.DLQ \
  --from-beginning --property print.headers=true
# Expect headers x-original-topic, x-error-name=PoisonPillError, x-attempts, x-dlq-at, etc.
```

## Example 4 — Trigger a `HandlerExhaustedError`

The order consumer is wired to throw on negative `amountCents`:

```ts
// src/features/orders/order-placed.consumer.ts
if (value.amountCents < 0) {
  throw new Error(`invalid amountCents=${value.amountCents} for orderId=${value.orderId}`);
}
```

To exercise it via the Orkes loop, send a negative amount:

```bash
curl -X POST http://localhost:3000/orkes/test-workflow \
  -H 'Content-Type: application/json' \
  -d '{"userId":"u-1","amountCents":-1,"currency":"USD","sku":"SKU-X"}'

# Expected: 3 retry attempts (per @KafkaRetry({ maxAttempts: 3, backoffMs: [100, 300, 800] })),
# then HandlerExhaustedError → DLQ (when KafkaDlqFilter is enabled).
```

## Example 5 — Trigger a producer-side validation rejection

`POST /users` with a missing required field hits `Type.isValid` before SR
encode and is mapped to `400 Bad Request` by `SchemaValidationExceptionFilter`:

```bash
curl -i -X POST http://localhost:3000/users \
  -H 'Content-Type: application/json' \
  -d '{"userId":"u-1"}'    # missing email

# HTTP/1.1 400 Bad Request
# {"statusCode":400,"error":"Bad Request","message":"Payload does not match the registered schema",
#  "reason":"Invalid message at email, expected \"string\", got undefined","paths":[["email"]]}
```

Note the producer-side `Type.isValid` check throws `SchemaPayloadInvalidError`
**before** the bytes ever reach the broker — the SR-side error you see in the
response is the second-stage check inside `@confluentinc/schemaregistry`.
