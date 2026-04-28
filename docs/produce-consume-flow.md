# Produce / Consume flow (UserCreated)

```mermaid
sequenceDiagram
  autonumber
  actor Client
  participant REST as UsersController
  participant PROD as ProducerService
  participant SR as SchemaRegistry
  participant KAFKA as Kafka
  participant REG as ConsumerRegistry
  participant HND as UserCreatedConsumer

  Client->>REST: POST /users
  REST->>PROD: produce({ topic: 'user.created', value })
  PROD->>SR: encode('user.created-value', value)
  SR-->>PROD: Avro bytes (magic byte + schema id + payload)
  PROD->>KAFKA: send (idempotent, GZIP)
  KAFKA-->>PROD: metadata
  PROD-->>REST: ack
  REST-->>Client: 202 Accepted { eventId }

  KAFKA->>REG: eachMessage('user.created')
  REG->>SR: decode(bytes)
  SR-->>REG: UserCreatedEvent
  REG->>HND: handle(DecodedKafkaMessage)
  HND-->>REG: ok
  REG->>KAFKA: auto-commit offset
```

## Failure paths

### Decode failure (poison pill)

```mermaid
sequenceDiagram
  participant KAFKA as Kafka
  participant REG as ConsumerRegistry
  participant SR as SchemaRegistry
  participant PROD as ProducerService
  participant DLQ as user.created.DLQ

  KAFKA->>REG: eachMessage
  REG->>SR: decode(bytes)
  SR--xREG: throws (bad magic byte / unknown schema id)
  REG->>PROD: produce({ topic: DLQ, raw: true, headers: { x-error-name: 'PoisonPillError', ... } })
  PROD->>DLQ: original bytes
  REG->>KAFKA: commit offset (skip)
```

### Handler exhaustion

```mermaid
sequenceDiagram
  participant REG as ConsumerRegistry
  participant HND as Handler
  participant PROD as Producer
  participant DLQ as &lt;topic&gt;.DLQ

  loop up to maxAttempts
    REG->>HND: handle
    HND--xREG: throws
    REG->>REG: sleep backoffMs[attempt]
  end
  REG->>PROD: produce(DLQ, headers: { x-error-name: 'HandlerExhaustedError', x-attempts: N })
  REG->>KAFKA: commit offset
```

## Subject naming

Default is `TopicNameStrategy`: subject is `<topic>-value`.

- Filename `user-created.avsc` → topic `user.created` → subject `user.created-value`.
- To change the convention, override `ProducerService.produce({ subject: '...' })` per call, or edit `SchemaRegistryService.subjectForFile`.
