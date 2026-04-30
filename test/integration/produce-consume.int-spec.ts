/**
 * Integration test: produce via Avro, consume via Avro, assert decoded payload.
 * Requires Docker.
 */
import { Test } from '@nestjs/testing';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { startKafkaStack, KafkaStack } from './kafka.containers';
import { AppConfigModule } from '../../src/config/config.module';
import { SchemaRegistryModule } from '../../src/schema-registry/schema-registry.module';
import { SchemaRegistryService } from '../../src/schema-registry/schema-registry.service';
import { KafkaModule } from '../../src/kafka/kafka.module';
import { UsersModule } from '../../src/features/users/users.module';
import { UserCreatedProducer } from '../../src/features/users/user-created.producer';
import { UserCreatedConsumer } from '../../src/features/users/user-created.consumer';
import { AvroSerializer } from '../../src/kafka/serdes/avro.serializer';
import { AvroDeserializer } from '../../src/kafka/serdes/avro.deserializer';
import type { INestApplication } from '@nestjs/common';

jest.setTimeout(180_000);

describe('produce → consume (integration)', () => {
  let stack: KafkaStack;
  let app: INestApplication;
  let producer: UserCreatedProducer;
  let consumer: UserCreatedConsumer;

  beforeAll(async () => {
    stack = await startKafkaStack();

    process.env.KAFKA_BROKERS = stack.brokersForHost;
    process.env.SCHEMA_REGISTRY_URL = stack.schemaRegistryUrl;
    process.env.KAFKA_CLIENT_ID = 'int-test';
    process.env.KAFKA_GROUP_ID = 'int-test-group';
    process.env.LOG_LEVEL = 'warn';

    const moduleRef = await Test.createTestingModule({
      imports: [AppConfigModule, SchemaRegistryModule, KafkaModule, UsersModule],
    }).compile();

    app = moduleRef.createNestApplication();
    app.enableShutdownHooks();
    await app.init();

    const sr = app.get(SchemaRegistryService);
    app.connectMicroservice<MicroserviceOptions>({
      transport: Transport.KAFKA,
      options: {
        client: {
          clientId: 'int-test-microservice',
          brokers: [stack.brokersForHost],
        },
        consumer: {
          groupId: 'int-test-group',
          allowAutoTopicCreation: true,
        },
        subscribe: { fromBeginning: true },
        run: { autoCommit: true },
        serializer: new AvroSerializer(sr),
        deserializer: new AvroDeserializer(sr),
      },
    });
    await app.startAllMicroservices();

    producer = app.get(UserCreatedProducer);
    consumer = app.get(UserCreatedConsumer);
  });

  afterAll(async () => {
    await app?.close();
    await stack?.shutdown();
  });

  it('round-trips a UserCreated event through Avro', async () => {
    const received: unknown[] = [];
    const origHandle = consumer.handle.bind(consumer);
    jest.spyOn(consumer, 'handle').mockImplementation(async (value, ctx) => {
      received.push(value);
      return origHandle(value, ctx);
    });

    const event = await producer.emit({
      userId: 'u-1',
      email: 'a@example.com',
      displayName: 'Alice',
    });

    await waitFor(() => received.length >= 1, 60_000);

    expect(received).toHaveLength(1);
    const [decoded] = received as Array<{ eventId: string; email: string }>;
    expect(decoded.eventId).toBe(event.eventId);
    expect(decoded.email).toBe('a@example.com');
  });
});

async function waitFor(predicate: () => boolean, timeoutMs: number): Promise<void> {
  const start = Date.now();
  while (!predicate()) {
    if (Date.now() - start > timeoutMs) {
      throw new Error(`waitFor timed out after ${timeoutMs}ms`);
    }
    await new Promise((r) => setTimeout(r, 250));
  }
}
