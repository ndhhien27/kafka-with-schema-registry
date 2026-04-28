/**
 * Integration test: produce via Avro, consume via Avro, assert decoded payload.
 * Requires Docker.
 */
import { Test } from '@nestjs/testing';
import { startKafkaStack, KafkaStack } from './kafka.containers';
import { AppConfigModule } from '../../src/config/config.module';
import { SchemaRegistryModule } from '../../src/schema-registry/schema-registry.module';
import { KafkaModule } from '../../src/kafka/kafka.module';
import { UsersModule } from '../../src/features/users/users.module';
import { UserCreatedProducer } from '../../src/features/users/user-created.producer';
import { UserCreatedConsumer } from '../../src/features/users/user-created.consumer';
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
    jest.spyOn(consumer, 'handle').mockImplementation(async (msg) => {
      received.push(msg);
      return origHandle(msg);
    });

    const event = await producer.emit({
      userId: 'u-1',
      email: 'a@example.com',
      displayName: 'Alice',
    });

    await waitFor(() => received.length >= 1, 30_000);

    expect(received).toHaveLength(1);
    const [decoded] = received as any[];
    expect(decoded.topic).toBe('user.created');
    expect(decoded.value.eventId).toBe(event.eventId);
    expect(decoded.value.email).toBe('a@example.com');
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
