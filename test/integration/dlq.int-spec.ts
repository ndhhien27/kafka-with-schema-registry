/**
 * Integration test: bad payload is routed to <topic>.DLQ with correct headers.
 * Exercises the order.placed consumer which throws on amountCents < 0.
 */
import { Test } from '@nestjs/testing';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { randomUUID } from 'crypto';
import { Kafka, logLevel } from 'kafkajs';
import { startKafkaStack, KafkaStack } from './kafka.containers';
import { AppConfigModule } from '../../src/config/config.module';
import { SchemaRegistryModule } from '../../src/schema-registry/schema-registry.module';
import { SchemaRegistryService } from '../../src/schema-registry/schema-registry.service';
import { KafkaModule } from '../../src/kafka/kafka.module';
import { OrdersModule } from '../../src/features/orders/orders.module';
import { ProducerService } from '../../src/kafka/producer.service';
import { AvroSerializer } from '../../src/kafka/serdes/avro.serializer';
import { AvroDeserializer } from '../../src/kafka/serdes/avro.deserializer';
import {
  ORDER_PLACED_TOPIC,
  OrderPlacedEvent,
} from '../../src/features/orders/order-events.types';
import type { INestApplication } from '@nestjs/common';

jest.setTimeout(240_000);

describe('DLQ routing (integration)', () => {
  let stack: KafkaStack;
  let app: INestApplication;
  let producer: ProducerService;

  beforeAll(async () => {
    stack = await startKafkaStack();

    process.env.KAFKA_BROKERS = stack.brokersForHost;
    process.env.SCHEMA_REGISTRY_URL = stack.schemaRegistryUrl;
    process.env.KAFKA_CLIENT_ID = 'int-dlq';
    process.env.KAFKA_GROUP_ID = 'int-dlq-group';
    process.env.LOG_LEVEL = 'warn';

    const moduleRef = await Test.createTestingModule({
      imports: [AppConfigModule, SchemaRegistryModule, KafkaModule, OrdersModule],
    }).compile();

    app = moduleRef.createNestApplication();
    app.enableShutdownHooks();
    await app.init();

    const sr = app.get(SchemaRegistryService);
    app.connectMicroservice<MicroserviceOptions>({
      transport: Transport.KAFKA,
      options: {
        client: {
          clientId: 'int-dlq-microservice',
          brokers: [stack.brokersForHost],
        },
        consumer: {
          groupId: 'int-dlq-group',
          allowAutoTopicCreation: true,
        },
        subscribe: { fromBeginning: true },
        run: { autoCommit: true },
        serializer: new AvroSerializer(sr),
        deserializer: new AvroDeserializer(sr),
      },
    });
    await app.startAllMicroservices();

    producer = app.get(ProducerService);
  });

  afterAll(async () => {
    await app?.close();
    await stack?.shutdown();
  });

  it('routes a handler-failed message to <topic>.DLQ with headers', async () => {
    const event: OrderPlacedEvent = {
      eventId: randomUUID(),
      occurredAt: Date.now(),
      orderId: 'o-1',
      userId: 'u-1',
      amountCents: -100, // triggers intentional throw in OrderPlacedConsumer
      currency: 'USD',
      items: [{ sku: 'ABC', quantity: 1 }],
    };

    await producer.produce<OrderPlacedEvent>({
      topic: ORDER_PLACED_TOPIC,
      key: event.orderId,
      value: event,
    });

    const kafka = new Kafka({
      clientId: 'dlq-observer',
      brokers: [stack.brokersForHost],
      logLevel: logLevel.NOTHING,
    });
    const observer = kafka.consumer({ groupId: `dlq-obs-${Date.now()}` });
    await observer.connect();
    await observer.subscribe({
      topic: `${ORDER_PLACED_TOPIC}.DLQ`,
      fromBeginning: true,
    });

    const seen: { headers: Record<string, string>; key: Buffer | null }[] = [];
    await observer.run({
      autoCommit: true,
      eachMessage: async ({ message }) => {
        const headers: Record<string, string> = {};
        for (const [k, v] of Object.entries(message.headers ?? {})) {
          headers[k] = v ? v.toString() : '';
        }
        seen.push({ headers, key: message.key });
      },
    });

    const deadline = Date.now() + 120_000;
    while (seen.length === 0 && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 500));
    }

    await observer.disconnect();

    expect(seen.length).toBeGreaterThanOrEqual(1);
    const [dlqMsg] = seen;
    expect(dlqMsg.key?.toString()).toBe('o-1');
    expect(dlqMsg.headers['x-original-topic']).toBe(ORDER_PLACED_TOPIC);
    expect(dlqMsg.headers['x-error-name']).toBe('HandlerExhaustedError');
    expect(Number(dlqMsg.headers['x-attempts'])).toBeGreaterThan(0);
  });
});
