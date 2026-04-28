/**
 * E2E test for the /health endpoint. Requires Docker (spins up Kafka + SR).
 */
import { Test } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import request from 'supertest';
import { startKafkaStack, KafkaStack } from '../integration/kafka.containers';
import { AppModule } from '../../src/app.module';

jest.setTimeout(180_000);

describe('/health (e2e)', () => {
  let stack: KafkaStack;
  let app: INestApplication;

  beforeAll(async () => {
    stack = await startKafkaStack();
    process.env.KAFKA_BROKERS = stack.brokersForHost;
    process.env.SCHEMA_REGISTRY_URL = stack.schemaRegistryUrl;
    process.env.LOG_LEVEL = 'warn';

    const moduleRef = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();
    app = moduleRef.createNestApplication();
    app.enableShutdownHooks();
    await app.init();
  });

  afterAll(async () => {
    await app?.close();
    await stack?.shutdown();
  });

  it('returns ok when Kafka + SR are reachable', async () => {
    const res = await request(app.getHttpServer()).get('/health');
    expect(res.status).toBe(200);
    expect(res.body.status).toBe('ok');
    expect(res.body.info.kafka.status).toBe('up');
    expect(res.body.info.schemaRegistry.status).toBe('up');
  });
});
