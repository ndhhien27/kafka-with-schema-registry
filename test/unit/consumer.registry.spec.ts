import { Test } from '@nestjs/testing';
import { DiscoveryModule } from '@nestjs/core';
import { Injectable } from '@nestjs/common';
import { ConsumerRegistry } from '../../src/kafka/consumer.registry';
import { KafkaSubscribe } from '../../src/kafka/decorators/kafka-subscribe.decorator';
import { KAFKA_CLIENT } from '../../src/kafka/kafka.tokens';
import { AppConfigService } from '../../src/config/app-config.service';
import { SchemaRegistryService } from '../../src/schema-registry/schema-registry.service';
import { ProducerService } from '../../src/kafka/producer.service';

interface RunArg {
  eachMessage: (payload: unknown) => Promise<void>;
}

function makeConsumer() {
  const listeners: Record<string, Function[]> = {};
  let runArg: RunArg | null = null;
  return {
    events: {
      GROUP_JOIN: 'consumer.group_join',
      CRASH: 'consumer.crash',
      DISCONNECT: 'consumer.disconnect',
    },
    on: (event: string, cb: Function) => {
      (listeners[event] ??= []).push(cb);
    },
    connect: jest.fn().mockResolvedValue(undefined),
    disconnect: jest.fn().mockResolvedValue(undefined),
    subscribe: jest.fn().mockResolvedValue(undefined),
    run: jest.fn().mockImplementation(async (arg: RunArg) => {
      runArg = arg;
    }),
    __dispatch: async (payload: unknown) => {
      if (!runArg) throw new Error('consumer.run was not called');
      return runArg.eachMessage(payload);
    },
  };
}

@Injectable()
class HappyHandler {
  public received: unknown[] = [];

  @KafkaSubscribe({ topic: 'unit.happy', groupId: 'unit-happy' })
  async onMessage(msg: unknown) {
    this.received.push(msg);
  }
}

@Injectable()
class FlakyHandler {
  public calls = 0;

  @KafkaSubscribe({
    topic: 'unit.flaky',
    groupId: 'unit-flaky',
    maxAttempts: 3,
    backoffMs: [1, 1, 1],
  })
  async onMessage() {
    this.calls += 1;
    throw new Error('always fails');
  }
}

describe('ConsumerRegistry', () => {
  const consumer = makeConsumer();
  const kafkaMock = { consumer: () => consumer };
  const srMock: Partial<SchemaRegistryService> = {
    decode: jest.fn(async (buf: Buffer) => ({ decoded: buf.toString() })),
  };
  const produce = jest.fn().mockResolvedValue(undefined);
  const producerMock: Partial<ProducerService> = { produce };
  const cfgMock: Partial<AppConfigService> = {
    kafka: { groupId: 'default-group' } as AppConfigService['kafka'],
  };

  let registry: ConsumerRegistry;
  let happy: HappyHandler;
  let flaky: FlakyHandler;

  beforeEach(async () => {
    jest.clearAllMocks();
    const moduleRef = await Test.createTestingModule({
      imports: [DiscoveryModule],
      providers: [
        ConsumerRegistry,
        HappyHandler,
        FlakyHandler,
        { provide: KAFKA_CLIENT, useValue: kafkaMock },
        { provide: AppConfigService, useValue: cfgMock },
        { provide: SchemaRegistryService, useValue: srMock },
        { provide: ProducerService, useValue: producerMock },
      ],
    }).compile();

    registry = moduleRef.get(ConsumerRegistry);
    happy = moduleRef.get(HappyHandler);
    flaky = moduleRef.get(FlakyHandler);
    await registry.onModuleInit();
  });

  const buildPayload = (topic: string, value = Buffer.from('hello')) => ({
    topic,
    partition: 0,
    message: {
      key: Buffer.from('k'),
      value,
      offset: '42',
      timestamp: String(Date.now()),
      headers: {},
    },
  });

  it('dispatches decoded messages to handler', async () => {
    await consumer.__dispatch(buildPayload('unit.happy'));
    expect(happy.received).toHaveLength(1);
    const [received] = happy.received as any[];
    expect(received.topic).toBe('unit.happy');
    expect(received.value).toEqual({ decoded: 'hello' });
  });

  it('routes to DLQ when SR decode throws', async () => {
    (srMock.decode as jest.Mock).mockRejectedValueOnce(new Error('bad magic byte'));
    await consumer.__dispatch(buildPayload('unit.happy'));
    expect(produce).toHaveBeenCalledTimes(1);
    const [call] = produce.mock.calls;
    expect(call[0].topic).toBe('unit.happy.DLQ');
    expect(call[0].raw).toBe(true);
    expect(call[0].headers['x-error-name']).toBe('PoisonPillError');
  });

  it('retries handler up to maxAttempts then sends to DLQ', async () => {
    await consumer.__dispatch(buildPayload('unit.flaky'));
    expect(flaky.calls).toBe(3);
    expect(produce).toHaveBeenCalledTimes(1);
    const [call] = produce.mock.calls;
    expect(call[0].topic).toBe('unit.flaky.DLQ');
    expect(call[0].headers['x-error-name']).toBe('HandlerExhaustedError');
    expect(call[0].headers['x-attempts']).toBe('3');
  });
});
