import type { ArgumentsHost } from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import { KafkaDlqFilter } from '../../src/kafka/filters/kafka-dlq.filter';
import {
  HandlerExhaustedError,
  PoisonPillError,
} from '../../src/kafka/errors/poison-pill.error';
import { KAFKA_RETRY_METADATA } from '../../src/kafka/decorators/kafka-retry.decorator';
import type { ProducerService } from '../../src/kafka/producer.service';

const buildHost = (overrides: {
  topic?: string;
  partition?: number;
  message?: { offset?: string; key?: Buffer | null; value?: Buffer | null };
  handler?: () => void;
  type?: 'rpc' | 'http';
} = {}): ArgumentsHost => {
  const message = {
    offset: '42',
    key: Buffer.from('k1'),
    value: Buffer.concat([Buffer.from([0x00, 0, 0, 0, 1]), Buffer.from('avro-payload')]),
    ...(overrides.message ?? {}),
  };
  const ctx = {
    getMessage: jest.fn().mockReturnValue(message),
    getTopic: jest.fn().mockReturnValue(overrides.topic ?? 'order.placed'),
    getPartition: jest.fn().mockReturnValue(overrides.partition ?? 0),
  };
  const handler = overrides.handler ?? function fakeHandler() {};
  return {
    getType: () => overrides.type ?? 'rpc',
    switchToRpc: () => ({ getContext: () => ctx }),
    getHandler: () => handler,
    getClass: () => class {},
  } as unknown as ArgumentsHost;
};

describe('KafkaDlqFilter', () => {
  let producer: { produce: jest.Mock };
  let reflector: Reflector;
  let filter: KafkaDlqFilter;

  beforeEach(() => {
    producer = { produce: jest.fn().mockResolvedValue([]) };
    reflector = new Reflector();
    filter = new KafkaDlqFilter(producer as unknown as ProducerService, reflector);
  });

  it('routes a poison-pill error to <topic>.DLQ with raw original buffer', async () => {
    const host = buildHost({ topic: 'user.created' });
    const err = new PoisonPillError('decode failed', new Error('boom'));

    await filter.catch(err, host);

    expect(producer.produce).toHaveBeenCalledTimes(1);
    const call = producer.produce.mock.calls[0][0];
    expect(call.topic).toBe('user.created.DLQ');
    expect(call.raw).toBe(true);
    expect(Buffer.isBuffer(call.value)).toBe(true);
    expect(call.headers['x-original-topic']).toBe('user.created');
    expect(call.headers['x-original-partition']).toBe('0');
    expect(call.headers['x-original-offset']).toBe('42');
    expect(call.headers['x-error-name']).toBe('PoisonPillError');
    expect(call.headers['x-error-message']).toContain('decode failed');
    expect(call.headers['x-attempts']).toBe('1');
    expect(call.headers['x-dlq-at']).toMatch(/^\d{4}-\d{2}-\d{2}T/);
  });

  it('uses HandlerExhaustedError attempts in x-attempts header', async () => {
    const host = buildHost({ topic: 'order.placed' });
    const err = new HandlerExhaustedError('exhausted', 3, new Error('nope'));

    await filter.catch(err, host);

    const call = producer.produce.mock.calls[0][0];
    expect(call.headers['x-error-name']).toBe('HandlerExhaustedError');
    expect(call.headers['x-attempts']).toBe('3');
    expect(call.topic).toBe('order.placed.DLQ');
  });

  it('respects @KafkaRetry dlqTopic override on the handler', async () => {
    function tagged() {}
    Reflect.defineMetadata(
      KAFKA_RETRY_METADATA,
      { dlqTopic: 'orders.dead', maxAttempts: 5 },
      tagged,
    );
    const host = buildHost({ topic: 'order.placed', handler: tagged });

    await filter.catch(new Error('handler boom'), host);

    expect(producer.produce.mock.calls[0][0].topic).toBe('orders.dead');
    expect(producer.produce.mock.calls[0][0].headers['x-attempts']).toBe('5');
  });

  it('rethrows non-rpc exceptions', async () => {
    const host = buildHost({ type: 'http' });
    await expect(filter.catch(new Error('nope'), host)).rejects.toThrow('nope');
    expect(producer.produce).not.toHaveBeenCalled();
  });

  it('swallows DLQ publish errors so the partition is not blocked', async () => {
    producer.produce.mockRejectedValueOnce(new Error('broker down'));
    const host = buildHost();

    await expect(filter.catch(new Error('handler boom'), host)).resolves.toBeUndefined();
  });
});
