import { Test } from '@nestjs/testing';
import { ProducerService } from '../../src/kafka/producer.service';
import { KAFKA_CLIENT } from '../../src/kafka/kafka.tokens';
import { SchemaRegistryService } from '../../src/schema-registry/schema-registry.service';

describe('ProducerService', () => {
  const send = jest.fn().mockResolvedValue([{ topicName: 'one-bth-dev-user-created-in-private' }]);
  const connect = jest.fn().mockResolvedValue(undefined);
  const disconnect = jest.fn().mockResolvedValue(undefined);
  const kafkaMock = { producer: () => ({ send, connect, disconnect }) };
  const encode = jest.fn().mockResolvedValue(Buffer.from('encoded'));
  const srMock = { encode };

  let service: ProducerService;

  beforeEach(async () => {
    jest.clearAllMocks();
    const moduleRef = await Test.createTestingModule({
      providers: [
        ProducerService,
        { provide: KAFKA_CLIENT, useValue: kafkaMock },
        { provide: SchemaRegistryService, useValue: srMock },
      ],
    }).compile();
    service = moduleRef.get(ProducerService);
    await service.onModuleInit();
  });

  it('connects on module init', () => {
    expect(connect).toHaveBeenCalledTimes(1);
  });

  it('encodes via Schema Registry by default, passing topic through', async () => {
    await service.produce({
      topic: 'one-bth-dev-user-created-in-private',
      key: 'u1',
      value: { userId: 'u1', email: 'a@b.c' },
    });
    expect(encode).toHaveBeenCalledWith('one-bth-dev-user-created-in-private', {
      userId: 'u1',
      email: 'a@b.c',
    });
    expect(send).toHaveBeenCalledTimes(1);
    const [call] = send.mock.calls;
    expect(call[0].topic).toBe('one-bth-dev-user-created-in-private');
    expect(call[0].messages[0].key).toBe('u1');
    expect(call[0].messages[0].value).toEqual(Buffer.from('encoded'));
  });

  it('skips Schema Registry encoding when raw=true', async () => {
    await service.produce({
      topic: 'one-bth-dev-user-created-in-private.DLQ',
      raw: true,
      value: Buffer.from('raw-bytes'),
    });
    expect(encode).not.toHaveBeenCalled();
    const [call] = send.mock.calls;
    expect(call[0].messages[0].value).toEqual(Buffer.from('raw-bytes'));
  });

  it('disconnects on shutdown', async () => {
    await service.onApplicationShutdown('SIGTERM');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
