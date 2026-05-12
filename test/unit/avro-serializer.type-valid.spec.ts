import { AvroSerializer } from '../../src/kafka/serdes/avro.serializer';
import { SchemaPayloadInvalidError } from '../../src/kafka/serdes/sr-errors';
import type { SchemaRegistryService } from '../../src/schema-registry/schema-registry.service';

const SCHEMA_TEXT = JSON.stringify({
  type: 'record',
  name: 'UserCreated',
  namespace: 'com.example.events',
  fields: [
    { name: 'eventId', type: 'string' },
    {
      name: 'occurredAt',
      type: { type: 'long', logicalType: 'timestamp-millis' },
    },
    { name: 'userId', type: 'string' },
    { name: 'email', type: 'string' },
    { name: 'displayName', type: ['null', 'string'], default: null },
  ],
});

const TOPIC = 'one-bth-dev-user-created-in-private';

const buildSr = (): jest.Mocked<
  Pick<SchemaRegistryService, 'encode' | 'decode' | 'getSchemaTextForTopic'>
> => {
  return {
    encode: jest.fn(async () => Buffer.from([0x00, 0, 0, 0, 1, 0xff])),
    decode: jest.fn(),
    getSchemaTextForTopic: jest.fn(() => SCHEMA_TEXT),
  } as unknown as jest.Mocked<
    Pick<SchemaRegistryService, 'encode' | 'decode' | 'getSchemaTextForTopic'>
  >;
};

describe('AvroSerializer Type.isValid pre-encode validation', () => {
  it('passes through when payload matches the local schema', async () => {
    const sr = buildSr();
    const ser = new AvroSerializer(sr as unknown as SchemaRegistryService);
    jest
      .spyOn(
        (ser as unknown as { logger: { log: jest.Mock; warn: jest.Mock } })
          .logger,
        'log',
      )
      .mockImplementation(() => undefined);

    const valid = {
      eventId: 'e-1',
      occurredAt: Date.now(),
      userId: 'u-1',
      email: 'a@b.c',
      displayName: 'Alice',
    };

    const out = await ser.serialize(valid, { pattern: TOPIC });

    expect(sr.encode).toHaveBeenCalledTimes(1);
    expect(sr.encode).toHaveBeenCalledWith(TOPIC, valid);
    expect(Buffer.isBuffer(out.value)).toBe(true);
  });

  it('rejects payloads with invalid field types and never calls SR.encode', async () => {
    const sr = buildSr();
    const ser = new AvroSerializer(sr as unknown as SchemaRegistryService);
    jest
      .spyOn(
        (ser as unknown as { logger: { warn: jest.Mock } }).logger,
        'warn',
      )
      .mockImplementation(() => undefined);

    const bogus = {
      eventId: 'e-1',
      occurredAt: 'not-a-number',
      userId: 'u-1',
      email: 'a@b.c',
    };

    await expect(
      ser.serialize(bogus, { pattern: TOPIC }),
    ).rejects.toBeInstanceOf(SchemaPayloadInvalidError);
    expect(sr.encode).not.toHaveBeenCalled();
  });

  it('rejects payloads missing required fields', async () => {
    const sr = buildSr();
    const ser = new AvroSerializer(sr as unknown as SchemaRegistryService);
    jest
      .spyOn(
        (ser as unknown as { logger: { warn: jest.Mock } }).logger,
        'warn',
      )
      .mockImplementation(() => undefined);

    const incomplete = { eventId: 'e-1', occurredAt: Date.now() };

    const promise = ser.serialize(incomplete, { pattern: TOPIC });
    await expect(promise).rejects.toBeInstanceOf(SchemaPayloadInvalidError);
    await expect(promise).rejects.toMatchObject({ topic: TOPIC });
    expect(sr.encode).not.toHaveBeenCalled();
  });

  it('falls through to SR.encode when no local schema is available (mocked SR)', async () => {
    const sr = buildSr();
    sr.getSchemaTextForTopic = jest.fn((_topic: string): string | undefined => undefined);
    const ser = new AvroSerializer(sr as unknown as SchemaRegistryService);
    jest
      .spyOn(
        (ser as unknown as { logger: { log: jest.Mock } }).logger,
        'log',
      )
      .mockImplementation(() => undefined);

    await ser.serialize({ anything: 'goes' }, { pattern: 'no-schema-topic' });

    expect(sr.encode).toHaveBeenCalledTimes(1);
  });
});
