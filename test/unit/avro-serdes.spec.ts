import type { KafkaMessage } from 'kafkajs';
import { AvroSerializer } from '../../src/kafka/serdes/avro.serializer';
import { AvroDeserializer } from '../../src/kafka/serdes/avro.deserializer';
import type { SchemaRegistryService } from '../../src/schema-registry/schema-registry.service';

const SR_FRAMED = (idLow: number, payload: string): Buffer => {
  const buf = Buffer.alloc(5 + Buffer.byteLength(payload));
  buf.writeUInt8(0, 0);
  buf.writeUInt32BE(idLow, 1);
  buf.write(payload, 5);
  return buf;
};

describe('AvroSerializer', () => {
  let sr: jest.Mocked<Pick<SchemaRegistryService, 'encode' | 'decode'>>;

  beforeEach(() => {
    sr = {
      encode: jest.fn(async (_topic: string, value: unknown) =>
        SR_FRAMED(1, JSON.stringify(value)),
      ),
      decode: jest.fn(async (_topic: string, _buf: Buffer) => ({ decoded: true })),
    } as unknown as jest.Mocked<Pick<SchemaRegistryService, 'encode' | 'decode'>>;
  });

  it('encodes plain values, passing topic through to SR', async () => {
    const ser = new AvroSerializer(sr as unknown as SchemaRegistryService);
    const logSpy = jest
      .spyOn(
        (ser as unknown as { logger: { log: jest.Mock } }).logger,
        'log',
      )
      .mockImplementation(() => undefined);

    const out = await ser.serialize({ userId: 'u-1' }, { pattern: 'one-bth-dev-user-created-in-private' });

    expect(sr.encode).toHaveBeenCalledWith('one-bth-dev-user-created-in-private', { userId: 'u-1' });
    expect(Buffer.isBuffer(out.value)).toBe(true);
    expect(out.key).toBeNull();
    expect(out.headers).toEqual({});
    expect(logSpy).toHaveBeenCalledTimes(1);
    const msg = logSpy.mock.calls[0][0] as string;
    expect(msg).toContain('topic=one-bth-dev-user-created-in-private');
    expect(msg).toContain('schemaId=1');
    expect(msg).toContain('hasKey=false');
    expect(msg).toContain('bytes=');
    expect(msg).toContain('encoded=');
    expect(msg).toContain('data={"userId":"u-1"}');
  });

  it('honors caller-provided key/headers envelope', async () => {
    const ser = new AvroSerializer(sr as unknown as SchemaRegistryService);
    const logSpy = jest
      .spyOn(
        (ser as unknown as { logger: { log: jest.Mock } }).logger,
        'log',
      )
      .mockImplementation(() => undefined);

    const out = await ser.serialize(
      { key: 'u-1', value: { userId: 'u-1' }, headers: { 'x-trace': 't' } },
      { pattern: 'one-bth-dev-user-created-in-private' },
    );

    expect(sr.encode).toHaveBeenCalledWith('one-bth-dev-user-created-in-private', { userId: 'u-1' });
    expect(out.key).toBe('u-1');
    expect(out.headers).toEqual({ 'x-trace': 't' });
    expect(logSpy).toHaveBeenCalledTimes(1);
    expect(logSpy.mock.calls[0][0] as string).toContain('hasKey=true');
  });

  it('throws when topic is missing', async () => {
    const ser = new AvroSerializer(sr as unknown as SchemaRegistryService);
    await expect(ser.serialize({ x: 1 })).rejects.toThrow(/without a topic/);
  });
});

describe('AvroDeserializer', () => {
  let sr: jest.Mocked<Pick<SchemaRegistryService, 'encode' | 'decode'>>;

  beforeEach(() => {
    sr = {
      encode: jest.fn(),
      decode: jest.fn(async () => ({ userId: 'u-1', email: 'a@b' })),
    } as unknown as jest.Mocked<Pick<SchemaRegistryService, 'encode' | 'decode'>>;
  });

  it('decodes SR-framed buffers and returns { pattern, data }', async () => {
    const des = new AvroDeserializer(sr as unknown as SchemaRegistryService);
    const logSpy = jest
      .spyOn(
        (des as unknown as { logger: { log: jest.Mock } }).logger,
        'log',
      )
      .mockImplementation(() => undefined);
    const msg = { value: SR_FRAMED(1, 'placeholder') } as KafkaMessage;

    const out = await des.deserialize(msg, { channel: 'one-bth-dev-user-created-in-private' });

    expect(sr.decode).toHaveBeenCalledTimes(1);
    expect(sr.decode).toHaveBeenCalledWith('one-bth-dev-user-created-in-private', expect.any(Buffer));
    expect(out).toEqual({
      pattern: 'one-bth-dev-user-created-in-private',
      data: { userId: 'u-1', email: 'a@b' },
    });
    expect(logSpy).toHaveBeenCalledTimes(1);
    const logMsg = logSpy.mock.calls[0][0] as string;
    expect(logMsg).toContain('topic=one-bth-dev-user-created-in-private');
    expect(logMsg).toContain('schemaId=1');
    expect(logMsg).toContain('framed=true');
    expect(logMsg).toContain('hasKey=false');
    expect(logMsg).toContain('decoded={"userId":"u-1","email":"a@b"}');
  });

  it('passes through non-SR buffers without calling SR.decode', async () => {
    const des = new AvroDeserializer(sr as unknown as SchemaRegistryService);
    const logSpy = jest
      .spyOn(
        (des as unknown as { logger: { log: jest.Mock } }).logger,
        'log',
      )
      .mockImplementation(() => undefined);
    const msg = { value: Buffer.from('plain') } as KafkaMessage;

    const out = await des.deserialize(msg, { channel: 't' });

    expect(sr.decode).not.toHaveBeenCalled();
    expect(out.pattern).toBe('t');
    expect(Buffer.isBuffer(out.data)).toBe(true);
    expect(logSpy).toHaveBeenCalledTimes(1);
    const logMsg = logSpy.mock.calls[0][0] as string;
    expect(logMsg).toContain('framed=false');
    expect(logMsg).toContain('schemaId=none');
    expect(logMsg).toContain('decoded=');
  });

  it('returns null data when message value is null', async () => {
    const des = new AvroDeserializer(sr as unknown as SchemaRegistryService);
    const logSpy = jest
      .spyOn(
        (des as unknown as { logger: { log: jest.Mock } }).logger,
        'log',
      )
      .mockImplementation(() => undefined);
    const msg = { value: null } as unknown as KafkaMessage;

    const out = await des.deserialize(msg, { channel: 't' });

    expect(out).toEqual({ pattern: 't', data: null });
    expect(logSpy).toHaveBeenCalledTimes(1);
    expect(logSpy.mock.calls[0][0] as string).toContain('decoded=null');
  });

  it('rethrows when SR decode fails (poison pill path)', async () => {
    sr.decode.mockRejectedValueOnce(new Error('bad schema id'));
    const des = new AvroDeserializer(sr as unknown as SchemaRegistryService);
    const logSpy = jest
      .spyOn(
        (des as unknown as { logger: { log: jest.Mock } }).logger,
        'log',
      )
      .mockImplementation(() => undefined);
    const msg = { value: SR_FRAMED(99, 'x') } as KafkaMessage;

    await expect(
      des.deserialize(msg, { channel: 'one-bth-dev-user-created-in-private' }),
    ).rejects.toThrow(/bad schema id/);
    expect(logSpy).not.toHaveBeenCalled();
  });
});
