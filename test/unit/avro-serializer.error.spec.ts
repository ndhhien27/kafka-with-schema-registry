import { AvroSerializer } from '../../src/kafka/serdes/avro.serializer';
import type { SchemaRegistryService } from '../../src/schema-registry/schema-registry.service';

const buildSerializationError = (message: string): Error => {
  const err = new Error(message);
  err.name = 'SerializationError';
  return err;
};

describe('AvroSerializer error handling', () => {
  it('logs validation errors with topic + parsed paths and rethrows', async () => {
    const sr = {
      encode: jest
        .fn()
        .mockRejectedValueOnce(
          buildSerializationError('Invalid message at email, expected "string", got 123'),
        ),
      decode: jest.fn(),
    } as unknown as SchemaRegistryService;

    const ser = new AvroSerializer(sr);
    const warnSpy = jest
      .spyOn((ser as unknown as { logger: { warn: jest.Mock } }).logger, 'warn')
      .mockImplementation(() => undefined);

    await expect(
      ser.serialize({ email: 123 }, { pattern: 'one-bth-dev-user-created-in-private' }),
    ).rejects.toMatchObject({ name: 'SerializationError' });

    expect(warnSpy).toHaveBeenCalledWith(
      expect.stringContaining('topic=one-bth-dev-user-created-in-private'),
    );
    expect(warnSpy).toHaveBeenCalledWith(
      expect.stringContaining('paths=[["email"]]'),
    );
  });

  it('logs generic SR encode errors at error level', async () => {
    const sr = {
      encode: jest.fn().mockRejectedValueOnce(new Error('SR unreachable')),
      decode: jest.fn(),
    } as unknown as SchemaRegistryService;

    const ser = new AvroSerializer(sr);
    const errorSpy = jest
      .spyOn((ser as unknown as { logger: { error: jest.Mock } }).logger, 'error')
      .mockImplementation(() => undefined);

    await expect(
      ser.serialize({ x: 1 }, { pattern: 'foo' }),
    ).rejects.toThrow(/SR unreachable/);

    expect(errorSpy).toHaveBeenCalledWith(
      expect.stringContaining('Schema Registry encode error topic=foo'),
    );
  });
});
