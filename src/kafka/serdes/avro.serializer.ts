import { Logger } from '@nestjs/common';
import type { Serializer } from '@nestjs/microservices';
import type { Message } from 'kafkajs';
import { SchemaRegistryService } from '../../schema-registry/schema-registry.service';
import {
  extractValidationPaths,
  isSchemaRegistryValidationError,
} from './sr-errors';

/**
 * Encodes outbound payloads via the Schema Registry (TopicNameStrategy: <topic>-value).
 *
 * NestJS's ClientKafka calls `serializer.serialize(packet.data, { pattern })`,
 * where `pattern` is the topic. The returned object becomes a kafkajs Message.
 *
 * Schema-validation errors from the Schema Registry are logged with topic + field
 * paths, then rethrown so an upstream exception filter (HTTP) can map them to a
 * 4xx response without crashing the process.
 */
export class AvroSerializer
  implements Serializer<unknown, Promise<Message>>
{
  private readonly logger = new Logger(AvroSerializer.name);

  constructor(private readonly sr: SchemaRegistryService) {}

  async serialize(
    value: unknown,
    options?: Record<string, unknown>,
  ): Promise<Message> {
    const topic = options?.pattern as string | undefined;
    if (!topic) {
      throw new Error(
        'AvroSerializer.serialize called without a topic (options.pattern)',
      );
    }
    const envelope = this.toEnvelope(value);

    let encoded: Buffer;
    try {
      encoded = await this.sr.encode(topic, envelope.value);
    } catch (err) {
      if (isSchemaRegistryValidationError(err)) {
        const paths = extractValidationPaths(err);
        this.logger.warn(
          `Schema validation failed topic=${topic}` +
            ` paths=${JSON.stringify(paths ?? [])} reason=${err.message}`,
        );
      } else {
        const message = err instanceof Error ? err.message : String(err);
        this.logger.error(
          `Schema Registry encode error topic=${topic}: ${message}`,
        );
      }
      throw err;
    }

    const schemaId = readSchemaId(encoded);
    this.logger.log(
      `Avro serialize topic=${topic}` +
        ` schemaId=${schemaId ?? 'none'}` +
        ` hasKey=${envelope.key != null}` +
        ` bytes=${encoded.length}` +
        ` encoded=${encoded.toString('base64')}` +
        ` data=${safeStringify(envelope.value)}`,
    );

    return {
      key: envelope.key ?? null,
      value: encoded,
      headers: envelope.headers ?? {},
    };
  }

  private toEnvelope(value: unknown): {
    key?: string | Buffer | null;
    value: unknown;
    headers?: Record<string, string | Buffer>;
  } {
    if (
      value !== null &&
      typeof value === 'object' &&
      ('key' in (value as object) || 'value' in (value as object))
    ) {
      const v = value as {
        key?: string | Buffer | null;
        value: unknown;
        headers?: Record<string, string | Buffer>;
      };
      return v;
    }
    return { value };
  }
}

function readSchemaId(buf: Buffer): number | null {
  if (!Buffer.isBuffer(buf) || buf.length < 5 || buf.readUInt8(0) !== 0) {
    return null;
  }
  return buf.readUInt32BE(1);
}

function safeStringify(value: unknown): string {
  try {
    return JSON.stringify(value, (_k, v) =>
      Buffer.isBuffer(v) ? `<Buffer ${v.length}b>` : v,
    );
  } catch {
    return '<unserializable>';
  }
}
