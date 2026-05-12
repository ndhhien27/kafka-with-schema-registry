import { Logger } from '@nestjs/common';
import type { Serializer } from '@nestjs/microservices';
import type { Message } from 'kafkajs';
import * as avsc from 'avsc';
import { SchemaRegistryService } from '../../schema-registry/schema-registry.service';
import {
  extractValidationPaths,
  isSchemaRegistryValidationError,
  SchemaPayloadInvalidError,
} from './sr-errors';

/**
 * Encodes outbound payloads via the Schema Registry (TopicNameStrategy: <topic>-value).
 *
 * Pre-encode, runs `Type.isValid` against the locally-loaded `.avsc` so callers
 * fail fast with field paths (BTH guideline: "App-side validation during
 * serialization"). Falls through silently when no local schema is available
 * (e.g. unit tests with a mocked SchemaRegistryService).
 *
 * NestJS's ClientKafka calls `serializer.serialize(packet.data, { pattern })`,
 * where `pattern` is the topic. The returned object becomes a kafkajs Message.
 */
export class AvroSerializer
  implements Serializer<unknown, Promise<Message>>
{
  private readonly logger = new Logger(AvroSerializer.name);
  private readonly typeCache = new Map<string, avsc.Type>();

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

    this.assertPayloadShape(topic, envelope.value);

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

  /**
   * BTH guideline: validate against the local `.avsc` before SR encode.
   * Skipped when no schema text is available for the topic (mocked SR in tests,
   * or topics whose schema lives only in the registry, not on disk).
   */
  private assertPayloadShape(topic: string, value: unknown): void {
    const type = this.getTypeForTopic(topic);
    if (!type) return;

    const paths: string[][] = [];
    const ok = type.isValid(value, {
      errorHook: (path) => {
        paths.push([...path]);
      },
    });

    if (!ok) {
      this.logger.warn(
        `Type.isValid rejected payload topic=${topic} paths=${JSON.stringify(paths)}`,
      );
      throw new SchemaPayloadInvalidError(topic, paths);
    }
  }

  private getTypeForTopic(topic: string): avsc.Type | undefined {
    const cached = this.typeCache.get(topic);
    if (cached) return cached;

    const schemaText = this.sr.getSchemaTextForTopic?.(topic);
    if (!schemaText) return undefined;

    try {
      const type = avsc.Type.forSchema(JSON.parse(schemaText));
      this.typeCache.set(topic, type);
      return type;
    } catch (err) {
      this.logger.warn(
        `Failed to parse local Avro schema for topic=${topic}: ${
          (err as Error).message
        }`,
      );
      return undefined;
    }
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
