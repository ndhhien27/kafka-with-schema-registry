import { Logger } from '@nestjs/common';
import type { Deserializer } from '@nestjs/microservices';
import type { KafkaMessage } from 'kafkajs';
import { SchemaRegistryService } from '../../schema-registry/schema-registry.service';

/**
 * Decodes Schema Registry framed payloads (magic byte 0x00 + 4-byte schema id + Avro)
 * and returns an IncomingEvent-shaped packet `{ pattern, data }` that NestJS's
 * ServerKafka expects.
 *
 * The original raw Buffer in `rawMessage.value` is preserved by the upstream
 * KafkaParser when the leading byte is 0x00, so KafkaContext.getMessage().value
 * still holds the SR-framed bytes — the DLQ filter relies on this.
 */
export class AvroDeserializer
  implements Deserializer<KafkaMessage, Promise<{ pattern: unknown; data: unknown }>>
{
  private readonly logger = new Logger(AvroDeserializer.name);

  constructor(private readonly sr: SchemaRegistryService) {}

  async deserialize(
    rawMessage: KafkaMessage,
    options?: Record<string, unknown>,
  ): Promise<{ pattern: unknown; data: unknown }> {
    const channel = options?.channel as string | undefined;
    const value = rawMessage.value;

    let data: unknown = null;
    let framed = false;
    let schemaId: number | null = null;
    if (Buffer.isBuffer(value) && value.length > 0 && value.readUInt8(0) === 0) {
      framed = true;
      schemaId = value.length >= 5 ? value.readUInt32BE(1) : null;
      try {
        data = await this.sr.decode(channel ?? '', value);
      } catch (err) {
        this.logger.error(
          `SR decode failed on topic=${channel ?? '?'}: ${(err as Error).message}`,
        );
        // Re-throw so ServerKafka's flow surfaces the error to the exception filter,
        // which routes the original SR-framed buffer to the DLQ.
        throw err;
      }
    } else if (value !== null && value !== undefined) {
      data = value;
    }

    const hasKey = rawMessage.key != null;
    const decodedRepr = Buffer.isBuffer(data)
      ? data.toString('base64')
      : safeStringify(data);
    this.logger.log(
      `Avro deserialize topic=${channel ?? '?'}` +
        ` schemaId=${schemaId ?? 'none'}` +
        ` framed=${framed}` +
        ` hasKey=${hasKey}` +
        ` decoded=${decodedRepr}`,
    );

    return { pattern: channel, data };
  }
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
