import {
  Inject,
  Injectable,
  Logger,
  OnApplicationShutdown,
  OnModuleInit,
} from '@nestjs/common';
import {
  Kafka,
  Producer,
  CompressionTypes,
  Message,
  RecordMetadata,
} from 'kafkajs';
import { KAFKA_CLIENT } from './kafka.tokens';
import { SchemaRegistryService } from '../schema-registry/schema-registry.service';

export interface ProduceOptions<T> {
  topic: string;
  key?: string | Buffer | null;
  value: T;
  headers?: Record<string, string | Buffer>;
  /** If true, skip Schema Registry encoding (value must already be Buffer|string|null). */
  raw?: boolean;
}

@Injectable()
export class ProducerService implements OnModuleInit, OnApplicationShutdown {
  private readonly logger = new Logger(ProducerService.name);
  private readonly producer: Producer;
  private connected = false;

  constructor(
    @Inject(KAFKA_CLIENT) private readonly kafka: Kafka,
    private readonly sr: SchemaRegistryService,
  ) {
    this.producer = this.kafka.producer({
      idempotent: true,
      maxInFlightRequests: 5,
      transactionTimeout: 30_000,
      allowAutoTopicCreation: true,
    });
  }

  async onModuleInit(): Promise<void> {
    await this.producer.connect();
    this.connected = true;
    this.logger.log('Kafka producer connected');
  }

  async onApplicationShutdown(signal?: string): Promise<void> {
    if (!this.connected) return;
    this.logger.log(`Disconnecting Kafka producer (signal=${signal ?? 'n/a'})`);
    try {
      await this.producer.disconnect();
    } catch (err) {
      this.logger.error(
        `Error disconnecting producer: ${(err as Error).message}`,
      );
    } finally {
      this.connected = false;
    }
  }

  async produce<T>(opts: ProduceOptions<T>): Promise<RecordMetadata[]> {
    const encoded =
      opts.raw === true
        ? (opts.value as unknown as Buffer | string | null)
        : await this.sr.encode(opts.topic, opts.value);

    const message: Message = {
      key: opts.key ?? null,
      value: encoded as Buffer,
      headers: opts.headers,
      timestamp: Date.now().toString(),
    };

    return this.producer.send({
      topic: opts.topic,
      compression: CompressionTypes.GZIP,
      messages: [message],
    });
  }

  /** Escape hatch for advanced callers. */
  raw(): Producer {
    return this.producer;
  }
}
