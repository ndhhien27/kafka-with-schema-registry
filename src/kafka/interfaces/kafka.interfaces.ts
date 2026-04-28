import type { KafkaMessage } from 'kafkajs';

export interface KafkaSubscribeOptions {
  topic: string;
  groupId?: string;
  fromBeginning?: boolean;
  /** If true, skip Schema Registry decode and deliver raw message. */
  raw?: boolean;
  /** Max handler attempts (including first). Defaults to 3. */
  maxAttempts?: number;
  /** Backoff delays between attempts (ms). Last value repeats if attempts > array length. */
  backoffMs?: number[];
  /** Override DLQ topic name. Defaults to `<topic>.DLQ`. */
  dlqTopic?: string;
}

export interface KafkaSubscriberMetadata extends KafkaSubscribeOptions {
  propertyKey: string;
}

export interface DecodedKafkaMessage<T = unknown> {
  topic: string;
  partition: number;
  offset: string;
  timestamp: string;
  key: Buffer | null;
  headers: Record<string, Buffer | string | undefined>;
  value: T;
  raw: KafkaMessage;
}
