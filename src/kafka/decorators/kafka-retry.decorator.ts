import { SetMetadata } from '@nestjs/common';

export const KAFKA_RETRY_METADATA = 'kafka:retry';

export interface KafkaRetryOptions {
  /** Max handler attempts (including the first). Defaults to 3. */
  maxAttempts?: number;
  /** Backoff delays between attempts (ms). Last value repeats if attempts > array length. */
  backoffMs?: number[];
  /** Override DLQ topic name. Defaults to `<topic>.DLQ`. */
  dlqTopic?: string;
}

/**
 * Per-handler retry policy for `@EventPattern` consumers.
 * Read by KafkaRetryInterceptor; on exhaustion the error propagates to KafkaDlqFilter.
 */
export const KafkaRetry = (options: KafkaRetryOptions = {}) =>
  SetMetadata(KAFKA_RETRY_METADATA, options);
