import {
  ArgumentsHost,
  Catch,
  ExceptionFilter,
  ExecutionContext,
  Logger,
} from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import { KafkaContext } from '@nestjs/microservices';
import { ProducerService } from '../producer.service';
import {
  KAFKA_RETRY_METADATA,
  KafkaRetryOptions,
} from '../decorators/kafka-retry.decorator';
import {
  HandlerExhaustedError,
  PoisonPillError,
} from '../errors/poison-pill.error';

/**
 * Routes any uncaught error from a Kafka @EventPattern / @MessagePattern handler
 * to `<topic>.DLQ`, preserving the original SR-framed Buffer (no re-encode) plus
 * structured headers compatible with the legacy ConsumerRegistry contract.
 *
 * The filter relies on KafkaContext.getMessage().value still holding the original
 * Buffer — NestJS's KafkaParser preserves it for SR payloads (leading 0x00 byte).
 */
@Catch()
export class KafkaDlqFilter implements ExceptionFilter {
  private readonly logger = new Logger(KafkaDlqFilter.name);

  constructor(
    private readonly producer: ProducerService,
    private readonly reflector: Reflector,
  ) {}

  async catch(exception: unknown, host: ArgumentsHost): Promise<void> {
    console.log('KafkaDlqFilter caught exception:', exception);
    if (host.getType<'rpc' | 'http'>() !== 'rpc') {
      throw exception;
    }

    const ctx = host.switchToRpc().getContext<KafkaContext>();
    if (typeof ctx?.getMessage !== 'function') {
      throw exception;
    }

    const error = exception instanceof Error ? exception : new Error(String(exception));
    const message = ctx.getMessage();
    const topic = ctx.getTopic();
    const partition = ctx.getPartition();

    // ExceptionFilters receive an ExecutionContextHost at runtime — typed as
    // ArgumentsHost but exposing getHandler/getClass. Cast to read handler metadata.
    const execHost = host as unknown as ExecutionContext;
    const handler =
      typeof execHost.getHandler === 'function' ? execHost.getHandler() : undefined;
    const retryOpts = handler
      ? this.reflector.get<KafkaRetryOptions | undefined>(
          KAFKA_RETRY_METADATA,
          handler,
        ) ?? {}
      : {};
    const dlqTopic = retryOpts.dlqTopic ?? `${topic}.DLQ`;
    const attempts =
      error instanceof HandlerExhaustedError
        ? error.attempts
        : retryOpts.maxAttempts ?? 1;

    const headers: Record<string, string> = {
      'x-original-topic': topic,
      'x-original-partition': String(partition),
      'x-original-offset': String(message.offset ?? ''),
      'x-error-name': error.name,
      'x-error-message': error.message.slice(0, 512),
      'x-attempts': String(attempts),
      'x-dlq-at': new Date().toISOString(),
    };

    try {
      await this.producer.produce({
        topic: dlqTopic,
        raw: true,
        key: message.key as Buffer | string | null | undefined,
        value: (message.value ?? null) as unknown as Buffer,
        headers,
      });
      this.logger.warn(
        `Routed message to DLQ topic=${dlqTopic} original_topic=${topic}` +
          ` offset=${message.offset} error=${error.message}`,
      );
    } catch (publishErr) {
      this.logger.error(
        `FAILED to route message to DLQ topic=${dlqTopic}: ${(publishErr as Error).message}`,
      );
      // Swallow: do not block the partition on DLQ failure. Surface via logs/metrics.
    }
  }
}

export { PoisonPillError, HandlerExhaustedError };
