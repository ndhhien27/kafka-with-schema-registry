import {
  ArgumentsHost,
  Catch,
  ExceptionFilter,
  HttpStatus,
  Logger,
} from '@nestjs/common';
import {
  extractValidationPaths,
  isSchemaRegistryError,
  isSchemaRegistryValidationError,
} from '../serdes/sr-errors';

interface HttpResponseLike {
  status(code: number): HttpResponseLike;
  json(payload: unknown): unknown;
}

interface HttpRequestLike {
  method?: string;
  url?: string;
}

/**
 * Converts Schema Registry encode errors raised on the HTTP request path into a
 * 400 Bad Request response. Without this filter, a payload that fails Avro
 * validation propagates an unhandled rejection and may crash the process.
 *
 * Non-HTTP contexts (Kafka @EventPattern handlers) re-throw so KafkaDlqFilter
 * keeps owning consume-side decode failures.
 */
@Catch()
export class SchemaValidationExceptionFilter implements ExceptionFilter {
  private readonly logger = new Logger(SchemaValidationExceptionFilter.name);

  catch(exception: unknown, host: ArgumentsHost): void {
    console.log('SchemaValidationExceptionFilter caught exception:', exception);

    if (!isSchemaRegistryError(exception)) {
      throw exception;
    }
    if (host.getType<'http' | 'rpc' | 'ws'>() !== 'http') {
      throw exception;
    }

    const ctx = host.switchToHttp();
    const res = ctx.getResponse<HttpResponseLike>();
    const req = ctx.getRequest<HttpRequestLike>();

    const paths = isSchemaRegistryValidationError(exception)
      ? extractValidationPaths(exception)
      : undefined;

    this.logger.warn(
      `Schema validation rejected request method=${req?.method ?? '?'}` +
        ` url=${req?.url ?? '?'} reason=${exception.message}` +
        (paths ? ` paths=${JSON.stringify(paths)}` : ''),
    );

    res.status(HttpStatus.BAD_REQUEST).json({
      statusCode: HttpStatus.BAD_REQUEST,
      error: 'Bad Request',
      message: 'Payload does not match the registered schema',
      reason: exception.message,
      paths,
    });
  }
}
