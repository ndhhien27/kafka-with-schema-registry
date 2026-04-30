import {
  CallHandler,
  ExecutionContext,
  Injectable,
  Logger,
  NestInterceptor,
} from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import { Observable, defer, from, throwError } from 'rxjs';
import { catchError, mergeMap } from 'rxjs/operators';
import {
  KAFKA_RETRY_METADATA,
  KafkaRetryOptions,
} from '../decorators/kafka-retry.decorator';
import { HandlerExhaustedError } from '../errors/poison-pill.error';

const DEFAULT_MAX_ATTEMPTS = 1;
const DEFAULT_BACKOFF_MS = [100, 500, 2000];

const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Per-handler retry/backoff for `@EventPattern` consumers, configured via @KafkaRetry.
 * On exhaustion, throws HandlerExhaustedError so KafkaDlqFilter can route to the DLQ.
 *
 * Bound globally via APP_INTERCEPTOR. Handlers without @KafkaRetry run once with no retry.
 */
@Injectable()
export class KafkaRetryInterceptor implements NestInterceptor {
  private readonly logger = new Logger(KafkaRetryInterceptor.name);

  constructor(private readonly reflector: Reflector) {}

  intercept(
    ctx: ExecutionContext,
    next: CallHandler,
  ): Observable<unknown> | Promise<Observable<unknown>> {
    if (ctx.getType<'rpc' | 'http'>() !== 'rpc') {
      return next.handle();
    }

    const opts =
      this.reflector.get<KafkaRetryOptions | undefined>(
        KAFKA_RETRY_METADATA,
        ctx.getHandler(),
      ) ?? {};

    const maxAttempts = opts.maxAttempts ?? DEFAULT_MAX_ATTEMPTS;
    if (maxAttempts <= 1) {
      return next.handle();
    }

    const backoff = opts.backoffMs ?? DEFAULT_BACKOFF_MS;
    const handlerName = ctx.getHandler().name;

    return defer(() => from(this.runWithRetry(next, maxAttempts, backoff, handlerName)));
  }

  private async runWithRetry(
    next: CallHandler,
    maxAttempts: number,
    backoff: number[],
    handlerName: string,
  ): Promise<unknown> {
    let attempt = 0;
    let lastError: unknown;

    while (attempt < maxAttempts) {
      try {
        return await observableToPromise(next.handle());
      } catch (err) {
        lastError = err;
        attempt += 1;
        if (attempt >= maxAttempts) break;
        const delay = backoff[Math.min(attempt - 1, backoff.length - 1)];
        this.logger.warn(
          `Handler ${handlerName} failed attempt=${attempt}/${maxAttempts}` +
            ` (${(err as Error).message}) — retrying in ${delay}ms`,
        );
        await sleep(delay);
      }
    }

    throw new HandlerExhaustedError(
      `Handler ${handlerName} exhausted ${maxAttempts} attempts`,
      attempt,
      lastError,
    );
  }
}

function observableToPromise(obs$: Observable<unknown>): Promise<unknown> {
  return new Promise((resolve, reject) => {
    let resolved = false;
    obs$.pipe(
      mergeMap((v) => {
        if (v && typeof (v as { then?: unknown }).then === 'function') {
          return from(v as Promise<unknown>);
        }
        return [v];
      }),
      catchError((err) => throwError(() => err)),
    ).subscribe({
      next: (val) => {
        if (!resolved) {
          resolved = true;
          resolve(val);
        }
      },
      error: (err) => reject(err),
      complete: () => {
        if (!resolved) {
          resolved = true;
          resolve(undefined);
        }
      },
    });
  });
}
