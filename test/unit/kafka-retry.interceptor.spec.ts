import type { CallHandler, ExecutionContext } from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import { of, defer, throwError } from 'rxjs';
import { KafkaRetryInterceptor } from '../../src/kafka/interceptors/kafka-retry.interceptor';
import {
  KAFKA_RETRY_METADATA,
  KafkaRetryOptions,
} from '../../src/kafka/decorators/kafka-retry.decorator';
import { HandlerExhaustedError } from '../../src/kafka/errors/poison-pill.error';

const exec = (handler: () => void, type: 'rpc' | 'http' = 'rpc'): ExecutionContext =>
  ({
    getType: () => type,
    getHandler: () => handler,
    getClass: () => class {},
    switchToRpc: () => ({}),
    switchToHttp: () => ({}),
    switchToWs: () => ({}),
  }) as unknown as ExecutionContext;

const tag = (handler: () => void, options: KafkaRetryOptions): void => {
  Reflect.defineMetadata(KAFKA_RETRY_METADATA, options, handler);
};

const promiseFromObservable = async (intercept: ReturnType<KafkaRetryInterceptor['intercept']>): Promise<unknown> => {
  const obs$ = await Promise.resolve(intercept);
  return new Promise((resolve, reject) => {
    obs$.subscribe({ next: resolve, error: reject, complete: () => resolve(undefined) });
  });
};

describe('KafkaRetryInterceptor', () => {
  let interceptor: KafkaRetryInterceptor;

  beforeEach(() => {
    interceptor = new KafkaRetryInterceptor(new Reflector());
  });

  it('passes through when no @KafkaRetry metadata is present', async () => {
    const handler = function noRetry() {};
    const next: CallHandler = { handle: () => of('ok') };

    const result = await promiseFromObservable(interceptor.intercept(exec(handler), next));
    expect(result).toBe('ok');
  });

  it('retries up to maxAttempts then throws HandlerExhaustedError', async () => {
    const handler = function withRetry() {};
    tag(handler, { maxAttempts: 3, backoffMs: [1, 1, 1] });

    let attempts = 0;
    const next: CallHandler = {
      handle: () =>
        defer(() => {
          attempts += 1;
          return throwError(() => new Error(`fail ${attempts}`));
        }),
    };

    await expect(
      promiseFromObservable(interceptor.intercept(exec(handler), next)),
    ).rejects.toBeInstanceOf(HandlerExhaustedError);
    expect(attempts).toBe(3);
  });

  it('returns successfully when a retried attempt eventually succeeds', async () => {
    const handler = function flaky() {};
    tag(handler, { maxAttempts: 3, backoffMs: [1, 1] });

    let attempts = 0;
    const next: CallHandler = {
      handle: () =>
        defer(() => {
          attempts += 1;
          if (attempts < 2) return throwError(() => new Error('transient'));
          return of('ok');
        }),
    };

    const result = await promiseFromObservable(interceptor.intercept(exec(handler), next));
    expect(result).toBe('ok');
    expect(attempts).toBe(2);
  });

  it('passes through for non-rpc contexts', async () => {
    const handler = function http() {};
    tag(handler, { maxAttempts: 3 });
    const next: CallHandler = { handle: () => of('http-ok') };

    const result = await promiseFromObservable(
      interceptor.intercept(exec(handler, 'http'), next),
    );
    expect(result).toBe('http-ok');
  });
});
