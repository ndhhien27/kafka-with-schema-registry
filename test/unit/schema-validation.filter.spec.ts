import type { ArgumentsHost } from '@nestjs/common';
import { SchemaValidationExceptionFilter } from '../../src/kafka/filters/schema-validation.filter';

const buildSerializationError = (message: string): Error => {
  const err = new Error(message);
  err.name = 'SerializationError';
  return err;
};

const buildRestError = (status: number, message: string): Error => {
  const err = new Error(message) as Error & { status: number };
  err.name = 'RestError';
  err.status = status;
  return err;
};

const buildHttpHost = (): {
  host: ArgumentsHost;
  res: { status: jest.Mock; json: jest.Mock };
} => {
  const res = {
    status: jest.fn().mockReturnThis(),
    json: jest.fn(),
  };
  const host = {
    getType: () => 'http',
    switchToHttp: () => ({
      getResponse: () => res,
      getRequest: () => ({ method: 'POST', url: '/users' }),
    }),
  } as unknown as ArgumentsHost;
  return { host, res };
};

describe('SchemaValidationExceptionFilter', () => {
  let filter: SchemaValidationExceptionFilter;

  beforeEach(() => {
    filter = new SchemaValidationExceptionFilter();
  });

  it('maps a SerializationError to 400 with parsed field path', () => {
    const { host, res } = buildHttpHost();
    filter.catch(
      buildSerializationError('Invalid message at email, expected "string", got 123'),
      host,
    );

    expect(res.status).toHaveBeenCalledWith(400);
    const body = res.json.mock.calls[0][0];
    expect(body.statusCode).toBe(400);
    expect(body.error).toBe('Bad Request');
    expect(body.paths).toEqual([['email']]);
    expect(body.reason).toContain('Invalid message');
  });

  it('parses nested paths like "order.items.0.sku"', () => {
    const { host, res } = buildHttpHost();
    filter.catch(
      buildSerializationError('Invalid message at order.items.0.sku, expected "string", got null'),
      host,
    );

    const body = res.json.mock.calls[0][0];
    expect(body.paths).toEqual([['order', 'items', '0', 'sku']]);
  });

  it('maps a RestError (e.g. subject not found) to 400 without paths', () => {
    const { host, res } = buildHttpHost();
    filter.catch(buildRestError(404, 'Subject not found'), host);

    expect(res.status).toHaveBeenCalledWith(400);
    const body = res.json.mock.calls[0][0];
    expect(body.paths).toBeUndefined();
    expect(body.reason).toContain('Subject not found');
  });

  it('rethrows non-SR errors so other filters can handle them', () => {
    const { host } = buildHttpHost();
    const other = new Error('totally unrelated');
    expect(() => filter.catch(other, host)).toThrow(other);
  });

  it('rethrows for non-HTTP contexts so KafkaDlqFilter still wins', () => {
    const host = { getType: () => 'rpc' } as unknown as ArgumentsHost;
    const err = buildSerializationError('Invalid message at x, expected "string", got 1');
    expect(() => filter.catch(err, host)).toThrow(err);
  });
});
