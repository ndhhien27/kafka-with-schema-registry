import { validateEnv } from '../../src/config/env.schema';

describe('validateEnv', () => {
  const base = {
    KAFKA_BROKERS: 'localhost:9092,localhost:9093',
    SCHEMA_REGISTRY_URL: 'http://localhost:8081',
  };

  it('parses comma-separated brokers into array', () => {
    const env = validateEnv(base);
    expect(env.KAFKA_BROKERS).toEqual(['localhost:9092', 'localhost:9093']);
  });

  it('coerces ports from strings', () => {
    const env = validateEnv({ ...base, HTTP_PORT: '8080' });
    expect(env.HTTP_PORT).toBe(8080);
  });

  it('coerces booleans from strings', () => {
    const env = validateEnv({ ...base, KAFKA_SSL: 'true', OTEL_ENABLED: 'TRUE' });
    expect(env.KAFKA_SSL).toBe(true);
    expect(env.OTEL_ENABLED).toBe(true);
  });

  it('defaults optional values', () => {
    const env = validateEnv(base);
    expect(env.NODE_ENV).toBe('development');
    expect(env.LOG_LEVEL).toBe('info');
    expect(env.METRICS_PORT).toBe(9464);
    expect(env.KAFKA_SSL).toBe(false);
  });

  it('rejects invalid URL', () => {
    expect(() =>
      validateEnv({ ...base, SCHEMA_REGISTRY_URL: 'not-a-url' }),
    ).toThrow(/SCHEMA_REGISTRY_URL/);
  });

  it('rejects missing brokers', () => {
    expect(() => validateEnv({ SCHEMA_REGISTRY_URL: 'http://localhost:8081' })).toThrow(
      /KAFKA_BROKERS/,
    );
  });

  it('treats empty SASL fields as undefined', () => {
    const env = validateEnv({ ...base, KAFKA_SASL_USERNAME: '', KAFKA_SASL_PASSWORD: '' });
    expect(env.KAFKA_SASL_USERNAME).toBeUndefined();
    expect(env.KAFKA_SASL_PASSWORD).toBeUndefined();
  });
});
