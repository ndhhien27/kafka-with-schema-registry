import { z } from 'zod';

const booleanFromString = z
  .union([z.boolean(), z.string()])
  .transform((v) => (typeof v === 'boolean' ? v : v.toLowerCase() === 'true'));

const optionalNonEmpty = z
  .string()
  .optional()
  .transform((v) => (v && v.length > 0 ? v : undefined));

export const envSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  HTTP_PORT: z.coerce.number().int().min(1).max(65535).default(3000),
  LOG_LEVEL: z
    .enum(['trace', 'debug', 'info', 'warn', 'error', 'fatal'])
    .default('info'),

  KAFKA_BROKERS: z
    .string()
    .min(1)
    .transform((v) => v.split(',').map((s) => s.trim()).filter(Boolean)),
  KAFKA_CLIENT_ID: z.string().min(1).default('nestjs-kafka-avro'),
  KAFKA_GROUP_ID: z.string().min(1).default('nestjs-kafka-avro-group'),
  KAFKA_SASL_MECHANISM: z
    .enum(['plain', 'scram-sha-256', 'scram-sha-512'])
    .or(z.literal(''))
    .optional()
    .transform((v) => (v && v.length > 0 ? v : undefined)),
  KAFKA_SASL_USERNAME: optionalNonEmpty,
  KAFKA_SASL_PASSWORD: optionalNonEmpty,
  KAFKA_SSL: booleanFromString.default(false),

  SCHEMA_REGISTRY_URL: z.string().url(),
  SCHEMA_REGISTRY_USER: optionalNonEmpty,
  SCHEMA_REGISTRY_PASS: optionalNonEmpty,

  OTEL_ENABLED: booleanFromString.default(false),
  OTEL_EXPORTER_OTLP_ENDPOINT: z.string().url().default('http://localhost:4318'),
  OTEL_SERVICE_NAME: z.string().default('nestjs-kafka-avro'),
  METRICS_PORT: z.coerce.number().int().min(1).max(65535).default(9464),
  CONSUMER_LAG_POLL_MS: z.coerce.number().int().min(1000).default(15_000),
});

export type Env = z.infer<typeof envSchema>;

export function validateEnv(raw: Record<string, unknown>): Env {
  const parsed = envSchema.safeParse(raw);
  if (!parsed.success) {
    const issues = parsed.error.issues
      .map((i) => `  - ${i.path.join('.')}: ${i.message}`)
      .join('\n');
    throw new Error(`Invalid environment variables:\n${issues}`);
  }
  return parsed.data;
}
