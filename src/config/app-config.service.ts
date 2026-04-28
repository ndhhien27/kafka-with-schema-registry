import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import type { Env } from './env.schema';

@Injectable()
export class AppConfigService {
  constructor(private readonly config: ConfigService<Env, true>) {}

  get nodeEnv(): Env['NODE_ENV'] {
    return this.config.get('NODE_ENV', { infer: true });
  }

  get isProduction(): boolean {
    return this.nodeEnv === 'production';
  }

  get httpPort(): number {
    return this.config.get('HTTP_PORT', { infer: true });
  }

  get logLevel(): Env['LOG_LEVEL'] {
    return this.config.get('LOG_LEVEL', { infer: true });
  }

  get kafka() {
    return {
      brokers: this.config.get('KAFKA_BROKERS', { infer: true }),
      clientId: this.config.get('KAFKA_CLIENT_ID', { infer: true }),
      groupId: this.config.get('KAFKA_GROUP_ID', { infer: true }),
      ssl: this.config.get('KAFKA_SSL', { infer: true }),
      saslMechanism: this.config.get('KAFKA_SASL_MECHANISM', { infer: true }),
      saslUsername: this.config.get('KAFKA_SASL_USERNAME', { infer: true }),
      saslPassword: this.config.get('KAFKA_SASL_PASSWORD', { infer: true }),
    };
  }

  get schemaRegistry() {
    return {
      url: this.config.get('SCHEMA_REGISTRY_URL', { infer: true }),
      user: this.config.get('SCHEMA_REGISTRY_USER', { infer: true }),
      pass: this.config.get('SCHEMA_REGISTRY_PASS', { infer: true }),
    };
  }

  get otel() {
    return {
      enabled: this.config.get('OTEL_ENABLED', { infer: true }),
      endpoint: this.config.get('OTEL_EXPORTER_OTLP_ENDPOINT', { infer: true }),
      serviceName: this.config.get('OTEL_SERVICE_NAME', { infer: true }),
    };
  }

  get metricsPort(): number {
    return this.config.get('METRICS_PORT', { infer: true });
  }

  get consumerLagPollMs(): number {
    return this.config.get('CONSUMER_LAG_POLL_MS', { infer: true });
  }
}
