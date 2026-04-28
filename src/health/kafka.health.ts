import { Injectable } from '@nestjs/common';
import {
  HealthCheckError,
  HealthIndicator,
  HealthIndicatorResult,
} from '@nestjs/terminus';
import { KafkaAdminService } from '../kafka/admin.service';
import { AppConfigService } from '../config/app-config.service';

@Injectable()
export class KafkaHealthIndicator extends HealthIndicator {
  constructor(
    private readonly admin: KafkaAdminService,
    private readonly appConfig: AppConfigService,
  ) {
    super();
  }

  async checkKafka(key = 'kafka'): Promise<HealthIndicatorResult> {
    try {
      const topics = await this.admin.getAdmin().listTopics();
      return this.getStatus(key, true, { topicCount: topics.length });
    } catch (err) {
      throw new HealthCheckError(
        'Kafka down',
        this.getStatus(key, false, { message: (err as Error).message }),
      );
    }
  }

  async checkSchemaRegistry(key = 'schemaRegistry'): Promise<HealthIndicatorResult> {
    const { url, user, pass } = this.appConfig.schemaRegistry;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 2_000);

    try {
      const headers: Record<string, string> = { Accept: 'application/json' };
      if (user && pass) {
        headers.Authorization = `Basic ${Buffer.from(`${user}:${pass}`).toString('base64')}`;
      }

      const res = await fetch(`${url.replace(/\/$/, '')}/subjects`, {
        method: 'GET',
        headers,
        signal: controller.signal,
      });

      if (!res.ok) {
        throw new Error(`Schema Registry returned HTTP ${res.status}`);
      }

      const subjects = (await res.json()) as string[];
      return this.getStatus(key, true, { subjectCount: subjects.length });
    } catch (err) {
      throw new HealthCheckError(
        'Schema Registry down',
        this.getStatus(key, false, { message: (err as Error).message }),
      );
    } finally {
      clearTimeout(timeout);
    }
  }
}
