import {
  Injectable,
  Logger,
  OnApplicationShutdown,
  OnModuleInit,
} from '@nestjs/common';
import { InjectMetric } from '@willsoto/nestjs-prometheus';
import type { Gauge } from 'prom-client';
import { AppConfigService } from '../../config/app-config.service';
import { KafkaAdminService } from '../../kafka/admin.service';

/**
 * Polls Kafka admin for consumer-group lag and exposes as a Prometheus gauge.
 * Lag = latest partition offset - committed group offset.
 *
 * All broker/admin calls here are best-effort: transient protocol errors
 * (e.g. UNKNOWN_TOPIC_OR_PARTITION before a topic is first written to) are
 * swallowed so they never take the app down.
 */
@Injectable()
export class ConsumerLagService implements OnModuleInit, OnApplicationShutdown {
  private readonly logger = new Logger(ConsumerLagService.name);
  private timer: NodeJS.Timeout | null = null;
  private kickoffTimer: NodeJS.Timeout | null = null;
  private stopped = false;

  constructor(
    @InjectMetric('kafka_consumer_lag') private readonly gauge: Gauge<string>,
    private readonly admin: KafkaAdminService,
    private readonly cfg: AppConfigService,
  ) {}

  async onModuleInit(): Promise<void> {
    const interval = this.cfg.consumerLagPollMs;
    this.timer = setInterval(() => {
      this.collect().catch((err) =>
        this.logger.warn(`collect() rejected: ${(err as Error).message}`),
      );
    }, interval);
    this.kickoffTimer = setTimeout(() => {
      this.collect().catch((err) =>
        this.logger.warn(`initial collect() rejected: ${(err as Error).message}`),
      );
    }, 5_000);
    this.logger.log(`Consumer-lag poll started (interval=${interval}ms)`);
  }

  async onApplicationShutdown(): Promise<void> {
    this.stopped = true;
    if (this.timer) clearInterval(this.timer);
    if (this.kickoffTimer) clearTimeout(this.kickoffTimer);
  }

  private async collect(): Promise<void> {
    if (this.stopped) return;
    const groupId = this.cfg.kafka.groupId;
    const admin = this.admin.getAdmin();

    let offsets: Awaited<ReturnType<typeof admin.fetchOffsets>>;
    try {
      const description = await admin.describeGroups([groupId]);
      const group = description.groups[0];
      if (!group || group.members.length === 0) return;
      offsets = await admin.fetchOffsets({ groupId });
    } catch (err) {
      this.logger.debug?.(`describe/fetchOffsets failed: ${(err as Error).message}`);
      return;
    }

    const topics = new Set<string>();
    for (const entry of offsets) topics.add(entry.topic);
    if (topics.size === 0) return;

    const latestOffsetsByTopic = new Map<string, Map<number, string>>();
    for (const topic of topics) {
      try {
        const latest = await admin.fetchTopicOffsets(topic);
        const perPartition = new Map<number, string>();
        for (const p of latest) perPartition.set(p.partition, p.offset);
        latestOffsetsByTopic.set(topic, perPartition);
      } catch (err) {
        // Common case: topic does not exist yet (UNKNOWN_TOPIC_OR_PARTITION).
        // Skip silently at DEBUG level so the poll keeps moving.
        this.logger.debug?.(
          `fetchTopicOffsets(${topic}) failed: ${(err as Error).message}`,
        );
      }
    }

    for (const entry of offsets) {
      const latestForTopic = latestOffsetsByTopic.get(entry.topic);
      if (!latestForTopic) continue;
      for (const p of entry.partitions) {
        const latest = latestForTopic.get(p.partition);
        if (!latest) continue;
        try {
          const lag = Number(BigInt(latest) - BigInt(p.offset));
          this.gauge.set(
            { topic: entry.topic, partition: String(p.partition), group: groupId },
            Number.isFinite(lag) ? lag : 0,
          );
        } catch {
          // offset strings should be numeric; ignore otherwise
        }
      }
    }
  }
}
