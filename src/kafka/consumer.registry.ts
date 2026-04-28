import {
  Inject,
  Injectable,
  Logger,
  OnApplicationShutdown,
  OnModuleInit,
} from '@nestjs/common';
import { DiscoveryService, MetadataScanner, Reflector } from '@nestjs/core';
import { InstanceWrapper } from '@nestjs/core/injector/instance-wrapper';
import { Consumer, EachMessagePayload, Kafka } from 'kafkajs';
import { KAFKA_CLIENT, KAFKA_SUBSCRIBE_METADATA } from './kafka.tokens';
import { AppConfigService } from '../config/app-config.service';
import { SchemaRegistryService } from '../schema-registry/schema-registry.service';
import { ProducerService } from './producer.service';
import type {
  DecodedKafkaMessage,
  KafkaSubscribeOptions,
  KafkaSubscriberMetadata,
} from './interfaces/kafka.interfaces';
import {
  HandlerExhaustedError,
  PoisonPillError,
} from './errors/poison-pill.error';

interface Subscriber {
  instance: unknown;
  handler: (msg: DecodedKafkaMessage) => Promise<void> | void;
  options: Required<
    Pick<KafkaSubscribeOptions, 'topic' | 'groupId' | 'fromBeginning' | 'raw' | 'maxAttempts'>
  > & {
    backoffMs: number[];
    dlqTopic: string;
  };
}

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

@Injectable()
export class ConsumerRegistry implements OnModuleInit, OnApplicationShutdown {
  private readonly logger = new Logger(ConsumerRegistry.name);
  private readonly consumers: { consumer: Consumer; groupId: string }[] = [];
  private shuttingDown = false;

  constructor(
    @Inject(KAFKA_CLIENT) private readonly kafka: Kafka,
    private readonly discovery: DiscoveryService,
    private readonly scanner: MetadataScanner,
    private readonly reflector: Reflector,
    private readonly cfg: AppConfigService,
    private readonly sr: SchemaRegistryService,
    private readonly producer: ProducerService,
  ) {}

  async onModuleInit(): Promise<void> {
    const subscribers = this.discoverSubscribers();
    if (subscribers.length === 0) {
      this.logger.log('No @KafkaSubscribe handlers discovered');
      return;
    }

    // Group handlers by (groupId, topic). One consumer per groupId, subscribing to multiple topics.
    const byGroup = new Map<string, Subscriber[]>();
    for (const sub of subscribers) {
      const key = sub.options.groupId;
      const arr = byGroup.get(key) ?? [];
      arr.push(sub);
      byGroup.set(key, arr);
    }

    for (const [groupId, subs] of byGroup) {
      await this.startConsumerForGroup(groupId, subs);
    }
  }

  async onApplicationShutdown(signal?: string): Promise<void> {
    this.shuttingDown = true;
    this.logger.log(
      `Disconnecting ${this.consumers.length} consumer(s) (signal=${signal ?? 'n/a'})`,
    );
    await Promise.all(
      this.consumers.map(async ({ consumer, groupId }) => {
        try {
          await consumer.disconnect();
          this.logger.log(`Consumer group=${groupId} disconnected`);
        } catch (err) {
          this.logger.error(
            `Error disconnecting consumer group=${groupId}: ${(err as Error).message}`,
          );
        }
      }),
    );
  }

  private discoverSubscribers(): Subscriber[] {
    const providers = this.discovery.getProviders();
    const result: Subscriber[] = [];

    for (const wrapper of providers) {
      const { instance } = wrapper as InstanceWrapper;
      if (!instance || typeof instance !== 'object') continue;
      const proto = Object.getPrototypeOf(instance);
      if (!proto) continue;

      const methods = this.scanner.getAllMethodNames(proto);
      for (const methodName of methods) {
        const methodRef = (instance as Record<string, unknown>)[methodName];
        if (typeof methodRef !== 'function') continue;
        const meta = this.reflector.get<KafkaSubscribeOptions | undefined>(
          KAFKA_SUBSCRIBE_METADATA,
          methodRef as (...args: unknown[]) => unknown,
        );
        if (!meta) continue;

        const defaults = this.withDefaults(meta);
        result.push({
          instance,
          handler: (instance as Record<string, Function>)[methodName].bind(instance),
          options: defaults,
        });
        this.logger.log(
          `Discovered handler ${(instance as object).constructor.name}.${methodName}` +
            ` topic=${defaults.topic} group=${defaults.groupId}`,
        );
      }
    }
    return result;
  }

  private withDefaults(meta: KafkaSubscribeOptions): Subscriber['options'] {
    return {
      topic: meta.topic,
      groupId: meta.groupId ?? this.cfg.kafka.groupId,
      fromBeginning: meta.fromBeginning ?? false,
      raw: meta.raw ?? false,
      maxAttempts: meta.maxAttempts ?? 3,
      backoffMs: meta.backoffMs ?? [100, 500, 2000],
      dlqTopic: meta.dlqTopic ?? `${meta.topic}.DLQ`,
    } satisfies Subscriber['options'];
  }

  private async startConsumerForGroup(
    groupId: string,
    subs: Subscriber[],
  ): Promise<void> {
    const consumer = this.kafka.consumer({
      groupId,
      sessionTimeout: 30_000,
      heartbeatInterval: 3_000,
      allowAutoTopicCreation: true,
    });

    consumer.on(consumer.events.GROUP_JOIN, (e) =>
      this.logger.log(
        `GROUP_JOIN group=${groupId} memberId=${e.payload.memberId}` +
          ` leader=${e.payload.leaderId} assignment=${JSON.stringify(
            e.payload.memberAssignment,
          )}`,
      ),
    );
    consumer.on(consumer.events.CRASH, (e) =>
      this.logger.error(
        `Consumer CRASH group=${groupId}: ${e.payload.error.message}`,
      ),
    );
    consumer.on(consumer.events.DISCONNECT, () =>
      this.logger.log(`Consumer DISCONNECT group=${groupId}`),
    );

    await consumer.connect();

    const topicToSub = new Map<string, Subscriber>();
    for (const sub of subs) {
      topicToSub.set(sub.options.topic, sub);
      await consumer.subscribe({
        topic: sub.options.topic,
        fromBeginning: sub.options.fromBeginning,
      });
    }

    this.consumers.push({ consumer, groupId });

    await consumer.run({
      autoCommit: true,
      eachMessage: async (payload) => {
        const sub = topicToSub.get(payload.topic);
        if (!sub) {
          this.logger.warn(
            `No subscriber registered for topic=${payload.topic} on group=${groupId}`,
          );
          return;
        }
        await this.dispatch(sub, payload);
      },
    });
  }

  private async dispatch(
    sub: Subscriber,
    payload: EachMessagePayload,
  ): Promise<void> {
    const { partition, topic } = payload;

    let decoded: DecodedKafkaMessage;
    try {
      decoded = await this.decodeMessage(sub, payload);
    } catch (err) {
      await this.routeToDlq(
        sub,
        payload,
        new PoisonPillError(
          `Failed to decode message on topic=${topic}: ${(err as Error).message}`,
          err,
        ),
        0,
      );
      return;
    }

    let attempt = 0;
    let lastError: unknown;
    while (attempt < sub.options.maxAttempts) {
      if (this.shuttingDown) {
        this.logger.warn(
          `Shutting down, abandoning handler for topic=${topic} partition=${partition}`,
        );
        return;
      }
      try {
        await sub.handler(decoded);
        return;
      } catch (err) {
        lastError = err;
        attempt += 1;
        if (attempt >= sub.options.maxAttempts) break;
        const delay =
          sub.options.backoffMs[
            Math.min(attempt - 1, sub.options.backoffMs.length - 1)
          ];
        this.logger.warn(
          `Handler failed topic=${topic} partition=${partition}` +
            ` attempt=${attempt}/${sub.options.maxAttempts}: ${(err as Error).message}` +
            ` — retrying in ${delay}ms`,
        );
        await sleep(delay);
      }
    }

    await this.routeToDlq(
      sub,
      payload,
      new HandlerExhaustedError(
        `Handler exhausted ${sub.options.maxAttempts} attempts on topic=${topic}`,
        attempt,
        lastError,
      ),
      attempt,
    );
  }

  private async decodeMessage(
    sub: Subscriber,
    payload: EachMessagePayload,
  ): Promise<DecodedKafkaMessage> {
    const { message, partition, topic } = payload;
    const value =
      sub.options.raw || message.value === null
        ? (message.value as unknown)
        : await this.sr.decode(message.value);

    const headers: Record<string, Buffer | string | undefined> = {};
    if (message.headers) {
      for (const [k, v] of Object.entries(message.headers)) {
        headers[k] = v as Buffer | string | undefined;
      }
    }

    return {
      topic,
      partition,
      offset: message.offset,
      timestamp: message.timestamp,
      key: message.key,
      headers,
      value,
      raw: message,
    };
  }

  private async routeToDlq(
    sub: Subscriber,
    payload: EachMessagePayload,
    error: Error,
    attempts: number,
  ): Promise<void> {
    const { message, topic, partition } = payload;
    const headers = {
      'x-original-topic': topic,
      'x-original-partition': String(partition),
      'x-original-offset': message.offset,
      'x-error-name': error.name,
      'x-error-message': error.message.slice(0, 512),
      'x-attempts': String(attempts),
      'x-dlq-at': new Date().toISOString(),
    } as Record<string, string>;

    try {
      await this.producer.produce({
        topic: sub.options.dlqTopic,
        raw: true,
        key: message.key,
        value: message.value as unknown as Buffer,
        headers,
      });
      this.logger.warn(
        `Routed message to DLQ topic=${sub.options.dlqTopic}` +
          ` original_topic=${topic} offset=${message.offset} error=${error.message}`,
      );
    } catch (err) {
      this.logger.error(
        `FAILED to route message to DLQ topic=${sub.options.dlqTopic}: ${(err as Error).message}`,
      );
      // Intentionally swallow: we do not want to block the partition on DLQ failure.
      // Surface via logs + metrics instead.
    }
  }
}
