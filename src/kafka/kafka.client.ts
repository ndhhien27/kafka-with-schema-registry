import { Logger } from '@nestjs/common';
import { Kafka, logLevel as KafkaLogLevel, SASLOptions } from 'kafkajs';
import { AppConfigService } from '../config/app-config.service';

const nestLogger = new Logger('Kafka');

const mapLevel = (level: KafkaLogLevel): 'log' | 'warn' | 'error' | 'debug' => {
  switch (level) {
    case KafkaLogLevel.ERROR:
    case KafkaLogLevel.NOTHING:
      return 'error';
    case KafkaLogLevel.WARN:
      return 'warn';
    case KafkaLogLevel.INFO:
      return 'log';
    case KafkaLogLevel.DEBUG:
    default:
      return 'debug';
  }
};

export function createKafkaClient(cfg: AppConfigService): Kafka {
  const k = cfg.kafka;

  const sasl: SASLOptions | undefined =
    k.saslMechanism && k.saslUsername && k.saslPassword
      ? ({
          mechanism: k.saslMechanism,
          username: k.saslUsername,
          password: k.saslPassword,
        } as SASLOptions)
      : undefined;

  return new Kafka({
    clientId: k.clientId,
    brokers: k.brokers,
    ssl: k.ssl,
    sasl,
    retry: { retries: 8, initialRetryTime: 300, maxRetryTime: 30_000 },
    logLevel: KafkaLogLevel.INFO,
    logCreator:
      () =>
      ({ level, log }) => {
        const { message, ...extra } = log;
        const method = mapLevel(level);
        nestLogger[method](`${message} ${JSON.stringify(extra)}`);
      },
  });
}
