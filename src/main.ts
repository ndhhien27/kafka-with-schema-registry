// IMPORTANT: keep this as the first import so OpenTelemetry can patch kafkajs.
import './observability/tracing';

import { NestFactory } from '@nestjs/core';
import { Logger as NestLogger, ValidationPipe } from '@nestjs/common';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { Logger } from 'nestjs-pino';
import { AppModule } from './app.module';
import { AppConfigService } from './config/app-config.service';
import { SchemaRegistryService } from './schema-registry/schema-registry.service';
import { AvroSerializer } from './kafka/serdes/avro.serializer';
import { AvroDeserializer } from './kafka/serdes/avro.deserializer';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(AppModule, { bufferLogs: true });
  app.useLogger(app.get(Logger));
  app.useGlobalPipes(new ValidationPipe({ whitelist: true, transform: true }));
  app.enableShutdownHooks();

  const cfg = app.get(AppConfigService);
  const sr = app.get(SchemaRegistryService);

  const k = cfg.kafka;
  const sasl =
    k.saslMechanism && k.saslUsername && k.saslPassword
      ? {
          mechanism: k.saslMechanism,
          username: k.saslUsername,
          password: k.saslPassword,
        }
      : undefined;

  app.connectMicroservice<MicroserviceOptions>(
    {
      transport: Transport.KAFKA,
      options: {
        client: {
          clientId: `${k.clientId}-microservice`,
          brokers: k.brokers,
          ssl: k.ssl,
          sasl: sasl as never,
        },
        consumer: {
          groupId: k.groupId,
          allowAutoTopicCreation: true,
        },
        subscribe: { fromBeginning: false },
        run: { autoCommit: true },
        serializer: new AvroSerializer(sr),
        deserializer: new AvroDeserializer(sr),
      },
    },
    { inheritAppConfig: true },
  );

  await app.startAllMicroservices();
  await app.listen(cfg.httpPort);
  app.get(Logger).log(
    `HTTP listening on :${cfg.httpPort} (env=${cfg.nodeEnv})`,
    'Bootstrap',
  );
}

void bootstrap();
