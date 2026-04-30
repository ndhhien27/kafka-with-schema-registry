import { Global, Module } from '@nestjs/common';
import { APP_FILTER, APP_INTERCEPTOR, DiscoveryModule } from '@nestjs/core';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { AppConfigModule } from '../config/config.module';
import { AppConfigService } from '../config/app-config.service';
import { SchemaRegistryModule } from '../schema-registry/schema-registry.module';
import { SchemaRegistryService } from '../schema-registry/schema-registry.service';
import { KAFKA_CLIENT, KAFKA_CLIENT_PRODUCER } from './kafka.tokens';
import { createKafkaClient } from './kafka.client';
import { ProducerService } from './producer.service';
import { KafkaAdminService } from './admin.service';
import { AvroSerializer } from './serdes/avro.serializer';
import { KafkaDlqFilter } from './filters/kafka-dlq.filter';
import { SchemaValidationExceptionFilter } from './filters/schema-validation.filter';
import { KafkaRetryInterceptor } from './interceptors/kafka-retry.interceptor';

@Global()
@Module({
  imports: [
    DiscoveryModule,
    AppConfigModule,
    SchemaRegistryModule,
    ClientsModule.registerAsync([
      {
        name: KAFKA_CLIENT_PRODUCER,
        imports: [AppConfigModule, SchemaRegistryModule],
        inject: [AppConfigService, SchemaRegistryService],
        useFactory: (cfg: AppConfigService, sr: SchemaRegistryService) => {
          const k = cfg.kafka;
          const sasl =
            k.saslMechanism && k.saslUsername && k.saslPassword
              ? {
                  mechanism: k.saslMechanism,
                  username: k.saslUsername,
                  password: k.saslPassword,
                }
              : undefined;
          return {
            transport: Transport.KAFKA,
            options: {
              client: {
                clientId: `${k.clientId}-producer`,
                brokers: k.brokers,
                ssl: k.ssl,
                sasl: sasl as never,
              },
              producer: {
                idempotent: true,
                allowAutoTopicCreation: true,
              },
              serializer: new AvroSerializer(sr),
            },
          };
        },
      },
    ]),
  ],
  providers: [
    {
      provide: KAFKA_CLIENT,
      inject: [AppConfigService],
      useFactory: (cfg: AppConfigService) => createKafkaClient(cfg),
    },
    ProducerService,
    KafkaAdminService,
    {
      provide: APP_INTERCEPTOR,
      useClass: KafkaRetryInterceptor,
    },
    {
      provide: APP_FILTER,
      useClass: SchemaValidationExceptionFilter,
    },
    // {
    //   provide: APP_FILTER,
    //   useClass: KafkaDlqFilter,
    // },
  ],
  exports: [
    KAFKA_CLIENT,
    ProducerService,
    KafkaAdminService,
    ClientsModule,
  ],
})
export class KafkaModule {}
