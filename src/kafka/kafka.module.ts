import { Global, Module } from '@nestjs/common';
import { DiscoveryModule } from '@nestjs/core';
import { AppConfigModule } from '../config/config.module';
import { AppConfigService } from '../config/app-config.service';
import { SchemaRegistryModule } from '../schema-registry/schema-registry.module';
import { KAFKA_CLIENT } from './kafka.tokens';
import { createKafkaClient } from './kafka.client';
import { ProducerService } from './producer.service';
import { ConsumerRegistry } from './consumer.registry';
import { KafkaAdminService } from './admin.service';

@Global()
@Module({
  imports: [DiscoveryModule, AppConfigModule, SchemaRegistryModule],
  providers: [
    {
      provide: KAFKA_CLIENT,
      inject: [AppConfigService],
      useFactory: (cfg: AppConfigService) => createKafkaClient(cfg),
    },
    ProducerService,
    ConsumerRegistry,
    KafkaAdminService,
  ],
  exports: [KAFKA_CLIENT, ProducerService, KafkaAdminService],
})
export class KafkaModule {}
