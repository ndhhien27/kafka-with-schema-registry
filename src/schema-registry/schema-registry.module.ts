import { Global, Module } from '@nestjs/common';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import { AppConfigModule } from '../config/config.module';
import { AppConfigService } from '../config/app-config.service';
import {
  SCHEMA_REGISTRY_CLIENT,
  SchemaRegistryService,
} from './schema-registry.service';

@Global()
@Module({
  imports: [AppConfigModule],
  providers: [
    {
      provide: SCHEMA_REGISTRY_CLIENT,
      inject: [AppConfigService],
      useFactory: (cfg: AppConfigService) => {
        const { url, user, pass } = cfg.schemaRegistry;
        return new SchemaRegistry({
          host: url,
          auth: user && pass ? { username: user, password: pass } : undefined,
        });
      },
    },
    SchemaRegistryService,
  ],
  exports: [SchemaRegistryService, SCHEMA_REGISTRY_CLIENT],
})
export class SchemaRegistryModule {}
