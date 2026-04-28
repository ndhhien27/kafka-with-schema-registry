import { Module } from '@nestjs/common';
import { AppConfigModule } from './config/config.module';
import { AppLoggerModule } from './observability/logger.module';
import { MetricsModule } from './observability/metrics/metrics.module';
import { SchemaRegistryModule } from './schema-registry/schema-registry.module';
import { KafkaModule } from './kafka/kafka.module';
import { HealthModule } from './health/health.module';
import { UsersModule } from './features/users/users.module';
import { OrdersModule } from './features/orders/orders.module';

@Module({
  imports: [
    AppConfigModule,
    AppLoggerModule,
    MetricsModule,
    SchemaRegistryModule,
    KafkaModule,
    HealthModule,
    UsersModule,
    OrdersModule,
  ],
})
export class AppModule {}
