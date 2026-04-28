import { Module } from '@nestjs/common';
import {
  PrometheusModule,
  makeGaugeProvider,
} from '@willsoto/nestjs-prometheus';
import { ConsumerLagService } from './consumer-lag.service';

@Module({
  imports: [
    PrometheusModule.register({
      defaultMetrics: { enabled: true },
      defaultLabels: { app: 'nestjs-kafka-avro' },
    }),
  ],
  providers: [
    makeGaugeProvider({
      name: 'kafka_consumer_lag',
      help: 'Lag (latest offset - committed) per topic/partition/group',
      labelNames: ['topic', 'partition', 'group'],
    }),
    ConsumerLagService,
  ],
  exports: [PrometheusModule],
})
export class MetricsModule {}
