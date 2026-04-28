import { Module } from '@nestjs/common';
import { LoggerModule } from 'nestjs-pino';
import { AppConfigModule } from '../config/config.module';
import { AppConfigService } from '../config/app-config.service';

@Module({
  imports: [
    LoggerModule.forRootAsync({
      imports: [AppConfigModule],
      inject: [AppConfigService],
      useFactory: (cfg: AppConfigService) => ({
        pinoHttp: {
          level: cfg.logLevel,
          redact: {
            paths: [
              'req.headers.authorization',
              'req.headers.cookie',
              '*.password',
              '*.token',
              '*.apiKey',
              'KAFKA_SASL_PASSWORD',
              'SCHEMA_REGISTRY_PASS',
            ],
            censor: '[REDACTED]',
          },
          transport: cfg.isProduction
            ? undefined
            : {
                target: 'pino-pretty',
                options: { singleLine: true, colorize: true },
              },
          autoLogging: { ignore: (req) => req.url === '/health' || req.url === '/metrics' },
        },
      }),
    }),
  ],
})
export class AppLoggerModule {}
