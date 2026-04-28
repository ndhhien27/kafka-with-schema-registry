// IMPORTANT: keep this as the first import so OpenTelemetry can patch kafkajs.
import './observability/tracing';

import { NestFactory } from '@nestjs/core';
import { Logger as NestLogger, ValidationPipe } from '@nestjs/common';
import { Logger } from 'nestjs-pino';
import { AppModule } from './app.module';
import { AppConfigService } from './config/app-config.service';

// Silence noisy upstream warnings that are out of our control:
//   - TimeoutNegativeWarning: kafkajs retry deadline math during cluster discovery
//   - DEP0190: ts-node/pnpm child_process shell-arg deprecation
// Everything else still surfaces.
// const SUPPRESSED_WARNINGS = new Set(['TimeoutNegativeWarning', 'DeprecationWarning']);
// process.on('warning', (warning: NodeJS.ErrnoException) => {
//   if (SUPPRESSED_WARNINGS.has(warning.name) && warning.code === 'DEP0190') return;
//   if (warning.name === 'TimeoutNegativeWarning') return;
//   // eslint-disable-next-line no-console
//   console.warn(warning);
// });

// const bootstrapLogger = new NestLogger('Bootstrap');

// // Guard against kafkajs protocol errors (e.g. transient UNKNOWN_TOPIC_OR_PARTITION
// // during metadata refresh) and other stray rejections taking the process down.
// process.on('unhandledRejection', (reason) => {
//   const err = reason instanceof Error ? reason : new Error(String(reason));
//   bootstrapLogger.error(`Unhandled rejection: ${err.message}`, err.stack);
// });

// process.on('uncaughtException', (err) => {
//   bootstrapLogger.error(`Uncaught exception: ${err.message}`, err.stack);
// });

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(AppModule, { bufferLogs: true });
  app.useLogger(app.get(Logger));
  app.useGlobalPipes(new ValidationPipe({ whitelist: true, transform: true }));
  app.enableShutdownHooks();

  const cfg = app.get(AppConfigService);
  await app.listen(cfg.httpPort);
  app.get(Logger).log(
    `HTTP listening on :${cfg.httpPort} (env=${cfg.nodeEnv})`,
    'Bootstrap',
  );
}

void bootstrap();
