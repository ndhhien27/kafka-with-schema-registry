/**
 * Must be imported BEFORE any other application code (specifically before kafkajs)
 * so OpenTelemetry can patch the libraries.
 *
 * Import as the very first line of main.ts: `import './observability/tracing';`
 */
import { NodeSDK } from '@opentelemetry/sdk-node';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import { Resource } from '@opentelemetry/resources';
import { SemanticResourceAttributes } from '@opentelemetry/semantic-conventions';
import { KafkaJsInstrumentation } from '@opentelemetry/instrumentation-kafkajs';
import { getNodeAutoInstrumentations } from '@opentelemetry/auto-instrumentations-node';

const enabled = (process.env.OTEL_ENABLED ?? 'false').toLowerCase() === 'true';
const endpoint = process.env.OTEL_EXPORTER_OTLP_ENDPOINT ?? 'http://localhost:4318';
const serviceName = process.env.OTEL_SERVICE_NAME ?? 'nestjs-kafka-avro';

if (enabled) {
  const sdk = new NodeSDK({
    resource: new Resource({
      [SemanticResourceAttributes.SERVICE_NAME]: serviceName,
    }),
    traceExporter: new OTLPTraceExporter({ url: `${endpoint}/v1/traces` }),
    instrumentations: [
      getNodeAutoInstrumentations({
        '@opentelemetry/instrumentation-fs': { enabled: false },
      }),
      new KafkaJsInstrumentation(),
    ],
  });

  try {
    sdk.start();
    // eslint-disable-next-line no-console
    console.log(`[otel] tracing enabled service=${serviceName} endpoint=${endpoint}`);
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error('[otel] failed to start SDK', err);
  }

  process.on('SIGTERM', () => {
    sdk
      .shutdown()
      .catch((err) => console.error('[otel] shutdown error', err))
      .finally(() => process.exit(0));
  });
}
