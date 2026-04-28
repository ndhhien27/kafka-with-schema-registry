/**
 * Testcontainers bootstrap for Kafka (KRaft) + Schema Registry.
 * Shared across integration specs.
 */
import {
  GenericContainer,
  Network,
  StartedNetwork,
  StartedTestContainer,
  Wait,
} from 'testcontainers';

export interface KafkaStack {
  network: StartedNetwork;
  kafka: StartedTestContainer;
  schemaRegistry: StartedTestContainer;
  brokersForHost: string;
  schemaRegistryUrl: string;
  shutdown: () => Promise<void>;
}

export async function startKafkaStack(): Promise<KafkaStack> {
  const network = await new Network().start();

  const kafka = await new GenericContainer('confluentinc/cp-kafka:7.6.1')
    .withNetwork(network)
    .withNetworkAliases('broker')
    .withExposedPorts(9092)
    .withEnvironment({
      KAFKA_NODE_ID: '1',
      KAFKA_PROCESS_ROLES: 'broker,controller',
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@broker:29093',
      KAFKA_LISTENERS: 'PLAINTEXT://broker:29092,CONTROLLER://broker:29093,PLAINTEXT_HOST://0.0.0.0:9092',
      KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://broker:29092,PLAINTEXT_HOST://localhost:9092',
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP:
        'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT',
      KAFKA_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT',
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER',
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: '1',
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: '0',
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: '1',
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: '1',
      KAFKA_LOG_DIRS: '/tmp/kraft-combined-logs',
      CLUSTER_ID: 'MkU3OEVBNTcwNTJENDM2Qk',
    })
    .withWaitStrategy(Wait.forLogMessage('Kafka Server started', 1))
    .withStartupTimeout(120_000)
    .start();

  const kafkaHostPort = kafka.getMappedPort(9092);
  const brokersForHost = `localhost:${kafkaHostPort}`;

  const schemaRegistry = await new GenericContainer('confluentinc/cp-schema-registry:7.6.1')
    .withNetwork(network)
    .withNetworkAliases('schema-registry')
    .withExposedPorts(8081)
    .withEnvironment({
      SCHEMA_REGISTRY_HOST_NAME: 'schema-registry',
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: 'PLAINTEXT://broker:29092',
      SCHEMA_REGISTRY_LISTENERS: 'http://0.0.0.0:8081',
    })
    .withWaitStrategy(Wait.forHttp('/subjects', 8081).forStatusCode(200))
    .withStartupTimeout(120_000)
    .start();

  const schemaRegistryUrl = `http://localhost:${schemaRegistry.getMappedPort(8081)}`;

  const shutdown = async () => {
    await Promise.allSettled([schemaRegistry.stop(), kafka.stop()]);
    await network.stop().catch(() => undefined);
  };

  return { network, kafka, schemaRegistry, brokersForHost, schemaRegistryUrl, shutdown };
}
