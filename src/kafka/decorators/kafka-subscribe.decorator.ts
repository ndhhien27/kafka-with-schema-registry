import { SetMetadata } from '@nestjs/common';
import { KAFKA_SUBSCRIBE_METADATA } from '../kafka.tokens';
import type { KafkaSubscribeOptions } from '../interfaces/kafka.interfaces';

export const KafkaSubscribe = (options: KafkaSubscribeOptions) =>
  SetMetadata(KAFKA_SUBSCRIBE_METADATA, options);
