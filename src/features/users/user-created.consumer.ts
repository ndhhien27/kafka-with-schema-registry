import { Injectable, Logger } from '@nestjs/common';
import { KafkaSubscribe } from '../../kafka/decorators/kafka-subscribe.decorator';
import type { DecodedKafkaMessage } from '../../kafka/interfaces/kafka.interfaces';
import { USER_CREATED_TOPIC, UserCreatedEvent } from './user-events.types';

@Injectable()
export class UserCreatedConsumer {
  private readonly logger = new Logger(UserCreatedConsumer.name);

  @KafkaSubscribe({
    topic: USER_CREATED_TOPIC,
    groupId: 'users-consumer',
    fromBeginning: false,
  })
  async handle(message: DecodedKafkaMessage<UserCreatedEvent>): Promise<void> {
    const { value, partition, offset } = message;
    this.logger.log(
      `UserCreated received id=${value.eventId} user=${value.userId}` +
        ` email=${value.email} partition=${partition} offset=${offset}`,
    );
    // TODO: downstream side-effect (email, search index, etc.)
  }
}
