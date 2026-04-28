import { Injectable } from '@nestjs/common';
import { randomUUID } from 'crypto';
import { ProducerService } from '../../kafka/producer.service';
import { USER_CREATED_TOPIC, UserCreatedEvent } from './user-events.types';

export interface CreateUserInput {
  userId: string;
  email: string;
  displayName?: string | null;
}

@Injectable()
export class UserCreatedProducer {
  constructor(private readonly producer: ProducerService) {}

  async emit(input: CreateUserInput): Promise<UserCreatedEvent> {
    console.log(JSON.stringify(input, null, 2));
    const event: UserCreatedEvent = {
      eventId: randomUUID(),
      occurredAt: Date.now(),
      userId: input.userId,
      email: input.email,
      displayName: input.displayName ?? null,
    };

    await this.producer.produce<UserCreatedEvent>({
      topic: USER_CREATED_TOPIC,
      key: input.userId,
      value: event,
    });

    return event;
  }
}
