import {
  Inject,
  Injectable,
  OnApplicationShutdown,
  OnModuleInit,
} from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { firstValueFrom } from 'rxjs';
import { randomUUID } from 'crypto';
import { KAFKA_CLIENT_PRODUCER } from '../../kafka/kafka.tokens';
import { USER_CREATED_TOPIC, UserCreatedEvent } from './user-events.types';

export interface CreateUserInput {
  userId: string;
  email: string;
  displayName?: string | null;
}

@Injectable()
export class UserCreatedProducer implements OnModuleInit, OnApplicationShutdown {
  constructor(
    @Inject(KAFKA_CLIENT_PRODUCER) private readonly client: ClientKafka,
  ) {}

  async onModuleInit(): Promise<void> {
    await this.client.connect();
  }

  async onApplicationShutdown(): Promise<void> {
    await this.client.close();
  }

  async emit(input: CreateUserInput): Promise<UserCreatedEvent> {
    const event: UserCreatedEvent = {
      eventId: randomUUID(),
      occurredAt: Date.now(),
      userId: input.userId,
      email: input.email,
      displayName: input.displayName ?? null,
    };

    await firstValueFrom(
      this.client.emit(USER_CREATED_TOPIC, {
        key: input.userId,
        value: event,
      }),
    );

    return event;
  }
}
