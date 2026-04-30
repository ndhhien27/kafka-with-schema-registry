import { Controller, Logger } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { USER_CREATED_TOPIC, UserCreatedEvent } from './user-events.types';

@Controller()
export class UserCreatedConsumer {
  private readonly logger = new Logger(UserCreatedConsumer.name);

  @EventPattern(USER_CREATED_TOPIC)
  async handle(
    @Payload() value: UserCreatedEvent,
    @Ctx() ctx: KafkaContext,
  ): Promise<void> {
    const partition = ctx.getPartition();
    const offset = ctx.getMessage().offset;
    this.logger.log(
      `UserCreated received id=${value.eventId} user=${value.userId}` +
        ` email=${value.email} partition=${partition} offset=${offset}`,
    );
  }
}
