import { Controller, Logger } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { KafkaRetry } from '../../kafka/decorators/kafka-retry.decorator';
import { ORDER_PLACED_TOPIC, OrderPlacedEvent } from './order-events.types';

@Controller()
export class OrderPlacedConsumer {
  private readonly logger = new Logger(OrderPlacedConsumer.name);

  @EventPattern(ORDER_PLACED_TOPIC)
  @KafkaRetry({ maxAttempts: 3, backoffMs: [100, 300, 800] })
  async handle(
    @Payload() value: OrderPlacedEvent,
    @Ctx() _ctx: KafkaContext,
  ): Promise<void> {
    if (value.amountCents < 0) {
      throw new Error(
        `invalid amountCents=${value.amountCents} for orderId=${value.orderId}`,
      );
    }

    this.logger.log(
      `OrderPlaced received order=${value.orderId} user=${value.userId}` +
        ` amount=${value.amountCents} ${value.currency} items=${value.items.length}`,
    );
  }
}
