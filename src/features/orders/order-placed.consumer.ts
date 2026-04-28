import { Injectable, Logger } from '@nestjs/common';
import { KafkaSubscribe } from '../../kafka/decorators/kafka-subscribe.decorator';
import type { DecodedKafkaMessage } from '../../kafka/interfaces/kafka.interfaces';
import { ORDER_PLACED_TOPIC, OrderPlacedEvent } from './order-events.types';

@Injectable()
export class OrderPlacedConsumer {
  private readonly logger = new Logger(OrderPlacedConsumer.name);

  @KafkaSubscribe({
    topic: ORDER_PLACED_TOPIC,
    groupId: 'orders-consumer',
    fromBeginning: false,
    maxAttempts: 3,
    backoffMs: [100, 300, 800],
  })
  async handle(message: DecodedKafkaMessage<OrderPlacedEvent>): Promise<void> {
    const { value } = message;

    // Intentional DLQ demo path: orders with amountCents < 0 are treated as invalid.
    if (value.amountCents < 0) {
      throw new Error(`invalid amountCents=${value.amountCents} for orderId=${value.orderId}`);
    }

    this.logger.log(
      `OrderPlaced received order=${value.orderId} user=${value.userId}` +
        ` amount=${value.amountCents} ${value.currency} items=${value.items.length}`,
    );
  }
}
