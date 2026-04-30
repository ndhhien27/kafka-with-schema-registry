import { Module } from '@nestjs/common';
import { OrderPlacedConsumer } from './order-placed.consumer';

@Module({
  controllers: [OrderPlacedConsumer],
})
export class OrdersModule {}
