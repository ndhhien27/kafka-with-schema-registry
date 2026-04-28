import { Module } from '@nestjs/common';
import { OrderPlacedConsumer } from './order-placed.consumer';

@Module({
  providers: [OrderPlacedConsumer],
})
export class OrdersModule {}
