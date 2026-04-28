import { Module } from '@nestjs/common';
import { UserCreatedProducer } from './user-created.producer';
import { UserCreatedConsumer } from './user-created.consumer';
import { UsersController } from './users.controller';

@Module({
  controllers: [UsersController],
  providers: [UserCreatedProducer, UserCreatedConsumer],
  exports: [UserCreatedProducer],
})
export class UsersModule {}
