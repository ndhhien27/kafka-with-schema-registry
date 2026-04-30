import { Module } from '@nestjs/common';
import { UserCreatedProducer } from './user-created.producer';
import { UserCreatedConsumer } from './user-created.consumer';
import { UsersController } from './users.controller';

@Module({
  controllers: [UsersController, UserCreatedConsumer],
  providers: [UserCreatedProducer],
  exports: [UserCreatedProducer],
})
export class UsersModule {}
