import { Body, Controller, HttpCode, Post } from '@nestjs/common';
import { UserCreatedProducer, CreateUserInput } from './user-created.producer';

@Controller('users')
export class UsersController {
  constructor(private readonly producer: UserCreatedProducer) {}

  @Post()
  @HttpCode(202)
  async create(@Body() body: CreateUserInput) {
    const event = await this.producer.emit(body);
    return { status: 'accepted', eventId: event.eventId };
  }
}
