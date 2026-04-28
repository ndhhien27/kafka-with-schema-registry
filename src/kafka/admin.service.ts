import {
  Inject,
  Injectable,
  Logger,
  OnApplicationShutdown,
  OnModuleInit,
} from '@nestjs/common';
import { Admin, Kafka } from 'kafkajs';
import { KAFKA_CLIENT } from './kafka.tokens';

@Injectable()
export class KafkaAdminService implements OnModuleInit, OnApplicationShutdown {
  private readonly logger = new Logger(KafkaAdminService.name);
  private readonly admin: Admin;
  private connected = false;

  constructor(@Inject(KAFKA_CLIENT) kafka: Kafka) {
    this.admin = kafka.admin();
  }

  async onModuleInit(): Promise<void> {
    await this.admin.connect();
    this.connected = true;
  }

  async onApplicationShutdown(): Promise<void> {
    if (!this.connected) return;
    try {
      await this.admin.disconnect();
    } catch (err) {
      this.logger.error(`Error disconnecting admin: ${(err as Error).message}`);
    }
  }

  getAdmin(): Admin {
    return this.admin;
  }
}
