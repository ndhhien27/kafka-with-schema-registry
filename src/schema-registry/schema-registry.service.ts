import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { promises as fs } from 'fs';
import { join } from 'path';
import {
  SchemaRegistry,
  SchemaType,
  readAVSCAsync,
} from '@kafkajs/confluent-schema-registry';
import { AppConfigService } from '../config/app-config.service';

export const SCHEMA_REGISTRY_CLIENT = Symbol('SCHEMA_REGISTRY_CLIENT');

export interface RegisteredSchema {
  subject: string;
  id: number;
  filename: string;
}

@Injectable()
export class SchemaRegistryService implements OnModuleInit {
  private readonly logger = new Logger(SchemaRegistryService.name);
  private readonly registered = new Map<string, number>();

  constructor(
    @Inject(SCHEMA_REGISTRY_CLIENT) private readonly client: SchemaRegistry,
    private readonly appConfig: AppConfigService,
  ) {}

  async onModuleInit(): Promise<void> {
    await this.registerAllFromDisk();
  }

  getClient(): SchemaRegistry {
    return this.client;
  }

  getIdForSubject(subject: string): number | undefined {
    return this.registered.get(subject);
  }

  async encode(subject: string, payload: unknown): Promise<Buffer> {
    const id = this.registered.get(subject);
    if (id === undefined) {
      const latest = await this.client.getLatestSchemaId(subject);
      this.registered.set(subject, latest);
      return this.client.encode(latest, payload);
    }
    return this.client.encode(id, payload);
  }

  async decode<T = unknown>(buffer: Buffer): Promise<T> {
    return this.client.decode(buffer) as Promise<T>;
  }

  private async registerAllFromDisk(): Promise<void> {
    const dir = await this.resolveSchemasDir();
    let files: string[];
    try {
      files = await fs.readdir(dir);
    } catch (err) {
      this.logger.warn(`Schemas directory not found at ${dir}; skipping auto-registration.`);
      return;
    }

    const avscFiles = files.filter((f) => f.endsWith('.avsc'));
    for (const file of avscFiles) {
      const path = join(dir, file);
      const subject = this.subjectForFile(file);
      try {
        const schema = await readAVSCAsync(path);
        const { id } = await this.client.register(
          { type: SchemaType.AVRO, schema: JSON.stringify(schema) },
          { subject },
        );
        this.registered.set(subject, id);
        this.logger.log(`Registered ${file} -> subject=${subject} id=${id}`);
      } catch (err) {
        this.logger.error(
          `Failed to register schema ${file}: ${(err as Error).message}`,
        );
        throw err;
      }
    }
  }

  private subjectForFile(file: string): string {
    // TopicNameStrategy: <topic>-value. We derive topic from filename (e.g. user-created.avsc -> user.created)
    const base = file.replace(/\.avsc$/, '');
    const topic = base.replace(/-/g, '.');
    return `${topic}-value`;
  }

  private async resolveSchemasDir(): Promise<string> {
    const candidates = [
      join(process.cwd(), 'schemas'),
      join(__dirname, '..', '..', 'schemas'),
      join(__dirname, '..', '..', '..', 'schemas'),
    ];
    for (const c of candidates) {
      try {
        await fs.access(c);
        return c;
      } catch {
        // try next
      }
    }
    return candidates[0];
  }
}
