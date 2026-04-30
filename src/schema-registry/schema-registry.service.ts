import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { promises as fs } from 'fs';
import { join } from 'path';
import {
  AvroDeserializer as PkgAvroDeserializer,
  AvroSerializer as PkgAvroSerializer,
  Client as SchemaRegistryClient,
  SchemaInfo,
  SerdeType,
  SubjectNameStrategyType,
} from '@confluentinc/schemaregistry';
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
  private serializer!: PkgAvroSerializer;
  private deserializer!: PkgAvroDeserializer;

  constructor(
    @Inject(SCHEMA_REGISTRY_CLIENT) private readonly client: SchemaRegistryClient,
  ) {}

  async onModuleInit(): Promise<void> {
    this.serializer = new PkgAvroSerializer(this.client, SerdeType.VALUE, {
      useLatestVersion: true,
      autoRegisterSchemas: false,
      subjectNameStrategyType: SubjectNameStrategyType.TOPIC,
    });
    this.deserializer = new PkgAvroDeserializer(this.client, SerdeType.VALUE, {
      subjectNameStrategyType: SubjectNameStrategyType.TOPIC,
    });
    await this.registerAllFromDisk();
  }

  getClient(): SchemaRegistryClient {
    return this.client;
  }

  getIdForSubject(subject: string): number | undefined {
    return this.registered.get(subject);
  }

  /**
   * Encodes `value` for `topic` using the latest registered schema (TopicNameStrategy).
   * Returns the SR-framed Buffer (magic byte + schema id + Avro payload).
   */
  async encode(topic: string, value: unknown): Promise<Buffer> {
    return this.serializer.serialize(topic, value);
  }

  /**
   * Decodes an SR-framed Buffer to its original message. The schema id is read
   * from the framing; `topic` is used by the underlying subject-name strategy
   * for migration/reader-schema lookups.
   */
  async decode<T = unknown>(topic: string, buffer: Buffer): Promise<T> {
    return (await this.deserializer.deserialize(topic, buffer)) as T;
  }

  private async registerAllFromDisk(): Promise<void> {
    const dir = await this.resolveSchemasDir();
    let files: string[];
    try {
      files = await fs.readdir(dir);
    } catch {
      this.logger.warn(`Schemas directory not found at ${dir}; skipping auto-registration.`);
      return;
    }

    const avscFiles = files.filter((f) => f.endsWith('.avsc'));
    for (const file of avscFiles) {
      const path = join(dir, file);
      const subject = this.subjectForFile(file);
      try {
        const raw = await fs.readFile(path, 'utf8');
        const schemaInfo: SchemaInfo = {
          schemaType: 'AVRO',
          schema: raw,
        };
        const id = await this.client.register(subject, schemaInfo, false);
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
