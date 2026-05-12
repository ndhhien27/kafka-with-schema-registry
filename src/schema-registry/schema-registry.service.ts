import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { promises as fs } from 'fs';
import { join, relative } from 'path';
import {
  AvroDeserializer as PkgAvroDeserializer,
  AvroSerializer as PkgAvroSerializer,
  Client as SchemaRegistryClient,
  Compatibility,
  SchemaInfo,
  SerdeType,
  SubjectNameStrategyType,
} from '@confluentinc/schemaregistry';
import { AppConfigService } from '../config/app-config.service';
export const SCHEMA_REGISTRY_CLIENT = Symbol('SCHEMA_REGISTRY_CLIENT');

export interface RegisteredSchema {
  subject: string;
  id: number;
  filename: string;
  schemaText: string;
}

@Injectable()
export class SchemaRegistryService implements OnModuleInit {
  private readonly logger = new Logger(SchemaRegistryService.name);
  private readonly registered = new Map<string, RegisteredSchema>();
  private serializer!: PkgAvroSerializer;
  private deserializer!: PkgAvroDeserializer;

  constructor(
    @Inject(SCHEMA_REGISTRY_CLIENT) private readonly client: SchemaRegistryClient,
    private readonly cfg: AppConfigService,
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
    return this.registered.get(subject)?.id;
  }

  /**
   * Returns the raw `.avsc` text for a given topic if it was loaded at boot.
   * Used by AvroSerializer for `Type.isValid` pre-encode validation.
   */
  getSchemaTextForTopic(topic: string): string | undefined {
    return this.registered.get(`${topic}-value`)?.schemaText;
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
      files = await this.collectAvscRecursively(dir);
    } catch {
      this.logger.warn(`Schemas directory not found at ${dir}; skipping auto-registration.`);
      return;
    }

    if (files.length === 0) {
      this.logger.warn(`No .avsc files found under ${dir}; skipping auto-registration.`);
      return;
    }

    const compatibility = this.cfg.schemaRegistry.compatibility as Compatibility;

    for (const path of files) {
      const filename = relative(dir, path);
      const subject = this.subjectForFile(filename);
      try {
        const raw = await fs.readFile(path, 'utf8');
        const schemaInfo: SchemaInfo = {
          schemaType: 'AVRO',
          schema: raw,
        };
        const id = await this.client.register(subject, schemaInfo, false);
        this.registered.set(subject, { subject, id, filename, schemaText: raw });
        this.logger.log(`Registered ${filename} -> subject=${subject} id=${id}`);

        try {
          await this.client.updateCompatibility(subject, compatibility);
          this.logger.log(`Set compatibility ${compatibility} on subject=${subject}`);
        } catch (compatErr) {
          this.logger.warn(
            `Failed to set compatibility=${compatibility} on subject=${subject}: ${
              (compatErr as Error).message
            }`,
          );
        }
      } catch (err) {
        this.logger.error(
          `Failed to register schema ${filename}: ${(err as Error).message}`,
        );
        throw err;
      }
    }
  }

  private async collectAvscRecursively(dir: string): Promise<string[]> {
    const out: string[] = [];
    const entries = await fs.readdir(dir, { withFileTypes: true });
    for (const entry of entries) {
      const full = join(dir, entry.name);
      if (entry.isDirectory()) {
        const nested = await this.collectAvscRecursively(full);
        out.push(...nested);
      } else if (entry.isFile() && entry.name.endsWith('.avsc')) {
        out.push(full);
      }
    }
    return out.sort();
  }

  /**
   * Maps a relative `.avsc` path to a subject under TopicNameStrategy.
   *
   * BTH naming convention: filename uses underscores, topic uses dashes.
   *   `chorus/users/profile/one_bth_dev_user_created_in_private.avsc`
   *     → topic `one-bth-dev-user-created-in-private`
   *     → subject `one-bth-dev-user-created-in-private-value`
   */
  private subjectForFile(relPath: string): string {
    const base = relPath
      .replace(/^.*[\\/]/, '') // strip leading dirs
      .replace(/\.avsc$/, '');
    const topic = base.replace(/_/g, '-');
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
