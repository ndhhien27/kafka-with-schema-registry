/**
 * Standalone script to pre-register schemas in Schema Registry (useful for CI).
 * Usage: pnpm register:schemas
 */
import 'reflect-metadata';
import { promises as fs } from 'fs';
import { join } from 'path';
import {
  SchemaRegistry,
  SchemaType,
  readAVSCAsync,
} from '@kafkajs/confluent-schema-registry';

async function main(): Promise<void> {
  const srUrl = process.env.SCHEMA_REGISTRY_URL ?? 'http://localhost:8081';
  const schemasDir = join(process.cwd(), 'schemas');
  const client = new SchemaRegistry({ host: srUrl });

  const files = (await fs.readdir(schemasDir)).filter((f) => f.endsWith('.avsc'));
  if (files.length === 0) {
    console.log(`No .avsc files in ${schemasDir}`);
    return;
  }

  for (const file of files) {
    const path = join(schemasDir, file);
    const schema = await readAVSCAsync(path);
    const topic = file.replace(/\.avsc$/, '').replace(/-/g, '.');
    const subject = `${topic}-value`;
    const { id } = await client.register(
      { type: SchemaType.AVRO, schema: JSON.stringify(schema) },
      { subject },
    );
    console.log(`✓ ${file} -> subject=${subject} id=${id}`);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
