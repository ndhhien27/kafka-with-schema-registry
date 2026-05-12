/**
 * Standalone script to pre-register schemas in Schema Registry.
 * Walks `schemas/` recursively, derives subject from filename via the BTH
 * convention (filename uses underscores, topic uses dashes), and applies the
 * `SCHEMA_COMPATIBILITY` policy after registration.
 *
 * Usage: pnpm register:schemas
 */
import 'reflect-metadata';
import { promises as fs } from 'fs';
import { join, relative } from 'path';
import {
  Compatibility,
  SchemaInfo,
  SchemaRegistryClient,
} from '@confluentinc/schemaregistry';

async function main(): Promise<void> {
  const srUrl = process.env.SCHEMA_REGISTRY_URL ?? 'http://localhost:8081';
  const compatibility = (process.env.SCHEMA_COMPATIBILITY ?? 'FULL') as Compatibility;
  const schemasDir = join(process.cwd(), 'schemas');
  const client = new SchemaRegistryClient({ baseURLs: [srUrl] });

  const files = await collectAvscRecursively(schemasDir);
  if (files.length === 0) {
    console.log(`No .avsc files found under ${schemasDir}`);
    return;
  }

  for (const path of files) {
    const rel = relative(schemasDir, path);
    const schema = await fs.readFile(path, 'utf8');
    const base = rel.replace(/^.*[\\/]/, '').replace(/\.avsc$/, '');
    const topic = base.replace(/_/g, '-');
    const subject = `${topic}-value`;

    const schemaInfo: SchemaInfo = { schemaType: 'AVRO', schema };
    const id = await client.register(subject, schemaInfo, false);
    console.log(`registered ${rel} -> subject=${subject} id=${id}`);

    try {
      const applied = await client.updateCompatibility(subject, compatibility);
      console.log(`  compatibility=${applied}`);
    } catch (err) {
      console.warn(
        `  WARN: failed to set compatibility=${compatibility} on subject=${subject}: ${
          (err as Error).message
        }`,
      );
    }
  }
}

async function collectAvscRecursively(dir: string): Promise<string[]> {
  const out: string[] = [];
  let entries: { name: string; isDirectory(): boolean; isFile(): boolean }[];
  try {
    entries = await fs.readdir(dir, { withFileTypes: true });
  } catch {
    return out;
  }
  for (const entry of entries) {
    const full = join(dir, entry.name);
    if (entry.isDirectory()) {
      const nested = await collectAvscRecursively(full);
      out.push(...nested);
    } else if (entry.isFile() && entry.name.endsWith('.avsc')) {
      out.push(full);
    }
  }
  return out.sort();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
