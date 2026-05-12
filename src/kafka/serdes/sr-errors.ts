/**
 * The @confluentinc/schemaregistry package raises:
 *   - `SerializationError` for both client-misuse (bad magic byte, missing schema id)
 *     AND payload-shape violations from `avsc.isValid` ("Invalid message at <path>, …").
 *   - `RestError` for HTTP failures against the registry.
 *
 * Neither is re-exported from the package's public typings in a way that survives
 * deep imports cleanly, so we identify them by `name` to keep the dependency loose.
 */

export const SERIALIZATION_ERROR_NAME = 'SerializationError';
export const REST_ERROR_NAME = 'RestError';

export interface SchemaValidationContext extends Error {
  paths?: string[][];
}

export const isSchemaRegistryError = (err: unknown): err is Error =>
  err instanceof Error &&
  (err.name === SERIALIZATION_ERROR_NAME || err.name === REST_ERROR_NAME);

/**
 * Schema-shape violations are SerializationErrors whose message starts with
 * "Invalid message at <path>" — produced by avsc's `isValid` errorHook.
 */
export const isSchemaRegistryValidationError = (
  err: unknown,
): err is SchemaValidationContext =>
  err instanceof Error &&
  err.name === SERIALIZATION_ERROR_NAME &&
  /^Invalid message at /.test(err.message);

/**
 * Extract the dotted field path embedded in a SerializationError message.
 * Returns `[['email']]` for `Invalid message at email, expected "string", got 123`,
 * or `undefined` if the message doesn't match the avsc errorHook format.
 */
export const extractValidationPaths = (
  err: SchemaValidationContext,
): string[][] | undefined => {
  if (Array.isArray(err.paths)) return err.paths;
  const match = /^Invalid message at ([^,]+),/.exec(err.message);
  if (!match) return undefined;
  const segments = match[1].split('.').filter(Boolean);
  return segments.length > 0 ? [segments] : undefined;
};

/**
 * Thrown by AvroSerializer when client-side `Type.isValid` pre-encode
 * validation rejects a payload. Carries field paths so callers (HTTP
 * exception filter, DLQ filter) can map to a precise 4xx response.
 */
export class SchemaPayloadInvalidError extends Error {
  readonly name = 'SchemaPayloadInvalidError';
  constructor(
    readonly topic: string,
    readonly paths: string[][],
    message?: string,
  ) {
    super(message ?? defaultMessage(topic, paths));
  }
}

function defaultMessage(topic: string, paths: string[][]): string {
  const flat = paths.map((p) => p.join('.')).join(', ');
  return `Payload failed Type.isValid for topic=${topic} paths=[${flat}]`;
}
