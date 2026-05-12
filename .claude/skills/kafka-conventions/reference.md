# kafka-conventions — reference

Loaded on demand from `kafka-conventions/SKILL.md`. Holds the long tables and
reference material that don't belong in the skill body.

## Full Avro types reference

### Primitive types

| Avro | Wire shape | TypeScript |
|---|---|---|
| `"null"` | absent | `null` |
| `"boolean"` | 1 byte | `boolean` |
| `"int"` | zigzag varint, 32-bit signed | `number` |
| `"long"` | zigzag varint, 64-bit signed | `number` (use `bigint` if > 2^53) |
| `"float"` | 4 bytes IEEE 754 | `number` |
| `"double"` | 8 bytes IEEE 754 | `number` |
| `"bytes"` | length-prefixed binary | `Buffer` |
| `"string"` | length-prefixed UTF-8 | `string` |

### Logical types (annotated primitives)

| Logical type | Underlying | Use |
|---|---|---|
| `date` | `int` (days since epoch) | `YYYY-MM-DD` calendar date |
| `time-millis` | `int` (ms since midnight) | time-of-day, ms precision |
| `time-micros` | `long` (µs since midnight) | time-of-day, µs precision |
| `timestamp-millis` | `long` (ms since epoch UTC) | **standard for `occurredAt` in this repo** |
| `timestamp-micros` | `long` (µs since epoch UTC) | µs-precision events |
| `local-timestamp-millis` | `long` | naive timestamp, no zone |
| `decimal` | `bytes` or `fixed` | arbitrary precision; use for currency when `long` minor units won't fit |
| `uuid` | `string` | UUID literal |
| `duration` | `fixed(12)` | months/days/millis triplet (rare) |

### Complex types

#### Record

```json
{
  "type": "record",
  "name": "RecordName",
  "namespace": "chorus.dom.module",
  "doc": "What this record represents.",
  "fields": [
    { "name": "fieldA", "type": "string" }
  ]
}
```

#### Enum

```json
{
  "name": "status",
  "type": {
    "type": "enum",
    "name": "Status",
    "symbols": ["NEW", "PROCESSING", "DONE"]
  }
}
```

Avro enums are **closed** — adding a symbol is forwards-compatible, but a
consumer that doesn't know the new symbol will fail to decode. For
forward-evolvable enums, model as `string` and validate at the application
level.

#### Array

```json
{
  "name": "items",
  "type": {
    "type": "array",
    "items": {
      "type": "record",
      "name": "OrderItem",
      "fields": [
        { "name": "sku",      "type": "string" },
        { "name": "quantity", "type": "int" }
      ]
    }
  }
}
```

Array items can be primitives, records, or unions.

#### Map

```json
{ "name": "tags", "type": { "type": "map", "values": "string" } }
```

Keys are always strings.

#### Union

```json
// Optional field — most common union
{ "name": "memo", "type": ["null", "string"], "default": null }

// Multi-type union (e.g. paymentMethod can be CreditCard or BankTransfer)
{
  "name": "method",
  "type": [
    {
      "type": "record",
      "name": "CreditCard",
      "fields": [
        { "name": "last4",   "type": "string" },
        { "name": "expires", "type": "string" }
      ]
    },
    {
      "type": "record",
      "name": "BankTransfer",
      "fields": [
        { "name": "iban", "type": "string" }
      ]
    }
  ]
}
```

For optional unions, **`null` must come first** (`["null", T]` with
`"default": null`). For multi-type unions, the default (if any) must match
the **first** branch's type — usually means defaults are awkward, so prefer
no default for multi-type unions.

#### Fixed

```json
{ "name": "checksum", "type": { "type": "fixed", "name": "Sha256", "size": 32 } }
```

Use for fixed-byte-length binary data (hashes, fingerprints, IBANs).

## Branch / cluster mapping (upstream BTH `om-schema-registry`)

| Branch | Subject prefix | Cluster (Confluent) |
|---|---|---|
| `develop` | `one-{app}-dev-...` | Develop |
| `test` | `one-{app}-test-...` | Develop |
| `stage` | `one-{app}-stage-...` | UAT |
| `production` | `one-{app}-prod-...` | Production |

PR target controls which cluster gets the schema. **Do not** merge to
`production` without rollout coordination.

## Subject naming strategies (for completeness)

This repo uses **TopicNameStrategy**. The Confluent subject-name strategies are:

| Strategy | Subject for value schema | Notes |
|---|---|---|
| `TopicNameStrategy` | `<topic>-value` | One schema per topic — what BTH uses |
| `RecordNameStrategy` | `<fully-qualified-record-name>` | Allows multiple event types per topic |
| `TopicRecordNameStrategy` | `<topic>-<fully-qualified-record-name>` | Hybrid — topic-scoped record namespacing |

Class names on the broker / Confluent side:
- `io.confluent.kafka.serializers.subject.TopicNameStrategy`
- `io.confluent.kafka.serializers.subject.RecordNameStrategy`
- `io.confluent.kafka.serializers.subject.TopicRecordNameStrategy`

Don't change the strategy without coordinating with every producer + consumer
on the cluster — it's a wire-format-affecting decision.

## Topic name slot reference (BTH spreadsheet)

The BTH naming spreadsheet (referenced in `docs/kafka.md`) maintains the
canonical list of `FEATURE` and `TYPE` values. A few representative slots:

| `APP` | Examples |
|---|---|
| `bth` | This repo's default |
| `om_com` | Common platform services |
| `om_pcs` | Port Community System |

| `TYPE` | Direction + visibility |
|---|---|
| `in-private` | Inbound to the owning team, private to ONE |
| `out-private` | Outbound from the owning team, private to ONE |
| `in-public` | Inbound from external systems |
| `out-public` | Outbound to external systems |

When in doubt, ask the owner of the BTH spreadsheet before claiming a new
`FEATURE` slot — collisions across teams are a recurring source of grief.

## Wire-format reference

The Schema-Registry frame for every Avro message:

| Bytes | Purpose | Read with |
|---|---|---|
| `0` | Magic byte `0x00` | `buf.readUInt8(0)` |
| `1`-`4` | Schema ID (big-endian uint32) | `buf.readUInt32BE(1)` |
| `5`+ | Avro-binary payload | Avro decoder using the resolved schema |

Anything else (no leading `0x00`, or fewer than 5 bytes) is a poison pill
when received by an Avro consumer.
