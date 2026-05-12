---
name: kafka-evolution
description: Decision matrix and rollout sequencing for Avro schema changes per BTH guideline — Confluent compatibility modes (BACKWARD / FORWARD / FULL / NONE), producer-vs-consumer-first ordering, aliases for renames, broker enforcement matrix, rollback procedure. Use BEFORE any add/delete/rename of an Avro field, change of field type, or rollback of a published schema.
---

# Kafka schema evolution

This skill picks the **correct compatibility mode** and **rollout sequence**
for any change to an existing `.avsc`. Get this right and the cluster keeps
moving; get it wrong and you stall a partition or break downstream consumers.

## When to use

User wants to:
- Add, remove, or rename a field in an existing `.avsc`.
- Change a field's type or default.
- Roll back a recently published schema version.
- Decide whether the producer or consumer ships first.

If the schema is **brand new** (no prior version published), skip evolution
rules and use `kafka-conventions` + `kafka-producer`.

## Default policy in this repo

`SCHEMA_COMPATIBILITY=FULL` — applied per subject after every registration in
[src/schema-registry/schema-registry.service.ts:111](src/schema-registry/schema-registry.service.ts).
Keep it on `FULL` during normal operation; switching to looser modes is an
**explicit decision tied to a coordinated rollout**.

## Compatibility matrix (Confluent)

| Mode | Allowed changes | Compares against | Upgrade order |
|---|---|---|---|
| `BACKWARD` (Confluent default) | Delete fields; add **optional** fields | Last version | **Consumers first** |
| `BACKWARD_TRANSITIVE` | Same as BACKWARD | All versions | Consumers first |
| `FORWARD` | Add fields; delete **optional** fields | Last version | **Producers first** |
| `FORWARD_TRANSITIVE` | Same as FORWARD | All versions | Producers first |
| `FULL` (BTH default) | Add **optional** fields; delete **optional** fields | Last version | Any order |
| `FULL_TRANSITIVE` | Same as FULL | All versions | Any order |
| `NONE` | All changes accepted; no checking | (none) | Outage-style coordinated change |

**Mnemonic**: `BACKWARD` = new schema can read old data → consumers go first.
`FORWARD` = old schema can read new data → producers go first.

## Decision tree — pick the change shape

```
What is the change?
│
├── Add a NEW required field
│      └── Compatibility BREAK on FULL.
│           Switch subject to FORWARD (or NONE for coordinated outage).
│           Producers first, then consumers. Fields with no default break BACKWARD.
│
├── Add a NEW optional field (["null", T] with default null)
│      └── FULL is fine. No upgrade order constraint.
│           Recommended path: producer-first.
│
├── DELETE an optional field
│      └── FULL is fine. No upgrade order constraint.
│           Confirm consumer code no longer reads it.
│
├── DELETE a required field
│      └── BACKWARD only. Consumers first (must tolerate missing field), then producers.
│
├── RENAME a field
│      └── Use the `aliases` attribute. The new field must list the old name in `aliases`.
│           Treated as compat-safe under FULL when aliases are present and types match.
│
├── CHANGE a field type
│      └── NONE only. Treat as a breaking change with downtime.
│
├── Reorder fields
│      └── Avro is positional in JSON-schema text but NOT on the wire — order is irrelevant.
│           Skip; no schema action required.
│
└── ADD + DELETE in the same revision
       └── NONE only. Not gracefully transition-able.
            Rollout = coordinated outage (see Rollback flow below).
```

## App-side rollout flows (per BTH guideline §Schema Evolution)

### Type 1A — App↔App, deleting a field

1. Set subject compatibility = `BACKWARD` (Confluent UI or `curl -X PUT`).
2. Push schema v2 to the registry.
3. **Consumer first**: deploy code that ignores the soon-deleted field but
   still reads v1 wire format.
4. **Producer second**: deploy code that uses schema v2 (without the field).
5. After both sides are stable, restore `FULL`.

### Type 1B — App↔App, adding a (required) field

1. Set subject compatibility = `FORWARD`.
2. Push schema v2.
3. **Producer first**: deploy code that emits v2 (with new field). Consumer
   is still on v1 (silently drops the unknown field).
4. **Consumer second**: deploy code that reads the new field.
5. Restore `FULL`.

### Type 1C — Add + delete simultaneously

1. Set compatibility = `NONE`.
2. Coordinate downtime — pause producer + consumer.
3. Push schema v2.
4. Deploy producer + consumer in lockstep.
5. Restore `FULL` after smoke test.

### Type 2 — Orkes is producer or consumer

- Orkes only references the **latest** schema version (no version pinning).
- Effective compatibility = `NONE`. Uploading a schema takes effect
  **immediately** for Orkes flows.
- Implication: every schema change touched by an Orkes path must assume
  zero-downtime is impossible without an explicit coordination plan.
- See `kafka-orkes` for further constraints.

## Schema/dataset violation matrix (broker enforces this)

| Case | Side | Outcome |
|---|---|---|
| Producer omits a **required** field | Producer | `Invalid "string": undefined` — message rejected |
| Producer omits an **optional** field | Producer | Succeeds; field replaced with `default` |
| Producer sends an extra **required** field | Producer | Succeeds; extra field silently dropped |
| Producer sends an extra **optional** field | Producer | Succeeds; extra field silently dropped |
| Consumer encounters unknown field id | Consumer | Decode succeeds; field ignored (forward-compat) |
| Consumer encounters missing required field | Consumer | Decode fails; throws — DLQ via poison-pill path |

## Rollback procedure

A rollback is an **incident**, not an evolution. Co-revert app + schema simultaneously.

### App-side rollback (Type 1)

1. Producer reverts to the prior schema ID — next message on the wire pairs
   old ID with old payload shape.
2. Consumer can already deserialize old shape, so the loop self-heals.
3. Delete the bad schema version in the Confluent subject (UI / CLI).
4. Revert the `.avsc` in `om-schema-registry` (or this repo's `schemas/`).
5. Confirm Confluent state matches repo state.

### Orkes rollback (Type 2)

1. Delete the bad schema version in the Confluent subject — Orkes immediately
   picks up the latest remaining version.
2. Revert the `.avsc` in the registry repo.
3. Confirm Confluent state matches repo state.
4. No Orkes-side action required (it always pulls latest).

## Aliases for renames

```jsonc
// v1
{ "name": "displayName", "type": ["null", "string"], "default": null }

// v2 (renamed to fullName, keeping displayName as an alias)
{
  "name": "fullName",
  "type": ["null", "string"],
  "default": null,
  "aliases": ["displayName"]
}
```

Without `aliases`, the rename is treated as `delete displayName` + `add
fullName` — which on `FULL` only works if both sides are optional, and even
then needs a coordinated rollout.

## Versioning rule

Preserve enough past schema versions on the subject to allow **at least one
rollback step**. BTH practice: keep the last 5 versions on every subject.
Confluent's default retention keeps all versions; if you've configured
`compatibility.group` cleanup, double-check the retention setting before
relying on rollback.

## Manipulating compatibility from the CLI

```bash
# Read current compatibility for a subject
curl http://localhost:8081/config/one-bth-dev-user-created-in-private-value | jq

# Switch to FORWARD ahead of a producer-first rollout
curl -X PUT http://localhost:8081/config/one-bth-dev-user-created-in-private-value \
  -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  -d '{"compatibility":"FORWARD"}'

# Restore FULL after the rollout completes
curl -X PUT http://localhost:8081/config/one-bth-dev-user-created-in-private-value \
  -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  -d '{"compatibility":"FULL"}'

# Delete a bad version (rollback)
curl -X DELETE http://localhost:8081/subjects/one-bth-dev-user-created-in-private-value/versions/3
```

In production, gate these calls behind change-management — the registry is
governance-controlled.

## Pre-merge checklist (schema PR)

- [ ] Subject's current compatibility mode noted in PR description
- [ ] Decision tree row above documented in the PR (which row applies, why)
- [ ] If `BACKWARD` / `FORWARD` / `NONE`: rollout order called out in deploy ticket
- [ ] If rename: `aliases` field added on the new field
- [ ] No required field added without a `default`
- [ ] After rollout, switch subject back to `FULL`
- [ ] If schema is shared with an Orkes path: pause-and-coordinate plan in
      the PR (per `kafka-orkes` effective-NONE caveat)

## Cross-refs

- For the `aliases` field syntax + Avro authoring: use `kafka-conventions`.
- For the Orkes-side caveat (effective `NONE`): use `kafka-orkes`.
- For producer / consumer code-side error mapping during rollouts: use
  `kafka-producer` / `kafka-consumer`.
- Project hard rules: [.claude/rules/kafka-standards.md](.claude/rules/kafka-standards.md).
- Authoritative spec: [docs/kafka.md](docs/kafka.md) ("Schema Evolution and
  Maintenance Guidelines" section).
