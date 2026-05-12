# kafka-conventions — examples

Worked `.avsc` examples loaded on demand from SKILL.md. Each example shows
both the BTH-spec snake-case form and notes on common variants.

## Example 1 — Minimal event (UserCreated)

```json
{
  "type": "record",
  "name": "one_bth_user_created_in_private",
  "namespace": "chorus.users.profile",
  "doc": "Emitted when a new user account is created.",
  "fields": [
    { "name": "eventId",     "type": "string",
      "doc": "UUID of this event — required, never null." },
    { "name": "occurredAt",  "type": { "type": "long", "logicalType": "timestamp-millis" },
      "doc": "Producer-side timestamp, epoch milliseconds UTC." },
    { "name": "userId",      "type": "string",
      "doc": "Stable user identifier." },
    { "name": "email",       "type": "string",
      "doc": "Lowercased email." },
    { "name": "displayName", "type": ["null", "string"], "default": null,
      "doc": "Optional human-readable name." }
  ]
}
```

Filename: `one_bth_user_created_in_private.avsc` (env-stripped per BTH
upstream convention) or `one_bth_dev_user_created_in_private.avsc` (this
repo's env-embedded variant).

## Example 2 — Event with array of nested records (OrderPlaced)

```json
{
  "type": "record",
  "name": "one_bth_order_placed_in_private",
  "namespace": "chorus.orders.checkout",
  "doc": "Emitted when an order is committed at checkout.",
  "fields": [
    { "name": "eventId",     "type": "string" },
    { "name": "occurredAt",  "type": { "type": "long", "logicalType": "timestamp-millis" } },
    { "name": "orderId",     "type": "string" },
    { "name": "userId",      "type": "string" },
    { "name": "amountCents", "type": "long",
      "doc": "Minor units (cents). NEVER use double for money." },
    { "name": "currency",    "type": "string", "default": "USD" },
    {
      "name": "items",
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "OrderItem",
          "fields": [
            { "name": "sku",      "type": "string" },
            { "name": "quantity", "type": "int" },
            { "name": "unitPriceCents", "type": "long" }
          ]
        }
      }
    },
    {
      "name": "shippingAddress",
      "type": ["null", {
        "type": "record",
        "name": "ShippingAddress",
        "fields": [
          { "name": "line1",      "type": "string" },
          { "name": "line2",      "type": ["null", "string"], "default": null },
          { "name": "city",       "type": "string" },
          { "name": "postalCode", "type": "string" },
          { "name": "country",    "type": "string", "doc": "ISO 3166-1 alpha-2." }
        ]
      }],
      "default": null
    }
  ]
}
```

Notes:
- `items` is required (no default). An empty array is allowed; missing field isn't.
- `shippingAddress` is optional (`["null", record]` + `default: null`).

## Example 3 — Enum + map (NotificationDispatched)

```json
{
  "type": "record",
  "name": "one_bth_notification_dispatched_in_private",
  "namespace": "chorus.notifications.dispatch",
  "doc": "Emitted when a notification is sent through any channel.",
  "fields": [
    { "name": "eventId",    "type": "string" },
    { "name": "occurredAt", "type": { "type": "long", "logicalType": "timestamp-millis" } },
    { "name": "userId",     "type": "string" },
    {
      "name": "channel",
      "type": {
        "type": "enum",
        "name": "NotificationChannel",
        "symbols": ["EMAIL", "SMS", "PUSH", "IN_APP"]
      }
    },
    { "name": "templateId", "type": "string" },
    {
      "name": "metadata",
      "type": { "type": "map", "values": "string" },
      "default": {},
      "doc": "Free-form key/value tags (e.g. campaignId, locale)."
    }
  ]
}
```

Notes:
- Enums are **closed**. Adding a new symbol later (`WHATSAPP`) is forwards-
  compatible only if **every consumer is upgraded first**. Use `string` if
  you need open evolution.
- Maps default to `{}` — empty object, not `null`.

## Example 4 — Renamed field with `aliases` (UserActivated)

When you rename a field on an existing schema, the new field must list the
old name in `aliases` so consumers using the old schema can still resolve it.

```json
{
  "type": "record",
  "name": "one_bth_user_activated_in_private",
  "namespace": "chorus.users.activation",
  "fields": [
    { "name": "eventId",    "type": "string" },
    { "name": "occurredAt", "type": { "type": "long", "logicalType": "timestamp-millis" } },
    { "name": "userId",     "type": "string" },
    {
      "name": "fullName",
      "type": ["null", "string"],
      "default": null,
      "aliases": ["displayName"],
      "doc": "Renamed from displayName in v2."
    }
  ]
}
```

See `kafka-evolution` for when this counts as compat-safe and when you still
need a coordinated rollout.

## Example 5 — Avoid these anti-patterns

```jsonc
// BAD — bare nullable type with no union
{ "name": "memo", "type": "string" }   // not actually optional

// BAD — union without default null
{ "name": "memo", "type": ["null", "string"] }   // breaks add-field on FULL

// BAD — required field with default
{ "name": "amount", "type": "long", "default": 0 }   // turns into "optional"

// BAD — money as double
{ "name": "amount", "type": "double" }   // float precision drift on aggregation
```

## Filename → topic → subject (cheat sheet)

| `.avsc` filename | Topic | Subject |
|---|---|---|
| `one_bth_user_created_in_private.avsc` | `one-bth-{env}-user-created-in-private` | `one-bth-{env}-user-created-in-private-value` |
| `one_bth_dev_user_created_in_private.avsc` | `one-bth-dev-user-created-in-private` | `one-bth-dev-user-created-in-private-value` |
| `one_om_com_ian_notification_in_private.avsc` | `one-om_com-{env}-ian-notification-in-private` | `one-om_com-{env}-ian-notification-in-private-value` |

Note `om_com` retains its underscore (it's a single APP slot value), while
everything else uses dashes when in topic form.
