---
name: orkes-workflows
description: Authoring and lifecycle for Orkes Conductor workflow definitions in this repo — JSON shape (name, version, schemaVersion, ownerEmail, timeoutSeconds, timeoutPolicy, inputParameters, outputParameters, failureWorkflow, variables), version-bumping rules, registration via OrkesBootstrapService, parameters vs variables, sub-workflow composition, and the auto-register file layout under orkes/workflows/. Use when creating, modifying, versioning, or debugging a workflow definition.
---

# Orkes workflow definitions

How to author and evolve a workflow JSON. For task internals (each task
type's config), use `orkes-tasks`. For the Event Handler that triggers a
workflow, use `orkes-event-handlers`. For high-level patterns, use
`orkes-patterns`.

## When to use

User is:
- Creating a new workflow definition (JSON file under `orkes/workflows/`).
- Bumping a workflow version or rolling back.
- Adding `inputParameters` / `outputParameters` / `variables` /
  `failureWorkflow`.
- Reviewing a workflow PR for shape, naming, lifecycle.
- Debugging a "workflow registration failed" or "wrong version pinned" issue.

## Workflow JSON anatomy

```json
{
  "name":           "exp_{team}_vendor_onboarding",
  "version":        1,
  "schemaVersion":  2,
  "ownerEmail":     "team-lead@example.com",
  "description":    "What this workflow does and why.",
  "timeoutSeconds": 3600,
  "timeoutPolicy":  "ALERT_ONLY",
  "failureWorkflow": "exp_{team}_vendor_onboarding_compensation",
  "inputParameters":  ["registration", "correlationId", "submittedAt"],
  "outputParameters": {
    "vendorId":  "${normalize.output.result.vendorId}",
    "decision":  "${route_by_risk.output.result}"
  },
  "variables": {
    "vesselSlotReservationId": null
  },
  "tasks": [
    /* ... see orkes-tasks ... */
  ]
}
```

### Required top-level fields

| Field | Purpose |
|---|---|
| `name` | Unique identifier in the environment. Lower_snake_case, team-prefixed. |
| `version` | Integer. Bumped on any task-graph or I/O change. |
| `schemaVersion` | Always `2` for new workflows. |
| `tasks` | Ordered list of task definitions. At least one. |

### Recommended top-level fields

| Field | Purpose |
|---|---|
| `description` | One-line human summary; appears in the UI. |
| `ownerEmail` | Who to ping on failures. Required by some Orkes deployments. |
| `timeoutSeconds` | Hard backstop. Set to a multiple of the SLA you actually expect. |
| `timeoutPolicy` | `ALERT_ONLY` (don't terminate) or `TIME_OUT_WF` (terminate). |
| `inputParameters` | List of names declaring the workflow's input contract. |
| `outputParameters` | Map of names → expressions; sets the workflow's output. |
| `failureWorkflow` | Triggers on uncaught failure — saga compensation entry point. |
| `variables` | Mutable state container; initial values. |

## Naming conventions

Per [docs/codelab.md](docs/codelab.md):

| Resource | Pattern | Example |
|---|---|---|
| Workflow | `exp_{team}_{purpose}` | `exp_spm_vendor_onboarding` |
| Sub-workflow | same pattern | `exp_spm_compliance_screening` |
| Compensation workflow | `{name}_compensation` | `exp_spm_vendor_onboarding_compensation` |
| Task `taskReferenceName` | lower_snake_case verb-noun | `normalize_registration`, `publish_workflow_started` |
| HUMAN task `taskReferenceName` | Pascal Case verb-noun (per training) | `ApproveRegistration`, `ReviewDocuments` |

> **Why Pascal Case for HUMAN tasks?** They render in the "My Task" UI and
> show the task name to operators. Pascal Case scans as a button label;
> snake_case looks like a system identifier.

## State management — Inputs vs Variables

| Aspect | Inputs (immutable) | Variables (mutable) |
|---|---|---|
| Defined | At workflow start (or in `inputParameters`) | Declared in `variables`, modified by `SET_VARIABLE` |
| Mutable | No | Yes |
| Access | `${workflow.input.X}` | `${workflow.variables.X}` |
| Use for | Caller-supplied data, stable across the run | Saga rollback context, accumulators, runtime decisions |

**Saga pattern usage**: as the workflow progresses, `SET_VARIABLE` records
rollback context (e.g. `vesselSlotReservationId`, `paymentId`). The
`failureWorkflow` reads `${workflow.variables.X}` to compensate. See
`orkes-patterns` (Saga / Compensation).

## Lifecycle

### Registration

`OrkesBootstrapService` in
[src/orkes/orkes-bootstrap.service.ts](src/orkes/orkes-bootstrap.service.ts)
walks `orkes/workflows/*.json` at boot when `ORKES_AUTO_REGISTER=true` and
calls `metadataClient.registerWorkflowDef(def, true)` (`overwrite=true`).

This means **pushing an updated `kafka_demo_workflow.json` redeploys in
place, replacing the latest version**. To preserve history, **bump the
`version` field in the JSON** before merging.

### Versioning rules

| Change | Version bump? |
|---|---|
| Edit `description` only | No (cosmetic) |
| Add a new optional field to `outputParameters` | Yes |
| Change a task type, add/remove a task, change `taskReferenceName` | Yes |
| Change `inputParameters` (rename, add required, remove) | Yes |
| Adjust `timeoutSeconds` / `timeoutPolicy` | Yes (operational behavior change) |
| Change a HUMAN task's `slaMinutes` | Yes |

> **Running workflows are pinned to the version they started under.** A
> running v1 keeps executing v1's task graph even after v2 is registered.
> This means rolling out a new version doesn't break in-flight runs — but
> it also means you must keep historical task workers alive until those
> runs drain.

### Triggering a workflow by version

```bash
# From the app
curl -X POST http://localhost:3000/orkes/test-workflow ...   # uses DEMO_WORKFLOW_VERSION constant in OrkesController

# Via Orkes API
curl -X POST "$ORKES_SERVER_URL/workflow/exp_spm_vendor_onboarding?version=2" \
  -H "Content-Type: application/json" \
  -H "x-authorization: <TOKEN>" \
  -d '{ "registration": {...}, "correlationId": "abc-123" }'
```

Omit `?version=` to use the **latest** version. Pin explicitly when behavior
must be deterministic (e.g. correlated runs that must see the same task
graph).

## Sub-workflows for reuse

Per [docs/orkes_training.md](docs/orkes_training.md): extract repeating
series of tasks into a **shareable sub-workflow** to improve readability and
promote modular reuse across CHORUS.

A sub-workflow is just another workflow definition — register it the same
way under `orkes/workflows/`. Invoke it from a parent via the `SUB_WORKFLOW`
task type (see `orkes-tasks`).

## `failureWorkflow` and compensation

Per training:

> "Failure Workflows trigger automatically when an execution fails. Mirrors
> the compensation transaction aspect of a Saga pattern in microservice
> architectures. Note: Must be designed and created alongside the primary
> workflow."

```json
{
  "name": "exp_{team}_vendor_onboarding",
  "failureWorkflow": "exp_{team}_vendor_onboarding_compensation",
  "variables": {
    "vesselSlotReservationId": null,
    "surchargeTransactionId":  null
  },
  "tasks": [ /* ... */ ]
}
```

The compensation workflow receives the failed run's input + variables and
runs the inverse operations in reverse order. See `orkes-patterns` (Saga).

## Auto-register file layout

```
orkes/
  workflows/
    exp_spm_vendor_onboarding.json
    exp_spm_vendor_onboarding_compensation.json
    exp_spm_compliance_screening.json     # sub-workflow
    kafka_demo_workflow.json              # demo
    kafka_demo_consumer_workflow.json     # demo
  event_handlers/
    exp_spm_vendor_onboarding_trigger.json
    exp_spm_vendor_doc_verified_handler.json
    order_placed_handler.json             # demo
```

`OrkesBootstrapService.listJson` sorts files alphabetically before
registering — design names so that dependencies (sub-workflows) register
before their parents if you depend on registration order.

## Common errors and fixes

| Symptom | Cause | Fix |
|---|---|---|
| `Workflow definition failed validation: schemaVersion required` | Missing `schemaVersion: 2` | Add it |
| Workflow registers but never runs | Forgot to bump `version` and an Event Handler still pins v1 | Update handler's `start_workflow.version`, or omit version to use latest |
| `Cannot resolve ${workflow.input.X}` | Task references an input not declared in `inputParameters` | Add to `inputParameters` array |
| Workflow times out at 60s | Default `timeoutSeconds`; long human flow needs more | Set `timeoutSeconds` explicitly; consider `timeoutPolicy: ALERT_ONLY` |
| `failureWorkflow` never fires | Task error is being caught (e.g. `optional: true`) | Failure workflow only triggers on **uncaught** workflow-level failure |

## Pre-merge checklist

- [ ] `name` follows team-prefixed snake_case
- [ ] `version` bumped if task graph or I/O changed
- [ ] `schemaVersion: 2`
- [ ] `ownerEmail` set
- [ ] `timeoutSeconds` chosen for the actual SLA, not Orkes default
- [ ] `inputParameters` lists every `${workflow.input.X}` referenced
- [ ] `outputParameters` references real task outputs (not typos)
- [ ] If saga: `failureWorkflow` set + matching compensation workflow exists
- [ ] If runs are long: `correlationId` propagated through every task
- [ ] No PII in `inputParameters` or `outputParameters`
- [ ] File lives under `orkes/workflows/` so `OrkesBootstrapService` picks it up

## Cross-refs

- For task internals (each task type): use `orkes-tasks`.
- For Event Handlers that start the workflow: use `orkes-event-handlers`.
- For pattern selection (saga, fork-join, etc.): use `orkes-patterns`.
- For Kafka-specific EVENT tasks (`_schema` etc.): use `kafka-orkes`.
- For Orkes module wiring + `OrkesBootstrapService`: use `orkes` (master).
- Project hard rules: [.claude/rules/orkes-standards.md](.claude/rules/orkes-standards.md).
- Authoritative material: [docs/orkes_training.md](docs/orkes_training.md),
  [docs/codelab.md](docs/codelab.md).
