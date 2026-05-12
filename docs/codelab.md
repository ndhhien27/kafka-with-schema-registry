☰

VO

Vendor Onboarding
Codelab

Codelab: Vendor
Onboarding with Orkes Conductor▶

Part 1: Introduction▶

* [Business Context](#h-2)
* [Architecture](#h-3)
* [Workflow Overview](#h-5)

Part 2: Conventions &
Prerequisites▶

* [Naming Convention](#h-9)
* [Kafka Message Types](#h-13)
* [Prerequisites](#h-17)

Part 3: Understanding Event
Handlers▶

* [What is an Event Handler?](#h-19)
* [Key Concepts](#h-20)

Part 4: Create the Trigger Event
Handler▶

* [Step 1: Navigate to Event Handler Definitions](#h-28)
* [Step 2: Create a New Event Handler](#h-29)
* [Step 3: Configure the Event Handler](#h-30)
* [Step 4: Add the Action — Start Workflow](#h-31)
* [Step 5: Save and Activate](#h-33)

Part 5: Create the
Workflow▶

* [Workflow Visual Overview](#h-36)
* [Step 1: Create the Workflow](#h-38)
* [Step 2: Task — normalize\_registration
  (JSON\_JQ\_TRANSFORM)](#h-39)
* [Step 3: Task — publish\_workflow\_started (EVENT)](#h-43)
* [Step 4: Task — verify\_documents\_loop (DO\_WHILE)](#h-47)
* [Step 5: Task — parallel\_compliance\_checks (FORK\_JOIN)](#h-55)
* [Step 6: Task — calculate\_risk\_score (INLINE)](#h-66)
* [Step 7: Task — route\_by\_risk (SWITCH)](#h-71)
* [Step 8: Task — publish\_final\_notification (EVENT)](#h-79)
* [Step 9: Task — complete\_workflow (TERMINATE)](#h-83)

Part 6: Deploy &
Test▶

* [Deploy via JSON](#h-87)
* [Verify All Event Handlers](#h-88)
* [Test End-to-End](#h-89)

Troubleshooting▶

Summary▶

# Codelab: Vendor Onboarding with Orkes Conductor

This codelab walks you through building a **container shipping vendor onboarding** workflow using
Orkes Conductor, Kafka, and a NestJS backend — from creating event handlers to deploying and testing the full
workflow.

**Table of Contents:**

* [Part 1: Introduction](#part-1-introduction)
* [Part 2: Conventions & Prerequisites](#part-2-conventions--prerequisites)
* [Part 3: Understanding Event Handlers](#part-3-understanding-event-handlers)
* [Part 4: Create the Trigger Event Handler](#part-4-create-the-trigger-event-handler)
* [Part 5: Create the Workflow](#part-5-create-the-workflow)
* [Part 6: Deploy & Test](#part-6-deploy--test)
* [Troubleshooting](#troubleshooting)

---

# Part 1: Introduction

## Business Context

Container shipping companies work with third-party vendors — trucking companies, customs brokers, freight
forwarders, terminal operators, and insurance providers. Before a vendor can participate in the supply chain, they
must be **onboarded**: credentials verified, compliance checked, and contracts approved.

Today this process is manual, fragmented, and slow (14-21 business days). By orchestrating vendor onboarding
through **Orkes Conductor**, we reduce onboarding to 2-4 days (LOW risk: same-day), automate
compliance checks, route by risk level, enforce SLAs, and maintain a full audit trail.

## Architecture

![Diagram](images/diagrams/diagram-0.png)

### Communication Pattern

All communication between Orkes Conductor and the NestJS backend is **asynchronous via Kafka**. No
direct REST calls, no polling.

![Diagram](images/diagrams/diagram-1.png)

## Workflow Overview

The vendor onboarding workflow processes a registration through 8 stages:

![Diagram](images/diagrams/diagram-2.png)

### Three Paths

| Path | Risk Score | What Happens |
| --- | --- | --- |
| **AUTO-APPROVE** | 0-30 (LOW) | All checks pass, vendor auto-approved. ~2 min |
| **COMPLIANCE REVIEW** | 31-60 (MEDIUM) | Compliance Officer reviews and approves/rejects. ~5 min |
| **MANAGER REVIEW** | 61-100 (HIGH) | Regional Manager reviews. SLA escalation if no action. ~5-8 min |

### Orkes Features Demonstrated

This scenario exercises **7 key Orkes Conductor features** in a single workflow:

| # | Feature | Task Type | What You'll Learn |
| --- | --- | --- | --- |
| 1 | **Event Handler** | — | Trigger workflows from Kafka messages |
| 2 | **jq Processing** | JSON\_JQ\_TRANSFORM | Transform and normalize data without code |
| 3 | **Loop/Iteration** | DO\_WHILE | Iterate over variable-length collections |
| 4 | **Parallel Execution** | FORK\_JOIN | Run multiple tasks simultaneously |
| 5 | **Inline JavaScript** | INLINE | Embed business logic directly in the workflow |
| 6 | **Conditional Routing** | SWITCH | Branch workflow based on computed values |
| 7 | **Human Task + SLA** | HUMAN | Pause for human approval with escalation |

---

# Part 2: Conventions & Prerequisites

## Naming Convention

Since multiple teams share the same Orkes environment **and the same Kafka topics**, two things must
be team-scoped:

### 1. Resource Names

All workflow and event handler names include your team name as a prefix:

```
exp_{team_name}_vendor_onboardingCopy
```

### 2. Message Types (Team-Prefixed)

All Kafka message types include the team name as a prefix. This ensures team isolation by design — event handler
conditions only need to match on `metadata.type`:

```
$.metadata.type == '{team_name}.vendor.registration.submitted'Copy
```

### Example (team: spm)

| Resource | Name / Condition |
| --- | --- |
| Workflow | `exp_spm_vendor_onboarding` |
| Event Handler (trigger) | `exp_spm_vendor_onboarding_trigger` |
| Trigger condition | `$.metadata.type == 'spm.vendor.registration.submitted'` |
| Event Handler (doc verified) | `exp_spm_vendor_doc_verified_handler` |
| Doc verified condition | `$.metadata.type == 'spm.vendor.document.verified'` |

> **Important:** Throughout this codelab, replace `{team_name}` with your actual team
> name (e.g., `spm`, `apm`, `csr`, `bnk`).

## Kafka Message Types

Message types include the **team name as a prefix** to ensure isolation across teams sharing the
same Kafka topics. For example, team `spm` uses `spm.vendor.registration.submitted`.

### OUT topic (NestJS -> Orkes)

| Message Type | Purpose |
| --- | --- |
| `{team_name}.vendor.registration.submitted` | Triggers the workflow |
| `{team_name}.vendor.document.verified` | Document verification result |
| `{team_name}.vendor.sanctions.screened` | Sanctions screening result |
| `{team_name}.vendor.insurance.validated` | Insurance validation result |
| `{team_name}.vendor.license.verified` | License verification result |
| `{team_name}.vendor.onboarding.notified` | Final notification acknowledgement |

### IN topic (Orkes -> NestJS)

| Message Type | Purpose |
| --- | --- |
| `{team_name}.vendor.workflow.started` | Notify backend of workflow start |
| `{team_name}.vendor.document.verify` | Request document verification |
| `{team_name}.vendor.sanctions.screen` | Request sanctions screening |
| `{team_name}.vendor.insurance.validate` | Request insurance validation |
| `{team_name}.vendor.license.verify` | Request license verification |
| `{team_name}.vendor.onboarding.notify` | Send final approval/rejection |

### Message Envelope

All messages follow a standard envelope format:

```
{
  "metadata": {
    "type": "{team_name}.vendor.registration.submitted",
    "team": "{team_name}",
    "correlationId": "abc-123",
    "timestamp": "2026-03-07T12:00:00Z"
  },
  "payload": {
    "vendorId": "VND-123",
    "companyName": "Acme Shipping",
    "..."
  }
}Copy
```

* `metadata.type` — identifies the message purpose **with team prefix** (used by Event
  Handler conditions for both routing and team isolation)
* `metadata.team` — team name for informational/logging purposes
* `metadata.correlationId` — links all messages in a single onboarding flow for end-to-end tracing
* `payload` — the actual business data

> **Why embed the team in the message type?** All teams share the same Kafka topics. By prefixing
> the message type with the team name (e.g., `spm.vendor.registration.submitted`), each team's event
> handlers naturally only match their own messages. This is simpler and more reliable than using a separate
> `metadata.team` filter in conditions.

## Prerequisites

Before starting, ensure you have:

1. **Orkes Conductor** access at `https://one-dev.orkesconductor.io/` (Google Login)
2. **Backend running** — `make dev` starts both NestJS (port 3000) and Next.js frontend
   (port 3001)
3. **Kafka integration** configured in Orkes (integration name: `one-om-dev-training`)

---

# Part 3: Understanding Event Handlers

## What is an Event Handler?

An Event Handler in Orkes Conductor listens for events on an external queue (Kafka, SQS, etc.) and triggers
actions — such as starting a workflow or completing a task — when a matching message arrives.

In our scenario, we use **6 event handlers**:

* **1 trigger handler** — listens for registration messages and starts the workflow
  (`start_workflow`)
* **5 completion handlers** — listen for backend responses and complete waiting tasks
  (`complete_task`)

## Key Concepts

### The Event Field

The event field tells Orkes which Kafka topic to listen on:

```
kafka:<integration_name>:<topic_name>Copy
```

* `kafka` — the queue type
* `one-om-dev-training` — the Kafka integration name configured in Orkes
* `one-om-dev-training-message-out-private` — the actual Kafka topic name

### Condition Expressions

The condition is a JavaScript expression evaluated against each incoming Kafka message. It uses `$` to
reference the root of the message:

```
$.metadata.type == '{team_name}.vendor.registration.submitted'Copy
```

Since the team name is embedded in the message type, there's no need for a separate `metadata.team`
filter. Team `spm`'s messages use `spm.vendor.registration.submitted`, while team
`apm`'s messages use `apm.vendor.registration.submitted` — they never collide.

### `${}` Expressions in Actions

Inside an event handler action, `${}` expressions reference fields from the incoming Kafka message.
The `$` root is the **entire message envelope**:

```
Kafka message (the $ root)
├── metadata
│   ├── type            → ${metadata.type}
│   ├── team            → ${metadata.team}
│   ├── correlationId   → ${metadata.correlationId}
│   └── timestamp       → ${metadata.timestamp}
└── payload
    ├── vendorId        → ${payload.vendorId}
    ├── companyName     → ${payload.companyName}
    └── ...             → ${payload.*}Copy
```

| Expression | Resolves To | Example Value |
| --- | --- | --- |
| `${payload}` | The entire `payload` object | `{ "vendorId": "VND-123", "companyName": "Acme Shipping", ... }` |
| `${metadata.correlationId}` | A single field from `metadata` | `"abc-123"` |
| `${payload.vendorId}` | A single field from `payload` | `"VND-123"` |

### Action Type 1: `start\_workflow`

Starts a new workflow instance when the condition matches. Used for the trigger handler.

```
{
  "action": "start_workflow",
  "start_workflow": {
    "name": "exp_{team_name}_vendor_onboarding",
    "version": 1,
    "correlationId": "${metadata.correlationId}",
    "input": {
      "registration": "${payload}",
      "correlationId": "${metadata.correlationId}",
      "submittedAt": "${metadata.timestamp}"
    }
  }
}Copy
```

| Field | Purpose |
| --- | --- |
| `name` | Which workflow definition to start |
| `version` | Which version of the workflow |
| `correlationId` | Links the workflow to the original request (for tracing) |
| `input` | Data passed to the workflow as `workflow.input.*` |

### Action Type 2: `complete\_task`

Completes a **waiting task** inside an already-running workflow. Used when a workflow task has
`asyncComplete: true` — the task publishes a request to Kafka and pauses, waiting for a response. The
event handler catches the response and signals the task to continue.

```
{
  "action": "complete_task",
  "complete_task": {
    "workflowId": "${payload.workflowId}",
    "taskRefName": "${payload.taskRefName}",
    "output": {
      "vendorId": "${payload.vendorId}",
      "status": "${payload.status}",
      "details": "${payload.details}"
    }
  }
}Copy
```

| Field | Purpose |
| --- | --- |
| `workflowId` | Identifies **which workflow instance** contains the waiting task |
| `taskRefName` | Identifies **which task** within that workflow to complete |
| `output` | Data passed to the task as its output (available to downstream tasks) |

### The asyncComplete Pattern

This is the most important pattern in our workflow. When a workflow task needs data back from the NestJS backend:

![Diagram](images/diagrams/diagram-3.png)

> **Key insight:** The `workflowId` and `taskRefName` are included in the
> **request message** by the workflow. The NestJS backend echoes them back in the **response
> message**. This is how the event handler knows exactly which workflow and which task to complete — even
> if multiple workflows are running simultaneously.

> **Rule of thumb:** Every EVENT task with `asyncComplete: true` requires a matching
> event handler. Without it, the task stays `IN_PROGRESS` forever and the workflow hangs.

---

# Part 4: Create the Trigger Event Handler

The trigger event handler listens for `{team_name}.vendor.registration.submitted` messages on Kafka
and starts the workflow.

![Diagram](images/diagrams/diagram-4.png)

## Step 1: Navigate to Event Handler Definitions

1. Log in to **Orkes Conductor** at `https://one-dev.orkesconductor.io/`
2. In the left sidebar, expand **Definitions**
3. Click **Event Handler**

You'll see the list of all event handlers in your environment.

![Event Handler Definitions list](images/02-event-handler-list.png)

## Step 2: Create a New Event Handler

Click the **"Define event handler"** button at the top right of the page.

![New Event Handler form](images/03-new-event-handler-form.png)

## Step 3: Configure the Event Handler

Fill in the following fields:

| Field | Value | Description |
| --- | --- | --- |
| **Name** | `exp_{team_name}_vendor_onboarding_trigger` | Unique identifier for this handler |
| **Event** | `kafka:one-om-dev-training:one-om-dev-training-message-out-private` | The Kafka topic to listen on |
| **Condition** | `$.metadata.type == '{team_name}.vendor.registration.submitted'` | Only trigger when team-prefixed message type matches |
| **Evaluator Type** | `javascript` | Use JavaScript for condition evaluation |

## Step 4: Add the Action — Start Workflow

Under the **Actions** section, click **Add Action** and select
**start\_workflow**.

Configure the action:

| Field | Value | Description |
| --- | --- | --- |
| **Workflow Name** | `exp_{team_name}_vendor_onboarding` | The workflow to start |
| **Version** | `1` | Workflow version |
| **Correlation ID** | `${metadata.correlationId}` | Links the workflow back to the original request |

### Input Mapping

The `input` section maps data from the Kafka message into the workflow's input parameters:

```
{
  "registration": "${payload}",
  "correlationId": "${metadata.correlationId}",
  "submittedAt": "${metadata.timestamp}"
}Copy
```

| Workflow Input Parameter | Source | What the Workflow Receives |
| --- | --- | --- |
| `registration` | `${payload}` | The full vendor registration object |
| `correlationId` | `${metadata.correlationId}` | The correlation ID string for tracing |
| `submittedAt` | `${metadata.timestamp}` | The timestamp of when the vendor submitted |

![Event Handler configuration](images/04-event-handler-config.png)

## Step 5: Save and Activate

1. Click **Save** to create the event handler
2. Ensure the handler shows as **Active** in the list (green status)
3. If it shows as paused, click the **Resume** button

### Complete JSON Reference

You can also create/update the event handler via the API:

```
{
  "name": "exp_{team_name}_vendor_onboarding_trigger",
  "event": "kafka:one-om-dev-training:one-om-dev-training-message-out-private",
  "condition": "$.metadata.type == '{team_name}.vendor.registration.submitted'",
  "actions": [
    {
      "action": "start_workflow",
      "start_workflow": {
        "name": "exp_{team_name}_vendor_onboarding",
        "version": 1,
        "correlationId": "${metadata.correlationId}",
        "input": {
          "registration": "${payload}",
          "correlationId": "${metadata.correlationId}",
          "submittedAt": "${metadata.timestamp}"
        }
      }
    }
  ],
  "active": true,
  "evaluatorType": "javascript"
}Copy
```

**API call** (using `curl`):

```
curl -X POST "https://one-dev.orkesconductor.io/api/event" \
  -H "Content-Type: application/json" \
  -H "x-authorization: <YOUR_TOKEN>" \
  -d @event-handler.jsonCopy
```

---

# Part 5: Create the Workflow

## Workflow Visual Overview

![Diagram](images/diagrams/diagram-5.png)

### Features Demonstrated

| # | Feature | Task Type | Purpose |
| --- | --- | --- | --- |
| 1 | **jq Processing** | JSON\_JQ\_TRANSFORM | Normalize raw registration payload |
| 2 | **Event Publishing** | EVENT | Notify backend of workflow start |
| 3 | **Loop/Iteration** | DO\_WHILE | Iterate over variable-length documents |
| 4 | **Parallel Execution** | FORK\_JOIN | Run 3 checks simultaneously |
| 5 | **Inline JavaScript** | INLINE | Calculate composite risk score |
| 6 | **Conditional Routing** | SWITCH | Route by risk level |
| 7 | **Human Task + SLA** | HUMAN | Manual approval with timeout |

## Step 1: Create the Workflow

1. Navigate to **Definitions > Workflow** in the left sidebar
2. Click **"Define workflow"**

![Workflow Definitions page](images/07-workflow-list.png)

3. Enter the workflow name: `exp_{team_name}_vendor_onboarding`
4. Set **Timeout** to `3600` seconds (1 hour)
5. Define input parameters: `registration`, `correlationId`, `submittedAt`

![Workflow list with exp_vendor_onboarding](images/11-new-workflow.png)

---

## Step 2: Task — normalize\_registration (JSON\_JQ\_TRANSFORM)

**Purpose:** Extract and reshape the raw registration payload into a clean, normalized structure
using jq.

**Why jq?** The incoming registration data may have nested structures, optional fields, or
inconsistent formatting. The jq transform ensures downstream tasks always receive a predictable schema.

### Configuration

| Field | Value |
| --- | --- |
| **Task Type** | `JSON_JQ_TRANSFORM` |
| **Task Reference Name** | `normalize_registration` |

### Input Parameters

```
{
  "rawRegistration": "${workflow.input.registration}",
  "queryExpression": "
    {
      vendorId:           .rawRegistration.vendorId,
      companyName:        .rawRegistration.companyName,
      country:            .rawRegistration.country,

      contactEmail:       .rawRegistration.contact.email,
      contactPhone:       .rawRegistration.contact.phone,

      documents: [
        .rawRegistration.documents[] | {
          type:       .type,
          number:     .number,
          issuedBy:   .issuedBy,
          expiryDate: .expiryDate
        }
      ],

      insurance: {
        provider:       .rawRegistration.insurance.provider,
        policyNumber:   .rawRegistration.insurance.policyNumber,
        coverageAmount: .rawRegistration.insurance.coverageAmount,
        expiryDate:     .rawRegistration.insurance.expiryDate
      },

      license: {
        type:         .rawRegistration.license.type,
        number:       .rawRegistration.license.number,
        jurisdiction: .rawRegistration.license.jurisdiction,
        expiryDate:   .rawRegistration.license.expiryDate
      },

      documentCount:      (.rawRegistration.documents | length),
      registrationNumber: .rawRegistration.registrationNumber
    }
  "
}Copy
```

### What the jq Expression Does

![Diagram](images/diagrams/diagram-6.png)

All subsequent tasks reference `${normalize_registration.output.result.*}` instead of raw input.

![normalize_registration task configuration](images/wf-01-normalize-registration.png)

---

## Step 3: Task — publish\_workflow\_started (EVENT)

**Purpose:** Notify the NestJS backend that the workflow has started, passing the
`workflowId` so the backend can link it to the vendor record in the database.

### Configuration

| Field | Value |
| --- | --- |
| **Task Type** | `EVENT` |
| **Task Reference Name** | `publish_workflow_started` |
| **Sink** | `kafka:one-om-dev-training:one-om-dev-training-message-in-private` |
| **Async Complete** | `false` (fire-and-forget) |

### Input Parameters

```
{
  "metadata": {
    "type": "{team_name}.vendor.workflow.started",
    "team": "{team_name}",
    "correlationId": "${workflow.input.correlationId}",
    "source": "orkes-conductor",
    "version": "1.0"
  },
  "payload": {
    "workflowId": "${workflow.workflowId}",
    "vendorId": "${normalize_registration.output.result.vendorId}",
    "companyName": "${normalize_registration.output.result.companyName}"
  }
}Copy
```

### asyncComplete: false vs true

* `asyncComplete: false` — The EVENT task publishes to Kafka and **immediately
  completes**. The workflow moves to the next task without waiting for a response. **No event
  handler needed.**
* `asyncComplete: true` — The EVENT task publishes to Kafka and **stays IN\_PROGRESS**,
  waiting for an external signal to complete it. **Requires an event handler.**

This task uses `false` because the workflow doesn't need any response — it's a one-way notification.

![publish_workflow_started task configuration](images/wf-02-publish-workflow-started.png)

---

## Step 4: Task — verify\_documents\_loop (DO\_WHILE)

**Purpose:** Iterate over the vendor's documents array and verify each one individually. The number
of documents varies per vendor.

### Configuration

| Field | Value |
| --- | --- |
| **Task Type** | `DO_WHILE` |
| **Task Reference Name** | `verify_documents_loop` |

### Input Parameters

```
{
  "documents": "${normalize_registration.output.result.documents}",
  "documentCount": "${normalize_registration.output.result.documentCount}"
}Copy
```

### Loop Condition

```
if ($.verify_documents_loop['iteration'] < $.compute_doc_task_ref.result.documentCount) true; else false;Copy
```

The loop runs while the current iteration index is less than the total document count.

### Loop Body — Two Sub-Tasks

#### Sub-Task A: compute\_doc\_task\_ref (INLINE)

Computes the current document to verify based on the loop iteration.

**Input Parameters:**

```
{
  "iteration": "${verify_documents_loop.output.iteration}",
  "documents": "${normalize_registration.output.result.documents}"
}Copy
```

**Expression (JavaScript):**

```
(function () {
  var idx = $.iteration - 1;
  return {
    taskRefName:   'publish_doc_verify__' + $.iteration,
    docIndex:      idx,
    document:      $.documents[idx],
    documentCount: $.documents.length
  };
})()Copy
```

#### Sub-Task B: publish\_doc\_verify (EVENT + asyncComplete)

Publishes a verification request to Kafka and **waits for the NestJS backend to respond**.

```
{
  "metadata": {
    "type": "{team_name}.vendor.document.verify",
    "team": "{team_name}",
    "correlationId": "${workflow.input.correlationId}"
  },
  "payload": {
    "workflowId": "${workflow.workflowId}",
    "taskRefName": "${compute_doc_task_ref.output.result.taskRefName}",
    "vendorId": "${normalize_registration.output.result.vendorId}",
    "document": "${compute_doc_task_ref.output.result.document}"
  }
}Copy
```

| Field | Value |
| --- | --- |
| **Sink** | `kafka:one-om-dev-training:one-om-dev-training-message-in-private` |
| **Async Complete** | `true` |

When `asyncComplete: true`, the task stays in `IN_PROGRESS` — the workflow pauses here.

![verify_documents_loop task configuration](images/wf-03-verify-documents-loop.png)

### Create Event Handler: Document Verification Response

Since `publish_doc_verify` uses `asyncComplete: true`, we need an event handler to complete
it. When the NestJS backend finishes verifying a document, it publishes a
`{team_name}.vendor.document.verified` message to the OUT topic. This event handler catches that
message and completes the waiting task.

Navigate to **Definitions > Event Handler** and create:

```
{
  "name": "exp_{team_name}_vendor_doc_verified_handler",
  "event": "kafka:one-om-dev-training:one-om-dev-training-message-out-private",
  "condition": "$.metadata.type == '{team_name}.vendor.document.verified'",
  "actions": [
    {
      "action": "complete_task",
      "complete_task": {
        "workflowId": "${payload.workflowId}",
        "taskRefName": "${payload.taskRefName}",
        "output": {
          "vendorId": "${payload.vendorId}",
          "documentType": "${payload.documentType}",
          "status": "${payload.status}",
          "details": "${payload.details}"
        }
      }
    }
  ],
  "active": true,
  "evaluatorType": "javascript"
}Copy
```

---

## Step 5: Task — parallel\_compliance\_checks (FORK\_JOIN)

**Purpose:** Run sanctions screening, insurance validation, and license verification
**simultaneously** to save time.

### Configuration

| Field | Value |
| --- | --- |
| **Task Type** | `FORK_JOIN` |
| **Task Reference Name** | `parallel_compliance_checks` |

### Fork Branches

The FORK\_JOIN creates 3 parallel branches, each containing an EVENT task with `asyncComplete: true`:

![Diagram](images/diagrams/diagram-7.png)

Each branch follows the same pattern as the document verify:

1. Publish a request to the Kafka IN topic
2. Wait for the NestJS backend to process and respond
3. An event handler completes the task with the response data

#### Branch 1: publish\_sanctions\_screen

| Field | Value |
| --- | --- |
| **Task Type** | `EVENT` |
| **Task Definition** | `publish_sanctions_screen` |
| **Task Reference Name** | `publish_sanctions_screen` |
| **Sink** | `kafka:one-om-dev-training:one-om-dev-training-message-in-private` |
| **Async Complete** | `true` |

**Input Parameters:**

```
{
  "metadata": {
    "type": "{team_name}.vendor.sanctions.screen",
    "team": "{team_name}",
    "correlationId": "${workflow.input.correlationId}"
  },
  "payload": {
    "workflowId": "${workflow.workflowId}",
    "taskRefName": "publish_sanctions_screen",
    "vendorId": "${normalize_registration.output.result.vendorId}",
    "companyName": "${normalize_registration.output.result.companyName}",
    "country": "${normalize_registration.output.result.country}"
  }
}Copy
```

#### Branch 2: publish\_insurance\_validate

| Field | Value |
| --- | --- |
| **Task Type** | `EVENT` |
| **Task Definition** | `publish_insurance_validate` |
| **Task Reference Name** | `publish_insurance_validate` |
| **Sink** | `kafka:one-om-dev-training:one-om-dev-training-message-in-private` |
| **Async Complete** | `true` |

**Input Parameters:**

```
{
  "metadata": {
    "type": "{team_name}.vendor.insurance.validate",
    "team": "{team_name}",
    "correlationId": "${workflow.input.correlationId}"
  },
  "payload": {
    "workflowId": "${workflow.workflowId}",
    "taskRefName": "publish_insurance_validate",
    "vendorId": "${normalize_registration.output.result.vendorId}",
    "insurance": "${normalize_registration.output.result.insurance}"
  }
}Copy
```

#### Branch 3: publish\_license\_verify

| Field | Value |
| --- | --- |
| **Task Type** | `EVENT` |
| **Task Definition** | `publish_license_verify` |
| **Task Reference Name** | `publish_license_verify` |
| **Sink** | `kafka:one-om-dev-training:one-om-dev-training-message-in-private` |
| **Async Complete** | `true` |

**Input Parameters:**

```
{
  "metadata": {
    "type": "{team_name}.vendor.license.verify",
    "team": "{team_name}",
    "correlationId": "${workflow.input.correlationId}"
  },
  "payload": {
    "workflowId": "${workflow.workflowId}",
    "taskRefName": "publish_license_verify",
    "vendorId": "${normalize_registration.output.result.vendorId}",
    "license": "${normalize_registration.output.result.license}"
  }
}Copy
```

### JOIN Task

After the FORK, add a JOIN task that waits for all 3 branches:

```
{
  "name": "join_compliance_checks",
  "taskReferenceName": "join_compliance_checks",
  "type": "JOIN",
  "joinOn": [
    "publish_sanctions_screen",
    "publish_insurance_validate",
    "publish_license_verify"
  ]
}Copy
```

![parallel_compliance_checks task configuration](images/wf-04-parallel-compliance-checks.png)

### Create Event Handlers: Compliance Responses

All 3 branches use `asyncComplete: true`, so each needs its own event handler to complete the waiting
task when the NestJS backend responds.

#### Event Handler: Sanctions Screened

Navigate to **Definitions > Event Handler** and create:

```
{
  "name": "exp_{team_name}_vendor_sanctions_screened_handler",
  "event": "kafka:one-om-dev-training:one-om-dev-training-message-out-private",
  "condition": "$.metadata.type == '{team_name}.vendor.sanctions.screened'",
  "actions": [
    {
      "action": "complete_task",
      "complete_task": {
        "workflowId": "${payload.workflowId}",
        "taskRefName": "${payload.taskRefName}",
        "output": {
          "vendorId": "${payload.vendorId}",
          "status": "${payload.status}",
          "details": "${payload.details}"
        }
      }
    }
  ],
  "active": true,
  "evaluatorType": "javascript"
}Copy
```

#### Event Handler: Insurance Validated

```
{
  "name": "exp_{team_name}_vendor_insurance_validated_handler",
  "event": "kafka:one-om-dev-training:one-om-dev-training-message-out-private",
  "condition": "$.metadata.type == '{team_name}.vendor.insurance.validated'",
  "actions": [
    {
      "action": "complete_task",
      "complete_task": {
        "workflowId": "${payload.workflowId}",
        "taskRefName": "${payload.taskRefName}",
        "output": {
          "vendorId": "${payload.vendorId}",
          "status": "${payload.status}",
          "details": "${payload.details}"
        }
      }
    }
  ],
  "active": true,
  "evaluatorType": "javascript"
}Copy
```

#### Event Handler: License Verified

```
{
  "name": "exp_{team_name}_vendor_license_verified_handler",
  "event": "kafka:one-om-dev-training:one-om-dev-training-message-out-private",
  "condition": "$.metadata.type == '{team_name}.vendor.license.verified'",
  "actions": [
    {
      "action": "complete_task",
      "complete_task": {
        "workflowId": "${payload.workflowId}",
        "taskRefName": "${payload.taskRefName}",
        "output": {
          "vendorId": "${payload.vendorId}",
          "status": "${payload.status}",
          "details": "${payload.details}"
        }
      }
    }
  ],
  "active": true,
  "evaluatorType": "javascript"
}Copy
```

> **Pattern:** Notice all `complete_task` event handlers share the same structure — they
> differ only in `name` and `condition`. The `workflowId` and
> `taskRefName` from the payload tell Orkes exactly which workflow and which task to complete.

---

## Step 6: Task — calculate\_risk\_score (INLINE)

**Purpose:** Compute a composite risk score (0-100) from all verification results using JavaScript.

### Configuration

| Field | Value |
| --- | --- |
| **Task Type** | `INLINE` |
| **Task Reference Name** | `calculate_risk_score` |
| **Evaluator Type** | `javascript` |

### Input Parameters

```
{
  "loopOutput": "${verify_documents_loop.output}",
  "documentCount": "${normalize_registration.output.result.documentCount}",
  "sanctionsResult": "${publish_sanctions_screen.output}",
  "insuranceResult": "${publish_insurance_validate.output}",
  "licenseResult": "${publish_license_verify.output}",
  "country": "${normalize_registration.output.result.country}"
}Copy
```

### JavaScript Expression

```
function calculateRisk() {
  var loopOutput = $.loopOutput || {};
  var docCount = $.documentCount || 0;
  var sanctions = $.sanctionsResult || {};
  var insurance = $.insuranceResult || {};
  var license = $.licenseResult || {};
  var country = $.country || '';

  // Document failures (25% weight)
  var docFailures = 0;
  for (var i = 1; i <= docCount; i++) {
    var iterData = loopOutput['' + i];
    if (iterData && iterData.publish_doc_verify) {
      if (iterData.publish_doc_verify.status !== 'VERIFIED') docFailures++;
    }
  }
  var docScore = Math.min((docFailures / Math.max(docCount, 1)) * 100, 100) * 0.25;

  // Sanctions (30% weight)
  var sanctionsScore = (sanctions.status === 'FLAGGED' ? 100 :
                        sanctions.status === 'POTENTIAL_MATCH' ? 60 : 0) * 0.30;

  // Country risk (15% weight)
  var highRiskCountries = ['IR','KP','SY','CU','VE','MM','RU','BY'];
  var countryScore = (highRiskCountries.indexOf(country) >= 0 ? 80 : 0) * 0.15;

  // Insurance (15% weight)
  var insuranceScore = (insurance.status === 'EXPIRED' ? 80 :
                        insurance.status === 'INSUFFICIENT' ? 50 : 0) * 0.15;

  // License (15% weight)
  var licenseScore = (license.status === 'EXPIRED' ? 80 :
                      license.status === 'INVALID_JURISDICTION' ? 60 :
                      license.status === 'INVALID_FORMAT' ? 40 : 0) * 0.15;

  var totalScore = Math.round(docScore + sanctionsScore + countryScore +
                              insuranceScore + licenseScore);

  var riskLevel = totalScore <= 30 ? 'LOW' :
                  totalScore <= 60 ? 'MEDIUM' : 'HIGH';

  return {
    score: totalScore,
    level: riskLevel,
    breakdown: {
      documents: Math.round(docScore),
      sanctions: Math.round(sanctionsScore),
      country: Math.round(countryScore),
      insurance: Math.round(insuranceScore),
      license: Math.round(licenseScore)
    }
  };
}
calculateRisk();Copy
```

### Output

```
{
  "result": {
    "score": 37,
    "level": "MEDIUM",
    "breakdown": {
      "documents": 8,
      "sanctions": 0,
      "country": 12,
      "insurance": 8,
      "license": 9
    }
  }
}Copy
```

![calculate_risk_score task configuration](images/wf-05-calculate-risk-score.png)

---

## Step 7: Task — route\_by\_risk (SWITCH)

**Purpose:** Route the workflow based on the calculated risk level. LOW risk auto-approves,
MEDIUM/HIGH risk requires human review.

### Configuration

| Field | Value |
| --- | --- |
| **Task Type** | `SWITCH` |
| **Task Reference Name** | `route_by_risk` |
| **Evaluator Type** | `value-param` |
| **Expression** | `riskLevel` |

### Input Parameters

```
{
  "riskLevel": "${calculate_risk_score.output.result.level}"
}Copy
```

![route_by_risk task configuration](images/wf-06-route-by-risk.png)

### Decision Cases

#### Case: LOW

No sub-tasks — the workflow proceeds directly to the final notification. The vendor is auto-approved.

#### Case: MEDIUM

Two sub-tasks:

1. **publish\_compliance\_review\_request** (EVENT) — Notifies the NestJS backend that a human review
   is needed. The backend creates a pending review task in its in-memory store.
2. **compliance\_review** (HUMAN) — A Human Task assigned to a `compliance_officer`. The
   workflow **pauses** here until a human approves or rejects the vendor.

```
{
  "name": "Review Vendor Compliance",
  "taskReferenceName": "compliance_review",
  "type": "HUMAN",
  "inputParameters": {
    "__humanTaskDefinition": {
      "assignmentCompletionStrategy": "LEAVE_OPEN",
      "assignments": [
        {
          "assignee": {
            "userType": "EXTERNAL_USER",
            "user": "compliance-officer"
          }
        }
      ],
      "displayName": "Review Vendor Compliance"
    },
    "vendorId": "${normalize_registration.output.result.vendorId}",
    "companyName": "${normalize_registration.output.result.companyName}",
    "riskScore": "${calculate_risk_score.output.result.score}",
    "riskLevel": "${calculate_risk_score.output.result.level}",
    "riskBreakdown": "${calculate_risk_score.output.result.breakdown}"
  },
  "timeoutSeconds": 600
}Copy
```

#### Case: HIGH

Same pattern but assigned to a `regional-manager` with a shorter timeout (360s).

### Key Concept: EXTERNAL\_USER

The `userType: "EXTERNAL_USER"` means the human task is managed **outside** Orkes — in
our case, by the NestJS backend's REST API. The frontend lists pending tasks and submits approve/reject decisions,
which the backend publishes back to Kafka as `{team_name}.vendor.review.completed`.

![compliance_review HUMAN task configuration](images/wf-07-compliance-review.png)

---

## Step 8: Task — publish\_final\_notification (EVENT)

**Purpose:** After all processing (including any human review), notify the NestJS backend of the
final decision.

### Configuration

| Field | Value |
| --- | --- |
| **Task Type** | `EVENT` |
| **Task Reference Name** | `publish_final_notification` |
| **Sink** | `kafka:one-om-dev-training:one-om-dev-training-message-in-private` |
| **Async Complete** | `true` |

### Input Parameters

```
{
  "metadata": {
    "type": "{team_name}.vendor.onboarding.notify",
    "team": "{team_name}",
    "correlationId": "${workflow.input.correlationId}"
  },
  "payload": {
    "workflowId": "${workflow.workflowId}",
    "taskRefName": "publish_final_notification",
    "vendorId": "${normalize_registration.output.result.vendorId}",
    "companyName": "${normalize_registration.output.result.companyName}",
    "decision": "${route_by_risk.output.selectedCase}",
    "riskScore": "${calculate_risk_score.output.result.score}",
    "riskLevel": "${calculate_risk_score.output.result.level}",
    "reason": "Onboarding ${route_by_risk.output.selectedCase} risk path completed",
    "approvedBy": "system"
  }
}Copy
```

The NestJS backend receives this, logs the notification, simulates email/ERP updates, and sends back an
acknowledgement via Kafka.

![publish_final_notification task configuration](images/wf-08-publish-final-notification.png)

### Create Event Handler: Onboarding Notified

Since this task uses `asyncComplete: true`, the workflow waits here until the backend acknowledges.
Create the event handler:

Navigate to **Definitions > Event Handler** and create:

```
{
  "name": "exp_{team_name}_vendor_onboarding_notified_handler",
  "event": "kafka:one-om-dev-training:one-om-dev-training-message-out-private",
  "condition": "$.metadata.type == '{team_name}.vendor.onboarding.notified'",
  "actions": [
    {
      "action": "complete_task",
      "complete_task": {
        "workflowId": "${payload.workflowId}",
        "taskRefName": "${payload.taskRefName}",
        "output": {
          "vendorId": "${payload.vendorId}",
          "notificationId": "${payload.notificationId}",
          "deliveredTo": "${payload.deliveredTo}",
          "decision": "${payload.decision}",
          "processedAt": "${payload.processedAt}"
        }
      }
    }
  ],
  "active": true,
  "evaluatorType": "javascript"
}Copy
```

---

## Step 9: Task — complete\_workflow (TERMINATE)

**Purpose:** Explicitly end the workflow with a COMPLETED status and output summary.

### Configuration

| Field | Value |
| --- | --- |
| **Task Type** | `TERMINATE` |
| **Task Reference Name** | `complete_workflow` |
| **Termination Status** | `COMPLETED` |

### Workflow Output

```
{
  "vendorId": "${normalize_registration.output.result.vendorId}",
  "companyName": "${normalize_registration.output.result.companyName}",
  "decision": "${route_by_risk.output.selectedCase}",
  "riskScore": "${calculate_risk_score.output.result.score}",
  "riskLevel": "${calculate_risk_score.output.result.level}",
  "completedAt": "${workflow.createTime}"
}Copy
```

![complete_workflow task configuration](images/wf-09-complete-workflow.png)

---

# Part 6: Deploy & Test

## Deploy via JSON

Instead of building the workflow task-by-task in the UI, you can deploy the entire definition via API:

```
curl -X PUT "https://one-dev.orkesconductor.io/api/metadata/workflow" \
  -H "Content-Type: application/json" \
  -H "x-authorization: <YOUR_TOKEN>" \
  -d @exp_{team_name}_vendor_onboarding_workflow.jsonCopy
```

The complete workflow JSON is available at:
`orkes/exp_{team_name}_vendor_onboarding_workflow.json`

## Verify All Event Handlers

Before testing, verify that all **6 event handlers** are created and **Active**:

| # | Handler | Created In | Related Task |
| --- | --- | --- | --- |
| 1 | `exp_{team_name}_vendor_onboarding_trigger` | [Part 4](#part-4-create-the-trigger-event-handler) | — (starts workflow) |
| 2 | `exp_{team_name}_vendor_doc_verified_handler` | [Step 4](#step-4-task--verify_documents_loop-do_while) | `publish_doc_verify` |
| 3 | `exp_{team_name}_vendor_sanctions_screened_handler` | [Step 5](#step-5-task--parallel_compliance_checks-fork_join) | `publish_sanctions_screen` |
| 4 | `exp_{team_name}_vendor_insurance_validated_handler` | [Step 5](#step-5-task--parallel_compliance_checks-fork_join) | `publish_insurance_validate` |
| 5 | `exp_{team_name}_vendor_license_verified_handler` | [Step 5](#step-5-task--parallel_compliance_checks-fork_join) | `publish_license_verify` |
| 6 | `exp_{team_name}_vendor_onboarding_notified_handler` | [Step 8](#step-8-task--publish_final_notification-event) | `publish_final_notification` |

Go to **Definitions > Event Handler** and confirm all 6 show a green **Active**
status. If any are missing, the corresponding workflow task will stay `IN_PROGRESS` indefinitely.

## Test End-to-End

1. Start the NestJS backend: `make dev`
2. Open the frontend at `http://localhost:3001`
3. Navigate to **Register Vendor**
4. Click **MEDIUM Risk** preset and submit
5. Watch the workflow execute in Orkes UI (**Executions > Workflow**)
6. Navigate to **Human Tasks** in the frontend
7. Click **View Details** on the pending task
8. Approve or reject the vendor
9. Verify the workflow completes in Orkes

### Expected Flow (MEDIUM Risk)

![Diagram](images/diagrams/diagram-8.png)

![Workflow execution completed](images/10-executions-completed.png)

---

# Troubleshooting

| Issue | Solution |
| --- | --- |
| Event handler not triggering | Check that the handler is **Active** (not paused) in Definitions > Event Handler |
| Wrong messages triggering | Verify the `condition` uses the correct team-prefixed type: `{team_name}.vendor.xxx` |
| Other team's messages triggering | Ensure the team prefix in `metadata.type` matches your team name |
| Workflow not found | Ensure `exp_{team_name}_vendor_onboarding` is deployed before creating the trigger handler |
| No messages arriving | Check Kafka connectivity in **Integrations** and verify the topic name |
| Task stuck in IN\_PROGRESS | Missing event handler for that task's response. Check the [event handler checklist](#verify-all-event-handlers) |
| Workflow times out | Default timeout is 3600s. Check if a task is stuck (missing handler) or the backend is not running |
| Human task not appearing | Verify the SWITCH routed to MEDIUM/HIGH. Check the review request EVENT was published |
| Backend not processing | Ensure `make dev` is running and Kafka consumer is connected (check backend logs) |

---

# Summary

You've built a complete vendor onboarding workflow that:

* **Triggers** from Kafka messages via an event handler
* **Normalizes** raw Kafka data with jq
* **Iterates** over a dynamic list of documents
* **Parallelizes** 3 compliance checks
* **Computes** a risk score with inline JavaScript
* **Routes** to different approval paths based on risk
* **Pauses** for human review when needed
* **Notifies** the backend of the final decision

All communication between Orkes Conductor and NestJS happens through **Kafka messages**, with
**6 event handlers** bridging the two systems:

* 1 trigger handler (`start_workflow`) to kick off the workflow
* 5 completion handlers (`complete_task`) to resume async tasks when the backend responds

↑
