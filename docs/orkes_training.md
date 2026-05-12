Orkes Training
2026
Mar 2026
Dat Nguyen TH
Copyright © Ocean Network Express Pte. Ltd. All Rights
Reserved

Agenda
No. Title Duration
1 Why Workflow Orchestration 15 min
Business value, use cases, the problem it solves
2 Orkes Conductor Core Concepts 15 min
Execution model, key task types, workflow patterns
3 Chorus Architecture Standard 20 min
Reference architecture, live workflow execution in Orkes UI
4 Guided Implementation Walkthrough 1 hour
Code walkthrough: workers, Kafka, human tasks, production controls
2
Copyright © Ocean Network Express Pte. Ltd. All Rights
Reserved

Why Workflow Orchestration
CCooppyyrriigghhtt ©© OOcceeaann NNeettwwoorrkk EExxpprreessss PPttee.. LLttdd.. AAllll RRiigghhttss
RReesseerrvveedd

Ad-hoc Process Coordination Breaks at Scale
Leading to operational blind spots and lost engineering velocity
Operational Symptoms Technical Root Cause
Failed step leaves export booking stuck → No durable execution model; state is lost on crash
Operations cannot track stalled shipments → Process state scattered across tables and logs
Business rule changes require weeks of recoding → Orchestration logic entangled with business logic
DG incident requires manual log correlation → No first-class event history or audit trail
Missed transshipment — no cut-off alert → No built-in timeout or deadline enforcement
Sprints wasted building retry logic → Every product team reinvents infrastructure
Copyright © Ocean Network Express Pte. Ltd. All Rights
Reserved

Workflow Orchestration Engine
Centralizes process coordination to guarantee durable execution and visibility
| Durable Execution |     | First-Class Audit Trail |
| ----------------- | --- | ----------------------- |
Persists state after every step; resumes on crash Immutable event history records every transition
Workflow Engine
| Visibility by Default | (The What) | SLA Enforcement |
| --------------------- | ---------- | --------------- |
Every instance queryable with step-by-step  Workflow Engine Built-in detection and action for timeouts
history
| Declarative Control |     | Decoupled Evolution |
| ------------------- | --- | ------------------- |
Timeouts, retries, branching in workflow  Change business rules without refactoring
definition
Copyright © Ocean Network Express Pte. Ltd. All Rights
Reserved

When to Orchestrate
Processes exhibiting 3+ coordination signals require workflow orchestration
Signal Exclusion Matrix (What NOT to Orchestrate)
Multiple services must coordinate in a specific order
Simple request-response → Direct function calls
→ Manual service chaining is fragile and invisible
A human must approve, review, or intervene mid-process High-frequency events (>10K/sec) → Kafka Streams / Flink
→ Async human tasks require durable state and timeout enforcement
The process spans minutes, hours, or days Pure data transformations → ETL tools
→ In-memory state is insufficient; the engine must persist progress
Failure in step N requires undoing steps 1 through N-1 Stateless notifications → Message Queue + Consumer
→ Compensation logic needs a saga coordinator
Regulatory or audit requirements demand a decision trail Basic CRUD operations → Standard APIs
→ The engine's execution history IS the audit trail
Different business rules route the process to different paths
→ SWITCH-based branching is declarative in a workflow, ad-hoc in code
SLAs or deadlines must be enforced automatically
→ Timeout and escalation are first-class workflow primitives
The same subprocess is reused across multiple processes
→ SUB_WORKFLOW enables composition and reuse
6
Copyright © Ocean Network Express Pte. Ltd. All Rights
Reserved

Why Orkes Conductor
Proven Heritage Cloud-Native & Unconstrained Built-in Operational
& Scale Event-Driven Polyglot Model Tooling
Open-source engine Runs on Kubernetes, Workers scale independently Visual workflow design,
orchestrates millions daily. integrates with in NestJS, TypeScript, Java, execution timeline, task-level
Orkes provides the Prometheus/Grafana, Python, Go within the same I/O inspection, manual
managed, SLA-backed supports Kafka triggers. workflow. retries, replay.
enterprise evolution.
Copyright © Ocean Network Express Pte. Ltd. All Rights
Reserved

Orkes Conductor Core Concepts
CCooppyyrriigghhtt ©© OOcceeaann NNeettwwoorrkk EExxpprreessss PPttee.. LLttdd.. AAllll RRiigghhttss
RReesseerrvveedd

Architectural Patterns (1/6)
1. Sequential Pipeline
A fixed sequence of automated steps, each feeding output to the next. The engine retries transient failures and enforces timeouts at every step.
Example: Export booking — validate cargo details → check vessel capacity → confirm rate → issue booking confirmation → dispatch terminal instructions
Conductor features: SIMPLE tasks with retryCount, retryLogic: EXPONENTIAL_BACKOFF, timeoutSeconds, responseTimeoutSeconds on each task.
Copyright © Ocean Network Express Pte. Ltd. All Rights
Reserved

Architectural Patterns (2/6)
2. Decision-Driven Flow
The process routes to different paths based on data — an amount, a risk score, a
customer tier, or a policy rule.
Example: Container release routing — route by cargo type: dangerous goods to
mandatory inspection, reefer to temperature verification, standard dry to
auto-release
Conductor features: SWITCH task with evaluatorType: javascript or value-param.
INLINE tasks for lightweight computation. Always define a defaultCase.
Copyright © Ocean Network Express Pte. Ltd. All Rights
Reserved

Architectural Patterns (3/6)
3. Human-in-the-Loop
The workflow pauses at one or more stages for a human to review, approve, reject, or provide input. The
engine enforces SLA deadlines and escalates automatically.
Example: Dangerous goods approval — automated hazmat class validation, then operations supervisor
review with 4h SLA, escalate to safety officer if breached
Conductor features: HUMAN task with `__humanTaskDefinition` (assignments, `slaMinutes`,
`assignmentCompletionStrategy`). Always add `timeoutSeconds` as a hard backstop beyond the SLA. Use
async completion API for programmatic approve/reject.
Copyright © Ocean Network Express Pte. Ltd. All Rights
Reserved

Architectural Patterns (4/6)
4. Saga / Compensation
A sequence of steps where each step has a corresponding "undo" action. If step N fails, the workflow
automatically compensates steps N-1 through 1 in reverse order.
Example: Export booking — reserve vessel slot → collect surcharges → issue bill of lading. If B/L issuance
fails, reverse surcharge collection → release vessel slot.
Conductor features: Define a `failureWorkflow` on the main workflow that triggers compensation steps. Use
`SET_VARIABLE` to store rollback context (e.g., reservation ID, payment ID) as the workflow progresses.
Each compensation step reads these variables to undo the corresponding action.
Copyright © Ocean Network Express Pte. Ltd. All Rights
Reserved

Architectural Patterns (5/6)
5. Scatter-Gather (Parallel Fan-Out)
Multiple independent tasks run in parallel. The workflow waits for all (or a subset) to complete, then
aggregates results.
Example: Trade compliance screening — run sanctions check, commodity embargo check, and export
licence validation simultaneously, then aggregate results for booking approval
Conductor features: `FORK_JOIN` for a static set of parallel tasks. `FORK_JOIN_DYNAMIC` when the
number of parallel branches is determined at runtime (e.g., run one check per vendor document). `JOIN`
with `joinOn` referencing all fork branch outputs.
Copyright © Ocean Network Express Pte. Ltd. All Rights
Reserved

Architectural Patterns (6/6)
6. Iterative Loop
A block of tasks repeats until a condition is met — a document is approved, retries are exhausted, or a
threshold is reached.
Example: BL amendment — shipper submits correction → system validates → if invalid or further changes
requested, notify and wait for re-submission → repeat until approved or max attempts reached
Conductor features: `DO_WHILE` with `loopCondition` (JavaScript expression evaluating iteration count
and task output). The loop body can contain any combination of task types. Use `loopOver` to define the
tasks inside the loop..
Copyright © Ocean Network Express Pte. Ltd. All Rights
Reserved

Capability Building Matrix
Targeted capability building across product, architecture, and engineering domains
| Capabilities | PPO | Technical Architect | Developer |
| ------------ | --- | ------------------- | --------- |
Business Capabilities
| Identify orchestration-fit processes | Owns | Reviews | Aware  |
| ------------------------------------ | ---- | ------- | ------ |
| Translate policies/SLAs to           | Owns | Reviews | Builds |
workflows
| Use operational dashboards for  | Owns | Reviews | Aware |
| ------------------------------- | ---- | ------- | ----- |
monitoring
Technical Capabilities
| Model complex workflows &  | Aware | Owns | Builds |
| -------------------------- | ----- | ---- | ------ |
evaluate patterns
| Implement NestJS + Kafka workers |     | Reviews | Builds |
| -------------------------------- | --- | ------- | ------ |
| Configure retry, timeout, and    |     | Reviews | Builds |
compensation
Owns = Accountable, drives decisions    Reviews = Validates and provides input    Builds = Hands-on implementation    Aware = Kept informed
Copyright © Ocean Network Express Pte. Ltd. All Rights
Reserved

Activate: Launch Your Pilot
Step 2: Score Against
Step 1: Identify Processes Step 3: Classify Pattern Step 4: Launch Pilot
Signals
List processes with 3+ Evaluate using the Map requirements to Orkes Prioritize highest business
steps, cross-service Diagnostic Scorecard: pattern: Sequential, impact. Build NestJS
boundaries, or human multi-service, durable state, Scatter-Gather, workers and define JSON
intervention SLA enforcement Human-in-the-Loop, Saga workflow in Orkes
Copyright © Ocean Network Express Pte. Ltd. All Rights
Reserved

Chorus Architecture Standard
& Live Demo
CCooppyyrriigghhtt ©© OOcceeaann NNeettwwoorrkk EExxpprreessss PPttee.. LLttdd.. AAllll RRiigghhttss
RReesseerrvveedd

Chorus Workflow Overview
Enables developers to focus strictly on implementing business logic rather than the mechanics of service
communication, flow durability, and failure handling.
Microservices
Orkes
Business Logic
Conductor
(Developer Focus)
Microservices
Microservices
Copyright © Ocean Network Express Pte. Ltd. All Rights
Reserved

The Four Core Architectural Principles
1. Orchestration 2. Event-Driven & 3. Strict Database 4. Backend Operation
Layer Only Asynchronous Isolation Guardrails
Orkes provides traceability Communication relies on an For security, Orkes has zero If using Orkes for backend
for business processes. It is event-driven architecture direct access to the operations, acknowledge the
not a no-code/low-code utilizing Kafka. Expect database. All database tasks asynchronous nature.
solution for replacing delays; operations are must be handled on the Evaluate on a strict
complex backend systems. fundamentally asynchronous. backend. case-by-case basis.
Copyright © Ocean Network Express Pte. Ltd. All Rights
Reserved

Integration Paradigm: Shift from Polling to Pub/Sub
AVOID: Job Worker Polling (Simple Tasks) Simple Tasks incur massive,
unexpected resource drains on the
GKE cluster and misalign with
Polling
Polling BackeBndackend event-driven architecture.
Simple task Simple task
Simple Task Job Worker The polling model creates constant
network overhead even when idle.
Recommended: Pub/Sub Model
Integrate CHORUS backend and Orkes
Produce Consume workflows using a three-step chain:
Backend
Kafka
Event Task Kafka Consumer 1. Event Task (Produces message).
Topic (IN)
2. Wait Task (Pauses execution).
Kafka Producer
3. Event Handler (Consumes and signals)
Wait Task Kafka Topic
(OUT)
Produce
Consume
Event
Handler
Copyright © Ocean Network Express Pte. Ltd. All Rights
Reserved

The Standardized Kafka Event Envelope
Purpose: Ensures consistent schema evolution, end-to-end traceability, and observability across ONEForce and CHORUS.
Standardized Kafka JSON Message
Metadata Object (Operational) Payload Object (Business)
● messageId: UUID for deduplication/ auditing. ● assignee: Used to assign a task to a
● correlationId: Critical for end-to-end tracing user/group.
across services. ● taskDisplayName: Renders on the My Task
● eventType, sourceSystem, screen..
destinationSystem, requester, timestamp. ● workflowId: Required for all messages from
CHORUS to external systems.
Copyright © Ocean Network Express Pte. Ltd. All Rights
Reserved

Ul Integration: Managing the 'My Task' Screen
Overview: CHORUS displays pending human tasks to logged-in users via direct Orkes integration.
Assignee: The pool or group (e.g.,
'BPM Group - GHQ') defined in the
workflow design. An assignee is
not necessarily the party acting on
the task.
Claimant: Updated manually when
a user claims the task. The
claimant is the actual party acting
on the task.
Available Actions
1. Assign | 2. Claim (changes claimant to logged-in user) | 3. Release (return task to assignee pool)
Copyright © Ocean Network Express Pte. Ltd. All Rights
Reserved

Task Building Blocks: Rules, States, and Anti-Patterns
Business Rule Task Update Task
Evaluates rules compiled in tabular format (CSV, XLS, XLSX). Dynamically modifies the status or output of a running task
Consolidates complicated, nested if-else statements into clean (e.g., completing long- running tasks or exiting loops) without
logic. restarting the workflow.
JDBC Task [AVOID]
All database operations must be performed exclusively through backend services. Direct DB interaction from Orkes is
strictly prohibited.
Copyright © Ocean Network Express Pte. Ltd. All Rights
Reserved

Configuring Human-in-the-Loop Interventions
Naming Convention
Use Pascal Case with intuitive ‘Verb + Noun’ phasing
(e.g., Approve Requests)
Assignment Policies
Target EXTERNAL_USER (@one- line email) or
EXTERNAL_GROUP.
Structure in a hierarchy.
Set User ID for specific external user or group name.
SLAs & Escalation
Set minute-based SLAs (e.g., SLA Minutes:
numeric). Once exceeded, automatically escalates to
the next assignee in line if applicable.
Naming Convention
Automate workflows (like email notifications) the
moment a task is assigned. Configure trigger event
and next workflow to start.
Strict Ul Rule: Orkes drag-and-drop User Forms are strictly prohibited in production. All forms must be built within the product using the CHORUS
Design System.
Copyright © Ocean Network Express Pte. Ltd. All Rights
Reserved

Workflow Assembly: Reusability & Saga Patterns
Sub Workflows
Main workflow Sub workflow
Extract repeating series of tasks into shareable workflows.
Improves readability and promotes modular reusability
across CHORUS.
Failure Workflows (Compensation)
Triggers automatically when an execution fails.
Mirrors the compensation transaction aspect of a Saga
Main workflow Error
pattern in microservice architectures.
Note: Must be designed and created alongside the primary
workflow.
Failure workflow
Copyright © Ocean Network Express Pte. Ltd. All Rights
Reserved

State Management: Parameters vs. Variables
Inputs (Immutable) Variables (Mutable)
● Defined at the start and exit of an instance. ● Declared, initialized, and modified dynamically using the
● Cannot be changed during execution. Set Variable operator task.
● Accessed via ${workflow.input.[inputParameterName]}. ● Only exist within the context of the current execution.
● Accessed via ${workflow.variables.[variableName]}.
Copyright © Ocean Network Express Pte. Ltd. All Rights
Reserved

Triggers & Automation: Event Handlers & Schedulers
Event Handlers
Event Handler
Kafka Topic Actively listen for Kafka messages. Can
be configured to Complete/Fail tasks, or
Start/Terminate workflows based on JSON
conditions ($.type == 'Created').
Schedulers
Scheduler
Run workflows at a predefined cadence using
Clock/Calendar
standard Cron expressions (e.g., 0 0*?**).
Copyright © Ocean Network Express Pte. Ltd. All Rights
Reserved

Webhooks: Pausing for External Callbacks
Sendgrid,
Twillo, etc.
Workflow Start Workflow Start
Wait for Webhook
Mechanism Implementation Capabilities
Unlike event handlers, Webhooks actively wait Must pair the globally defined Webhook Ensures reliability with configurable
for HTTP callbacks from external sources with a Wait For Webhook Task inside timeouts, authenticates payloads, and
(GitHub, Twilio, custom services)
the specific workflow. can be configured to dynamically spawn
new workflows upon receipt.
Copyright © Ocean Network Express Pte. Ltd. All Rights
Reserved

Platform Configuration & Security
Environment Variables Non-sensitive, platform/product-wide Accessed via ${workflow.env.[NAME]}.
constants (e.g., DOWNLOAD_FOLDER).
Handled by OM-COM TAs via JIRA
requests.
Secrets Encrypted in Google Secret Manager. Accessed via ${workflow.secrets.[NAME]}.
Scrum teams are responsible for securing
and tagging these.
Copyright © Ocean Network Express Pte. Ltd. All Rights
Reserved

Architecture Illustration (High-level)
CHORUS User CHORUS Operator User
Informed workflow status Setup workflow
CHORUS
Orkes
Get tasks being Change workflow
Common assigned to me Workflow execution status Domain App
Workflow App APIs (SPM, APM, etc.)
Publish, consume
Workflow
Engine
Kafka messages
REST APIs REST APIs
Extract info from
Send email
invoice (OCR) - store received invoices (.pdf)
notification
- store extracted invoice data (.json)
Google
Sendgrid
Document AI
Google Storage
Copyright © Ocean Network Express Pte. Ltd. All Rights
Reserved

Architecture Illustration (Low-level)
Orkes (External)
APIs & Tools Workflows
CHORUS (Internal)
Kafka
REST, gRPC
GraphQL
Workflow UI
gRPC
BFF BE
GraphQL
Other Modules UIs Databases
(CSR, APM, SPM)
Front end (NextJs) Back end (NestJs) + Postgresql
Copyright © Ocean Network Express Pte. Ltd. All Rights
Reserved

Guided Implementation Walkthrough
CCooppyyrriigghhtt ©© OOcceeaann NNeettwwoorrkk EExxpprreessss PPttee.. LLttdd.. AAllll RRiigghhttss
RReesseerrvveedd

Codelab
https://github.com/ocean-network-express/orkes-training-vietnamese
Copyright © Ocean Network Express Pte. Ltd. All Rights
Reserved

Thank you
