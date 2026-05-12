Kafka Development Guideline
Intro
This is the problem Schema Registry and Avro solve.
Core Concepts
What is Avro?
What is Schema Registry?
Naming Convention
Topic Naming Convention:
Subject / Schema Naming Convention:
TopicNameStrategy
Avro format Guide
Example of an Avro Schema:
Tool support
OPTIONAL Attributes
Schema manager Repository
Folder Structure
Branch Strategy
Pipeline
Trigger Conditions
Pipeline Steps Breakdown
Development Guideline
Library Standard
Basic Connection Configuration:
Producer Development Guide
Consumer Development Guide
Kafka Runner
Orkes Schema Registry Setup Guide
Integrations
Consumer in Orkes
Producer in Orkes
Schema Evolution and Maintenance Guidelines
Evolution Strategy
Explain Evolution Strategy
Schema and Dataset Violation
Schema Evolution flow
Type 1. Producer and Consumer application
Type 2. Producer or Consumer is Orkes
Schema Rollback
Case 1. For application
Case 2. For Orkes
Version Management
R&R
References
Schema Validation

Intro
As we scale our use of Kafka, managing the "shape" of data becomes our biggest challenge.
You may have already faced questions like:
"What data is in this topic?", “What kind of data the micro service provide?”
"A consumer just broke. Did the producer change the JSON format?"
"How do we update a message format without breaking all downstream services?"
Currently, Kafka sees messages as just an array of bytes. It doesn't enforce any structure.
This is the problem Schema Registry and Avro solve.
Apache Avro is a data serialization system. It defines a data structure using a language-
neutral schema (usually in JSON format).
Confluent Schema Registry is a centralized, external service that stores, versions, and
manages your Avro schemas.
By using them together, we move from "data-as-bytes" to "data-as-a-contract," ensuring that
all data written to and read from Kafka is 100% valid and well-defined.
Core Concepts
What is Avro?
Avro is a schema-based serialization format. Think of it like a more robust, efficient, and
evolvable version of JSON.
Schema-First: You must first define the structure of your data in an .avsc file (which is just
JSON).
Binary Format: When you serialize data (e.g., a Java object) with an Avro schema, the output
is a very compact binary format, not JSON. This is smaller and faster to transmit over the
network.
Schema Evolution: Avro is designed to handle schema changes (like adding or removing
fields) gracefully, so new producers don't break old consumers, and vice-versa.
Here is a simple Avro schema for a User :
one-om-dev-poc-com-ian-notification-in-private-value.avsc
1 {
2 "namespace": "chorus.com.ian",

3 "type": "record",
4 "name": "one_om_dev_com_ian_notification_in_private",
5 "doc": "Avro schema for ian notification in private.",
6 "fields": [
7 {
8 "doc": "The notification title.",
9 "name": "title",
10 "type": "string"
11 },
12 {
13 "doc": "The link to the details page.",
14 "name": "options",
15 "type": ["null", "string"],
16 "default": null
17 }
18 ]
19 }
What is Schema Registry?
Schema Registry is a server (a separate process) that acts as the "single source of truth" for
your schemas.
Centralized Storage: It stores a versioned history of all schemas for all topics.
Schema Validation: When a producer tries to write data, it first registers its schema with the
registry. The registry checks if this new schema is compatible with previous versions.
Global Schema ID: If the schema is new (or a new version), the registry assigns it a unique,
global ID.

Naming Convention
| Category |     | Convention |     | Example |
| -------- | --- | ---------- | --- | ------- |
TOPIC name Follwing  https://docs.google.com/spreads one-om_com-dev-error-
heets/d/1PMpNrIQedFuJBwceEW_HezqYnja report-in-private
peN3Zpvht12RicU4/edit?gid=0#gid=0 Conn
ect your Google account
Subject name in Following  https://oneline.atlassian.net/wiki/ one-om_com-dev-error-
the Confluent spaces/BTH/pages/3259203792 Request a report-in-private-value
ccess
| File name of the  | Overall, the naming convention adheres to     |     |         |     |
| ----------------- | --------------------------------------------- | --- | ------- | --- |
| Avro in the       | the Schema Subject standard. However, the     |     |         |     |
| Github            | delimiter used to denote the environment will |     |         |     |
| Repository        | be replaced by the phrase                     |     | {env} . |     |
name  field in The naming convention will follow the topic 1 "type": "record",
2 "name":
| the  Avro  file | name (excluding environment details), with |     |     |     |
| --------------- | ------------------------------------------ | --- | --- | --- |
"one_om_com_error_report_in
_private",
all dashes replaced by underscores.
3 "fields": [
Topic Naming Convention:
1 {ORG}-{APP}-{ENV}-{FEATURE}-{TYPE}
2 eg) one-om_com-dev-error-report-in-private
For detail, please refer this naming convention  https://docs.google.com/spreadsheets/d/1P
MpNrIQedFuJBwceEW_HezqYnjapeN3Zpvht12RicU4/edit?gid=0#gid=0 Connect your Googl
e account
Subject / Schema Naming Convention:
TopicNameStrategy
Uses the topic name to determine the subject to be used for schema lookups. The subjects with
| appended suffixes such as  | -value | .   |     |     |
| -------------------------- | ------ | --- | --- | --- |

Topic: eg) one-om_com-dev-ian-notification-in-private
Schema (subject name): <topic name>-value, eg) one-om_com-dev-ian-
notification-in-private-value
Avro format Guide
name: a JSON string providing the name of the record (required).
namespace, a JSON string that qualifies the name (optional);
doc: a JSON string providing documentation to the user of this schema (optional).
aliases: a JSON array of strings, providing alternate names for this record (optional).
fields: a JSON array, listing fields (required)
Primitive type: Avro supports all the primitive types. We use primitive type name to define
a type of a given field. For example, a value which holds a String should be declared as
{“type”: “string”} in the schem
Complex type: Avro supports six kinds of complex types: records, enums, arrays, maps,
unions and fixed
Example of an Avro Schema:
1 {
2 "namespace": "chorus.com.ian", ## Follows the GitHub repository directory domain.
3 "type": "record",
4 "name": "one_om_com_ian_notification_out_private", ## The naming convention will follow
the topic name (excluding environment delimiter), with all dashes replaced by underscores.
"doc": "Avro schema for ian notification out private.", ## Description
5 "fields": [
6 {
7 "doc": "The REQUIRED field",
8 "name": "title",
9 "type": "string"
10 },
11 {

| 12 "doc":  | "The COMPLEX type field", |                   |            |
| ---------- | ------------------------- | ----------------- | ---------- |
| 13 "name": | "recipients",             |                   |            |
| 14 "type": | { "type":                 | "array", "items": | "string" } |
15 },
16 {
| 17 "doc":     | "The OPTIONAL field", |            |     |
| ------------- | --------------------- | ---------- | --- |
| 18 "name":    | "location",           |            |     |
| 19 "type":    | ["null",              | "string"], |     |
| 20 "default": | null                  |            |     |
21 }
22 ]
23 }
Tool support
To get familiar with the Avro format, you can refer to this conversion tool :  CodeUtil - Free Dev
eloper Tools
You can input JSON data here.
Then click the “Convert to Avro Schema” button here. This tool supports many data types such
as :
| String →  string                            |           |     |     |
| ------------------------------------------- | --------- | --- | --- |
| Integer →  int                              |  or  long |     |     |
| Decimal →                                   | double    |     |     |
| Boolean →                                   | boolean   |     |     |
| Array →  { type: "array", items: ... }      |           |     |     |
| Object →  { type: "record", fields: [...] } |           |     |     |

Date/Time Logical Types (when auto-detect enabled):
ISO Date (YYYY-MM-DD) → { type: "int", logicalType: "date" }
ISO DateTime (ISO 8601) → { type: "long", logicalType: "timestamp-
millis" }
Unix Timestamp (milliseconds) → { type: "long", logicalType: "timestamp-
millis" }
Important note : This tool can only force all fields to be optional. It is a supporting tool, so you
will need to review and manually adjust the union types. Verifying and reviewing the schema is

also necessary to quickly become familiar with the Avro schema.
For more Avro specification : Specification
OPTIONAL Attributes
The Avro format defines that these fields MUST use the null type and the actual type through
the union.
Example:
1 ["null", "string"]
Schema manager Repository
https://github.com/ocean-network-express/om-schema-registry Connect your Github accou
nt
Folder Structure
All avro files should be organized in the directory structure mirroring their namespace. Different
versions of avro files must be stored in their respective version folder. The document
corresponding to each Avro file is exported and saved with an identical file structure.
1 ├── docs ### AsyncAPI Document Export Path
2 │ └── chorus
3 │ └── {domain}
4 │ ├── {module}
5 │ │ ├── asyncapi-{topic_name}.yaml
6 ...
7
8 └── schemas ### Schema
9 └── chorus
10 │ └── {domain}
11 │ ├── {module}
12 │ │ ├── {topic_name}-value.avsc
13 ...
Branch Strategy
Branch name Corresponding Schema Corresponding Cluster ID
Registry (Public Endpoint)

| production |     | https://psrc-qzvrm.asia- |     | lkc-oq397j |
| ---------- | --- | ------------------------ | --- | ---------- |
southeast1.gcp.confluent.clou
stage lkc-nwvo16
d
test lkc-ymoq5o
develop
Environment/Branch Action (Merge/PR) Schema Registry Kafka Cluster
| (GitHub) |     |     | Subject |     |
| -------- | --- | --- | ------- | --- |
develop  branch PR: Review feature one-om_com-dev- Confluent Develop
|     | from develop → |     | error-report-in- |     |
| --- | -------------- | --- | ---------------- | --- |
|     | Merge          |     | private-value    |     |
test  branch PR: Review feature one-om_com-test- Confluent Develop
|     | from test  → Merge |     | error-report-in- |     |
| --- | ------------------ | --- | ---------------- | --- |
private-value
| stage  branch | PR: Review feature |     | one-om_com-  | Confluent UAT |
| ------------- | ------------------ | --- | ------------ | ------------- |
|               | from stage → Merge |     | stage-error- | (=Stage)      |
report-in-
private-value
production  branch PR: Review feature one-om_com-prod- Confluent Production
|     | from production → |     | error-report-in- |     |
| --- | ----------------- | --- | ---------------- | --- |
|     | Merge             |     | private-value    |     |

Pipeline
Trigger Conditions
The pipeline runs under a condition: Whenever changes are pushed to a branch that modify files
matching the pattern schemas/**.avsc (any Avro schema files in the schemas directory)
Pipeline Steps Breakdown
Setup Phase
1.
Checkout Code: Downloads the repository content
Change Detection
2.
Detect Modified Schemas
Schema Validation
3.
Pre-check the validation of the schema
Schema Registration
4.
Register with Schema Registry
Confluent Cloud Authentication
5.
CLI Login: Authenticates with Confluent Cloud using stored credentials
Cluster Configuration: Sets up connection to specific Kafka cluster
API Key Management: Configures service account API keys for operations
AsyncAPI Documentation Generation
6.
Export AsyncAPI Specs
Schema Metadata Enhancement
7.
Version ID Integration: Retrieves latest schema version ID from Schema Registry
Documentation Persistence
8.
Commits generated AsyncAPI files to the repository
Development Guideline
Library Standard
Why choose the confluent-kafka-javascript library ? ( Document )
Origin : Developed and maintained by Confluent, the founding team of Apache Kafka. It build
on top of librdkafka a C/C++ library used by most other official clients (such as C, Go, and

.NET). Thereby ensuring performance, reliability and feature consistency.
Performance: inherit the full power of librdkafka, meaning it real-word production
performance is comparable to native C/C++ and Java client
Confluent Cloud Support : provides native integration with Confluent Cloud and Schema
Registry. It supports OAuth 2.0, API Key, and SASL_SSL directly in the client configuration. It
includes build-in Schema Registry integration with Avro, Protobuf and JSON Schema
encode/decode capabilities
Develop Experience: design for modern Javascript/Typescript developer. It provides both
Promise-base and Callback-based API, making easy to integrate with any framework ,
allowing developers to implement producer/consumer with just a few lines of code while
leveraging Confluent Cloud debugging and logging
Document and Maintenance: Confluent maintains on the same release as other official
clients. It update synchronized with Kafka core, comprehensive and official documentation is
available, support is provided when using the confluent cloud
Deployment Environment : engineered for stability across all modern Nodejs deployment
environments, including container (Docker, Kubernetes, cloud-native) and serverless/edge
platform ( AWS , Google Cloud Functions). It integrates smoothly with CI/CD due to it build-
free setup and supports Windows, Linux and MacOS without extra system dependencies
Basic Connection Configuration:
(Full Source Code Guideline)
Topic Constant Guideline
Create a file at src/common/constants/kafka-topics.constant.ts and declare the
Topic names and Schema ID inside it.
1 // src/common/constants/kafka-topics.constant.ts
2 export const KAFKA_TOPICS = {
3 ERROR_REPORT_IN: process.env.KAFKA_TOPIC_ERROR_REPORT_IN ||'one-om-dev-poc-com-ian-
notification-in-private',
4 } as const;
1 // src/common/constants/kafka-topics.constant.ts
2 export const SCHEMA_REGISTRY_VERSION = {
3 ERROR_REPORT_IN: process.env.SCHEMA_ERROR_REPORT_IN || 100126,
4 };
Secret key Guideline
Add .env file and declare secret key inside it.
1 KAFKA_BROKER = 'your-broker'
2 KAFKA_API_KEY= 'your-api-key'
3 KAFKA_API_SECRET= 'your-api-secret'
4 KAFKA_GROUP_ID = 'your-group'

5
6 KAFKA_TOPIC_ERROR_REPORT_IN='your-feature-error-report'
7
| 8 SCHEMA_REGISTRY_URL=         |     |     | 'your-url-registry' |                            |     |     |     |
| ------------------------------ | --- | --- | ------------------- | -------------------------- | --- | --- | --- |
| 9 SCHEMA_REGISTRY_API_KEY=     |     |     |                     | 'your-registry-api-key'    |     |     |     |
| 10 SCHEMA_REGISTRY_API_SECRET= |     |     |                     | 'your-registry-api-secret' |     |     |     |
| 11 SCHEMA_ERROR_REPORT_IN=     |     |     |                     | 'your-schema-id'           |     |     |     |
12
13
Basic Security Settings (SASL/SSL)) with Kafka client :
Here is sample for connect with Kafka client :
| 1 import | { Injectable |     | } from | '@nestjs/common';                 |     |     |     |
| -------- | ------------ | --- | ------ | --------------------------------- | --- | --- | --- |
| 2 import | { KafkaJS    |     | } from | '@confluentinc/kafka-javascript'; |     |     |     |
3
4 @Injectable()
| 5 export        | class | KafkaClientService |     |                | {                 |     |     |
| --------------- | ----- | ------------------ | --- | -------------- | ----------------- | --- | --- |
| 6 private       |       | readonly kafka:    |     | KafkaJS.Kafka; |                   |     |     |
| 7 private       |       | readonly producer: |     |                | KafkaJS.Producer; |     |     |
| 8 private       |       | readonly consumer: |     |                | KafkaJS.Consumer; |     |     |
| 9 constructor() |       |                    | {   |                |                   |     |     |
10 // ---- KafkaJS client connect ----
| 11 this.kafka |          | =   | new KafkaJS.Kafka({ |     |     |     |     |
| ------------- | -------- | --- | ------------------- | --- | --- | --- | --- |
| 12            | kafkaJS: | {   |                     |     |     |     |     |
13 clientId: process.env.KAFKA_CLIENT_ID ?? 'your-client-id',
| 14  | brokers: |                                        | [process.env.KAFKA_BROKER |     |     | ??  | 'your-broker'],    |
| --- | -------- | -------------------------------------- | ------------------------- | --- | --- | --- | ------------------ |
| 15  | ssl:     | true,                                  |                           |     |     |     |                    |
| 16  | sasl:    | {                                      |                           |     |     |     |                    |
| 17  |          | mechanism:                             | 'plain',                  |     |     |     |                    |
| 18  |          | username: process.env.KAFKA_API_KEY    |                           |     |     |     | ?? 'your-api-key', |
| 19  |          | password: process.env.KAFKA_API_SECRET |                           |     |     |     | ?? 'your-secret',  |
| 20  | },       |                                        |                           |     |     |     |                    |
| 21  | },       |                                        |                           |     |     |     |                    |
22 });
23
| 24 this.producer |                                        |     | = this.kafka.producer(); |     |     |     |               |
| ---------------- | -------------------------------------- | --- | ------------------------ | --- | --- | --- | ------------- |
| 25 this.consumer |                                        |     | = this.kafka.consumer({  |     |     |     |               |
| 26               | 'group.id': process.env.KAFKA_GROUP_ID |     |                          |     |     | ??  | 'your-group', |
27 });
28 }
29
30 // Get producer client instance
| 31 getProducer(): |     |                | KafkaJS.Producer |     | {   |     |     |
| ----------------- | --- | -------------- | ---------------- | --- | --- | --- | --- |
| 32 return         |     | this.producer; |                  |     |     |     |     |
33 }
34 // Get consumer client instance
| 35 getConsumer(): |     |                | KafkaJS.Consumer |     | {   |     |     |
| ----------------- | --- | -------------- | ---------------- | --- | --- | --- | --- |
| 36 return         |     | this.consumer; |                  |     |     |     |     |
37 }
38 }
Basic Security Settings  with Schema Registry client
Here is sample for connect with Schema Registry client :
| 1 import | {   | Injectable, | Logger |     | } from '@nestjs/common'; |     |     |
| -------- | --- | ----------- | ------ | --- | ------------------------ | --- | --- |
| 2 import | {   |             |        |     |                          |     |     |
3 SchemaRegistryClient,
4 AvroSerializer,
5 AvroDeserializer,
6 SerdeType,
7 SchemaId,
8 AvroSerializerConfig,
9 AvroDeserializerConfig,

| 10 } from | '@confluentinc/schemaregistry'; |        |              |     |
| --------- | ------------------------------- | ------ | ------------ | --- |
| 11 import | { Schema,                       | Type } | from 'avsc'; |     |
12
13 @Injectable()
| 14 export  | class SchemaRegistryClient |     | {                     |     |
| ---------- | -------------------------- | --- | --------------------- | --- |
| 15 private | readonly registry:         |     | SchemaRegistryClient; |     |
| 16 private | readonly serializer:       |     | AvroSerializer;       |     |
| 17 private | readonly deserializer:     |     | AvroDeserializer;     |     |
18 private readonly logger = new Logger(SchemaConfluentRegistryService.name);
19
| 20 constructor() |               | {     |                        |     |
| ---------------- | ------------- | ----- | ---------------------- | --- |
| 21               | this.registry | = new | SchemaRegistryClient({ |     |
| 22               | baseURLs:     | [     |                        |     |
23 process.env.SCHEMA_REGISTRY_URL ?? 'https://your-schema-registry-url',
| 24  | ],                    |     |              |     |
| --- | --------------------- | --- | ------------ | --- |
| 25  | basicAuthCredentials: |     | {            |     |
| 26  | credentialsSource:    |     | 'USER_INFO', |     |
| 27  | userInfo:             |     |              |     |
`${process.env.SCHEMA_REGISTRY_API_KEY}:${process.env.SCHEMA_REGISTRY_API_SECRET}`,
| 28  | },  |     |     |     |
| --- | --- | --- | --- | --- |
29 });
30 // Initialize serializer
| 31  | const avroSerializerConfig: |     | AvroSerializerConfig | = { |
| --- | --------------------------- | --- | -------------------- | --- |
| 32  | useLatestVersion:           |     | true,                |     |
| 33  | autoRegisterSchemas:        |     | false,               |     |
34 };
| 35  | this.serializer       | = new | AvroSerializer( |     |
| --- | --------------------- | ----- | --------------- | --- |
| 36  | this.registry,        |       |                 |     |
| 37  | SerdeType.VALUE,      |       |                 |     |
| 38  | avroSerializerConfig, |       |                 |     |
39 );
40
41 // Initialize deserializer
42 const avroDeserializerConfig: AvroDeserializerConfig = {};
| 43  | this.deserializer       | =   | new AvroDeserializer( |     |
| --- | ----------------------- | --- | --------------------- | --- |
| 44  | this.registry,          |     |                       |     |
| 45  | SerdeType.VALUE,        |     |                       |     |
| 46  | avroDeserializerConfig, |     |                       |     |
47 );
48 }
49
50 /**
51 * Encode message with specific schemaId for producer
| 52 * @param | topic : string - Kafka topic name             |     |     |     |
| ----------- | --------------------------------------------- | --- | --- | --- |
| 53 * @param | payload : any - The message payload to encode |     |     |     |
54 * @param schemaId : number - The schema ID to use for encoding from producer
| 55 * @returns |     | {Promise<Buffer>} |     |     |
| ------------- | --- | ----------------- | --- | --- |
56 * @throws Error if encoding fails
57 * Usage:
58 * const encoded = await schemaService.encodeMessage('my-topic', myPayload, 100126);
59 */
| 60 async | encodeMessage(  |                                 |     |     |
| -------- | --------------- | ------------------------------- | --- | --- |
| 61       | topic: string,  |                                 |     |     |
| 62       | payload: any,   |                                 |     |     |
| 63       | schemaId:       | number,                         |     |     |
| 64       | onSchemaError?: | SchemaValidationErrorCallback,  |     |     |
| 65 ):    | Promise<Buffer> | {                               |     |     |
| 66       | const sid =     | new SchemaId('AVRO', schemaId); |     |     |
| 67       | const subject = | `${topic}-value`;               |     |     |
68
69 // Fetch schema by ID
70 const schemaInfo = await this.registry.getBySubjectAndId(subject, schemaId);
| 71  | if (!schemaInfo?.schema) |        | {   |     |
| --- | ------------------------ | ------ | --- | --- |
| 72  | throw new                | Error( |     |     |
73 `Schema not found for subject ${subject} and ID ${schemaId}`,
| 74  | );  |     |     |     |
| --- | --- | --- | --- | --- |
75 }

76
| 77  | // Parse schema and create Avro type |     |     |     |     |
| --- | ------------------------------------ | --- | --- | --- | --- |
78 const schema: Schema = JSON.parse(schemaInfo.schema) as Schema;
| 79  | const type = | Type.forSchema(schema); |     |     |     |
| --- | ------------ | ----------------------- | --- | --- | --- |
80
| 81  | // validate payload with schema |                                      |     |     |     |
| --- | ------------------------------- | ------------------------------------ | --- | --- | --- |
| 82  | const errors =                  | this.getSchemaErrors(type, payload); |     |     |     |
83
84 // You can log or send email or handle errors suitably with your use case
| 85  | if (errors.length                                       | >                         | 0) { |     |     |
| --- | ------------------------------------------------------- | ------------------------- | ---- | --- | --- |
| 86  | this.logger.error('Schema validation failed:', errors); |                           |      |     |     |
| 87  | if (onSchemaError)                                      |                           | {    |     |     |
| 88  | onSchemaError(errors);                                  |                           |      |     |     |
| 89  | }                                                       |                           |      |     |     |
| 90  | throw new                                               | Error(errors.join('; ')); |      |     |     |
| 91  | }                                                       |                           |      |     |     |
| 92  | try {                                                   |                           |      |     |     |
| 93  | // Validate payload against schema                      |                           |      |     |     |
| 94  | const payloadBuffer = type.toBuffer(payload);           |                           |      |     |     |
95
| 96  | // Serialize with schema ID |     |     |     |     |
| --- | --------------------------- | --- | --- | --- | --- |
97 return this.serializer.serializeSchemaId(topic, payloadBuffer, sid);
| 98  | } catch (error)                                      | {   |     |     |     |
| --- | ---------------------------------------------------- | --- | --- | --- | --- |
| 99  | this.logger.error('Error encoding message:', error); |     |     |     |     |
| 100 | throw error;                                         |     |     |     |     |
| 101 | }                                                    |     |     |     |     |
102 }
103 /**
104 * Decode a message using the normal deserialization process.
| 105 | * @param topic : string - Kafka topic name                |                                |     |     |     |
| --- | --------------------------------------------------------- | ------------------------------ | --- | --- | --- |
| 106 | * @param payload : Buffer - The message payload to decode |                                |     |     |     |
| 107 | * @returns                                                | {Promise<Record<string, any>>} |     |     |     |
108 * @throws Error if decoding fails
109 * Usage:
110 * const decoded = await schemaService.decodeMessageNormal('my-topic',
messageBuffer);
111 */
| 112 async | decodeMessageNormal(       |     |       |     |     |
| --------- | -------------------------- | --- | ----- | --- | --- |
| 113       | topic: string,             |     |       |     |     |
| 114       | payload: Buffer,           |     |       |     |     |
| 115 ):    | Promise<Record<string,     |     | any>> | {   |     |
| 116       | try {                      |     |       |     |     |
| 117       | // Deserialize the message |     |       |     |     |
118 return (await this.deserializer.deserialize(topic, payload)) as Record<
| 119 | string,                                              |     |     |     |     |
| --- | ---------------------------------------------------- | --- | --- | --- | --- |
| 120 | any                                                  |     |     |     |     |
| 121 | >;                                                   |     |     |     |     |
| 122 | } catch (error)                                      | {   |     |     |     |
| 123 | this.logger.error('Error decoding message:', error); |     |     |     |     |
| 124 | throw error;                                         |     |     |     |     |
| 125 | }                                                    |     |     |     |     |
126 }
127
128 /**
129 * Track schema validation errors
| 130 | * @param type : Type - The Avro schema type      |     |     |     |     |
| --- | ------------------------------------------------ | --- | --- | --- | --- |
| 131 | * @param payload : any - The payload to validate |     |     |     |     |
132 * @returns string[] - An array of validation error messages, empty if valid
133 */
| 134 getSchemaErrors(type: |               |          | Type, payload: | any): string[] | {   |
| ------------------------- | ------------- | -------- | -------------- | -------------- | --- |
| 135                       | const errors: | string[] | = [];          |                |     |
136
| 137 | const isValid = type.isValid(payload, |                     |     | {    |     |
| --- | ------------------------------------- | ------------------- | --- | ---- | --- |
| 138 | errorHook:                            | (path, value, type) |     | => { |     |
| 139 | errors.push(                          |                     |     |      |     |
140 `Field error at ${path.join('.') || '(root)'}: expected ${type.toString()},
got ${value}`,

| 141 | );  |     |     |     |
| --- | --- | --- | --- | --- |
| 142 | },  |     |     |     |
| 143 | }); |     |     |     |
144
| 145   | return isValid ? | [] : errors; |     |     |
| ----- | ---------------- | ------------ | --- | --- |
| 146   | }                |              |     |     |
| 147 } |                  |              |     |     |
Producer Development Guide
Change Points: AS-IS → TO-BE (Producer Implementation) for Developer
| Category |     | AS IS |     | TO BE |
| -------- | --- | ----- | --- | ----- |
Application The producer can modify Producer can only be deployed first when the
Evolution the payload structure freely. changes are “Add fields”, “Delete optional fields”. All
Principles There are no constraints other changes must wait until the consumer has
when adding, removing, or been updated to ensure safe schema evolution.
renaming fields.
Schema No integration with Schema Integrated with Confluent Schema Registry using
Manageme Registry; messages are sent versioned Avro schemas. You must register the
nt as plain JSON schema in the schema repository using Avro format
first.
Message Raw JSON messages Messages encoded in Avro binary format with
| Format | without version control |     | schema Id |     |
| ------ | ----------------------- | --- | --------- | --- |
Deploymen Independent from Schema Requires more configuration for
| t   | Registry |     | SCHEMA_REGISTRY_URL, API_KEY, and |     |
| --- | -------- | --- | --------------------------------- | --- |
API_SECRET
| Message | Manual serialization using  |     | Use  |     |
| ------- | --------------------------- | --- | ---- | --- |
Serializatio
|     | 1 JSON.stringify(). |     | 1 AvroSerializer  |     |
| --- | ------------------- | --- | ----------------- | --- |
n
from @confluentinc/schemaregistry.
1 serializer.serialize(topic, payload);
Developme No dependency between Schema updates happen in the schema repo, then
nt app and schema repo. referenced in producer code via : subject + version/
| Workflow |     |     | schema Id/last version. |     |
| -------- | --- | --- | ----------------------- | --- |
Dependenc No dependency on schema Adds lightweight dependency on
| y   | registry client. |     | @confluentinc/schemaregistry. |     |
| --- | ---------------- | --- | ----------------------------- | --- |

Manageme
nt
Example code producer NestJs (Typerscript): (Full Producer Example Code)
With our Kafka Producer connected to Confluent Schema Registry, we can now serialize our
message using the registered Avro schema and send it to the target topic. The Avro serializer
ensures that each message is encoded according to the schema definition, embedding the
corresponding schema Id in the payload.
| 1 import | { Inject, | Injectable, | Logger | } from | '@nestjs/common'; |
| -------- | --------- | ----------- | ------ | ------ | ----------------- |
2 import { ProduceMessageDto } from './dto/producer-message.dto';
| 3 import | { KafkaJS            | } from | '@confluentinc/kafka-javascript'; |                           |     |
| -------- | -------------------- | ------ | --------------------------------- | ------------------------- | --- |
| 4 import | { KafkaClientService |        | } from                            | './kafka-client.service'; |     |
5 import { SchemaConfluentRegistryService } from './schema-client.service';
| 6 import | { ClientKafka | }   | from '@nestjs/microservices'; |     |     |
| -------- | ------------- | --- | ----------------------------- | --- | --- |
7
8 @Injectable()
| 9 export | class ConfluentKafkaProducerService |     |     |     | {   |
| -------- | ----------------------------------- | --- | --- | --- | --- |
10 private readonly logger = new Logger(ConfluentKafkaProducerService.name);
| 11 private             | readonly producer: |        | KafkaJS.Producer; |     |     |
| ---------------------- | ------------------ | ------ | ----------------- | --- | --- |
| 12 private connected = |                    | false; |                   |     |     |
13 constructor(
14 private readonly registry: SchemaConfluentRegistryService,
| 15 private | readonly clientService: |     |     | KafkaClientService, |     |
| ---------- | ----------------------- | --- | --- | ------------------- | --- |
16 @Inject('KAFKA_PRODUCER') private readonly clientKafka: ClientKafka,
17 ) {
| 18 this.producer |     | = this.clientService.getProducer(); |     |     |     |
| ---------------- | --- | ----------------------------------- | --- | --- | --- |
19 }
20
| 21 async | connectProducer()                                |     | {   |     |     |
| -------- | ------------------------------------------------ | --- | --- | --- | --- |
| 22 try   | {                                                |     |     |     |     |
| 23       | this.logger.log('Kafka producer connecting...'); |     |     |     |     |
24
| 25  | await this.producer.connect(); |     |       |     |     |
| --- | ------------------------------ | --- | ----- | --- | --- |
| 26  | this.connected                 | =   | true; |     |     |
27
| 28   | this.logger.log('Kafka producer connected');             |     |        |     |     |
| ---- | -------------------------------------------------------- | --- | ------ | --- | --- |
| 29 } | catch (err)                                              | {   |        |     |     |
| 30   | this.logger.error('Kafka producer connect failed', err); |     |        |     |     |
| 31   | this.connected                                           | =   | false; |     |     |
32 }
33 }
34
35 /**
36 * Emit a notification message to the specified Kafka topic.
37 * @param dto - The data transfer object containing message details.
38 * @returns Promise<void>
39 * @throws Error if message sending fails
40 * Usage:
41 * const dto: ProduceMessageDto = {
42 *   topic: 'your-topic',
43 *   value: { key: 'value' },
44 *   schemaId: 100126,
45 * };
46 * await producerService.emitNotificationWithSchemaId(dto);
47 */
48 async emitNotificationWithSchemaId(dto: ProduceMessageDto) {
| 49 try | {                    |     |     |     |     |
| ------ | -------------------- | --- | --- | --- | --- |
| 50     | if (!this.connected) |     | {   |     |     |

| 51  | this.logger.error(                                      |     |     |     |
| --- | ------------------------------------------------------- | --- | --- | --- |
| 52  | 'Cannot send message: Kafka producer is not connected', |     |     |     |
| 53  | );                                                      |     |     |     |
| 54  | return;                                                 |     |     |     |
| 55  | }                                                       |     |     |     |
56
| 57  | const payload = | { ...dto.value | };  |     |
| --- | --------------- | -------------- | --- | --- |
58
| 59  | const encodedValue = | await this.registry.encodeMessage( |     |     |
| --- | -------------------- | ---------------------------------- | --- | --- |
| 60  | dto.topic,           |                                    |     |     |
| 61  | payload,             |                                    |     |     |
| 62  | dto.schemaId,        |                                    |     |     |
| 63  | );                   |                                    |     |     |
64
| 65  | // This sample for using latest schema id from registry |     |     |     |
| --- | ------------------------------------------------------- | --- | --- | --- |
66 // const encodedValue = await this.registry.encodeWithLatestSchemaId(
| 67  | //   dto.topic, |     |     |     |
| --- | --------------- | --- | --- | --- |
| 68  | //   payload,   |     |     |     |
| 69  | // );           |     |     |     |
70
| 71  | await this.producer.send({ |                            |     |     |
| --- | -------------------------- | -------------------------- | --- | --- |
| 72  | topic: dto.topic,          |                            |     |     |
| 73  | messages:                  | [{ value: encodedValue }], |     |     |
| 74  | });                        |                            |     |     |
75
| 76  | // If using ClientKafka |     |     |     |
| --- | ----------------------- | --- | --- | --- |
77 // this.clientKafka.emit(dto.topic, { value: encodedValue });
| 78 } | catch (error)      | {   |     |     |
| ---- | ------------------ | --- | --- | --- |
| 79   | this.logger.error( |     |     |     |
80 `❌ Failed to send message to topic [${dto.topic}]: ${error}`,
| 81  | );  |     |     |     |
| --- | --- | --- | --- | --- |
82 }
83
84 this.logger.log(`📤 Sent message to topic [${dto.topic}]`);
85 }
86 }
87
Consumer Development Guide
Change Points: AS-IS → TO-BE (Consumer Implementation) for Developer
| Category |     | AS IS |     | TO BE |
| -------- | --- | ----- | --- | ----- |
Application The consumer can Consumers can only be deployed first when the
Evolution deserialize messages freely changes are “Add optional fields” or “Delete fields .”
| Principles | without schema version |     |     |     |
| ---------- | ---------------------- | --- | --- | --- |
All other changes must wait until the producer has
control. There are no
been updated to ensure safe schema evolution.
guarantees for compatibility
when the producer changes
the payload structure.
Schema No integration with Schema Integrated with Confluent Schema Registry using
| Manageme | Registry; messages are |     | versioned Avro schemas |     |
| -------- | ---------------------- | --- | ---------------------- | --- |
| nt       | recived as plain JSON  |     |                        |     |

Message Raw JSON messages Messages decode in Avro binary format with
| Format | without version control |     |     | schema Id from payload |
| ------ | ----------------------- | --- | --- | ---------------------- |
Deploymen Independent from Schema Requires more configuration for
| t   | Registry |     |     | SCHEMA_REGISTRY_URL, API_KEY, and |
| --- | -------- | --- | --- | --------------------------------- |
API_SECRET
| Message | Manual deserialization using |     |     | Use  |
| ------- | ---------------------------- | --- | --- | ---- |
Deserializat
|     | 1 JSON.parse(). |     |     | 1 AvroDeserializer  |
| --- | --------------- | --- | --- | ------------------- |
ion
from @confluentinc/schemaregistry.
1 deserializer.deserialize(topic, message.value),
Developme No dependency between Schema updates happen in the schema repo, then
nt app and schema repo. referenced in producer code via : subject + version/
| Workflow |     |     |     | schema Id/last version. |
| -------- | --- | --- | --- | ----------------------- |
Dependenc No dependency on schema Adds lightweight dependency on
| y   | registry client. |     |     | @confluentinc/schemaregistry. |
| --- | ---------------- | --- | --- | ----------------------------- |
Manageme
nt
Example code consumer with Nestjs ( Typerscript ): (Full Consumer Example Code)
To complete the full producer–consumer flow, we implement a consumer using Confluent
Kafka for JavaScript (CJSK) integrated with the Schema Registry. This consumer is
responsible for connecting to the Kafka cluster, subscribing to topics, and decoding Avro
messages using schema IDs from the registry.
After configuring the consumer, we connect to the Kafka cluster, subscribe to the target
topic, and then continuously process incoming messages using the eachMessage handler.
For each message, the consumer uses an AvroDeserializer to decode the Avro binary
payload back into structured JSON, automatically resolving the schema from the registry by
schemaId.
| 1 import | { KafkaJS            | } from '@confluentinc/kafka-javascript'; |        |                           |
| -------- | -------------------- | ---------------------------------------- | ------ | ------------------------- |
| 2 import | { Injectable,        | Logger                                   | } from | '@nestjs/common';         |
| 3 import | { KafkaClientService |                                          | } from | './kafka-client.service'; |
4 import { SchemaConfluentRegistryService } from './schema-client.service';
| 5 import | { KAFKA_TOPICS  | } from | '@common/constant';           |     |
| -------- | --------------- | ------ | ----------------------------- | --- |
| 6 import | { EventEmitter2 | }      | from '@nestjs/event-emitter'; |     |
7 @Injectable()
| 8 export | class ConfluentConsumeService |     |     | {   |
| -------- | ----------------------------- | --- | --- | --- |

9 private readonly logger = new Logger(ConfluentConsumeService.name);
| 10 private | readonly consumer: |     | KafkaJS.Consumer; |     |     |
| ---------- | ------------------ | --- | ----------------- | --- | --- |
11 constructor(
| 12 private | readonly kafkaClientService: |     |     |     | KafkaClientService, |
| ---------- | ---------------------------- | --- | --- | --- | ------------------- |
13 private readonly confluentRegistry: SchemaConfluentRegistryService,
| 14 private | readonly eventEmitter: |     |     | EventEmitter2, |     |
| ---------- | ---------------------- | --- | --- | -------------- | --- |
15 ) {
| 16 this.consumer |     | = this.kafkaClientService.getConsumer(); |     |     |     |
| ---------------- | --- | ---------------------------------------- | --- | --- | --- |
17 }
18
| 19 async | start()                                          | {   |     |     |     |
| -------- | ------------------------------------------------ | --- | --- | --- | --- |
| 20 try   | {                                                |     |     |     |     |
| 21       | this.logger.log('Kafka consumer connecting...'); |     |     |     |     |
22
| 23  | await this.consumer.connect();               |                              |     |     |     |
| --- | -------------------------------------------- | ---------------------------- | --- | --- | --- |
| 24  | this.logger.log('Kafka consumer connected'); |                              |     |     |     |
| 25  | const topics =                               | Object.values(KAFKA_TOPICS); |     |     |     |
26
| 27  | await Promise.all( |     |     |     |     |
| --- | ------------------ | --- | --- | --- | --- |
28 topics.map((topic) => this.consumer.subscribe({ topic })),
| 29  | );  |     |     |     |     |
| --- | --- | --- | --- | --- | --- |
30
| 31   | await this.listen();                                     |     |     |     |     |
| ---- | -------------------------------------------------------- | --- | --- | --- | --- |
| 32 } | catch (err)                                              | {   |     |     |     |
| 33   | this.logger.error('Kafka consumer connect failed', err); |     |     |     |     |
| 34   | return;                                                  |     |     |     |     |
35 }
36 }
37
| 38 private | async               | listen()                                    | {                    |     |        |
| ---------- | ------------------- | ------------------------------------------- | -------------------- | --- | ------ |
| 39 await   | this.consumer.run({ |                                             |                      |     |        |
| 40         | eachMessage:        | async                                       | ({ topic, message }) |     | => {   |
| 41         | if (!message.value) |                                             | return;              |     |        |
| 42         | try {               |                                             |                      |     |        |
| 43         | const decoded:      |                                             | Record<string,       |     | any> = |
| 44         | await               | this.confluentRegistry.decodeMessageNormal( |                      |     |        |
| 45         | topic,              |                                             |                      |     |        |
| 46         | message.value,      |                                             |                      |     |        |
| 47         | );                  |                                             |                      |     |        |
48 this.eventEmitter.emit(`kafka.${topic}`, { topic, data: decoded });
| 49  | } catch | (err) | {   |     |     |
| --- | ------- | ----- | --- | --- | --- |
50 this.logger.error(`Error decoding message from ${topic}`, err);
| 51  | }                                                    |     |     |     |     |
| --- | ---------------------------------------------------- | --- | --- | --- | --- |
| 52  | },                                                   |     |     |     |     |
| 53  | //     // eachBatch: async (payload) => {            |     |     |     |     |
| 54  | //     //   const { batch } = payload;               |     |     |     |     |
| 55  | //     //   for (const message of batch.messages) {  |     |     |     |     |
| 56  | //     //     if (!payload.isRunning()) break;       |     |     |     |     |
| 57  | //     //     if (!message.value) continue;          |     |     |     |     |
| 58  | //     //     try {                                  |     |     |     |     |
| 59  | //     //       const decoded: Record<string, any> = |     |     |     |     |
60 //     //         await this.confluentRegistry.decodeMessageNormal(
| 61  | //     //           batch.topic,   |     |     |     |     |
| --- | ---------------------------------- | --- | --- | --- | --- |
| 62  | //     //           message.value, |     |     |     |     |
| 63  | //     //         );               |     |     |     |     |
64 //     //       this.eventEmitter.emit(`kafka.${batch.topic}`, {
| 65  | //     //         topic: batch.topic,                  |     |     |     |     |
| --- | ------------------------------------------------------ | --- | --- | --- | --- |
| 66  | //     //         data: decoded,                       |     |     |     |     |
| 67  | //     //       });                                    |     |     |     |     |
| 68  | //     //       payload.resolveOffset(message.offset); |     |     |     |     |
| 69  | //     //       await payload.heartbeat();             |     |     |     |     |
| 70  | //     //     } catch (err) {                          |     |     |     |     |
| 71  | //     //       this.logger.error(                     |     |     |     |     |
72 //     //         `Error decoding message from ${batch.topic}`,
| 73  | //     //         err, |     |     |     |     |
| --- | ---------------------- | --- | --- | --- | --- |
| 74  | //     //       );     |     |     |     |     |
| 75  | //     //     }        |     |     |     |     |

76 // // }
77 // // },
78 // });
79 });
80 }
81 }
82
Kafka Runner
This is runner to connect kafka during the application bootstrap phase.
1 import { Injectable } from '@nestjs/common';
2 import { ConfluentConsumeService } from './kafka-confluent-consumer.service';
3 import { ConfluentKafkaProducerService } from './kafka-confluent-producer.service';
4 /**
5 * Kafka connections are initialized during the application bootstrap phase..
6 * Usage:
7 * main.ts main.ts
8 * async function bootstrap() {
9 * ------------------
10 * const kafkaRunner = app.get(KafkaRunner);
11 * await kafkaRunner.start();
12 *
13 * await app.listen(process.env.PORT ?? 3000);
14 * }
15 * bootstrap();
16 */
17 @Injectable()
18 export class KafkaRunner {
19 constructor(
20 private readonly confluentConsumeService: ConfluentConsumeService,
21 private readonly confluentProducerService: ConfluentKafkaProducerService,
22 ) {}
23
24 async start() {
25 await this.confluentProducerService.connectProducer();
26 await this.confluentConsumeService.start();
27 }
28 }
29
Orkes Schema Registry Setup Guide
Integrations
With integrations we will using Apache Kafka. Apache Kafka Integration with Orkes Conduct
or | Orkes Conductor Documentation

We must be select some properties about : Sending Protocol (ARVO) , Connection Security
(SASL_SSL),Schema Registry Auth Type (Schema Registry User Info (Key/Password)) , Value
Subject Name Strategy ( io.confluent.kafka.serializers.subject.TopicNameStrategy)
Consumer in Orkes
We using Event Handler to consume in Orkes . Using Event Handlers | Orkes Conductor Docu
mentation
This is example for Event Handle :

Producer in Orkes
We using Event Task to produce in Orkes. When we produce in Orkes we must be add field
_schema into Input parameters with following : topic-value (This is subject)
Event | Orkes Conductor Documentation
This is example for Event Task :

Schema Evolution and Maintenance Guidelines
Evolution Strategy
During normal operation, the Compatibility mode should be set to FULL .
The FULL setting technically blocks any changes that would add or delete a required
field. It is important to keep this on FULL during normal times to prevent human errors
from the producer side, such as making unannounced changes. Schema updates must
only proceed when all relevant stakeholders are fully aware of the changes.

Explain Evolution Strategy
Schema Evolution and Compatibility for Schema Registry on Confluent Platform | Confluent D
ocumentation
In Confluent Schema Registry, an evolution strategy defines how schemas can change over
time without breaking compatibility between producers and consumers of Kafka messages.
Since producers write Avro (or JSON/Protobuf) messages encoded according to a specific
schema, and consumers read them using another schema version, the Schema Registry
enforces compatibility rules to ensure data exchange continues to work smoothly after schema
changes.
The following table presents a summary of the types of schema changes allowed for the
different compatibility types, for a given subject. The Confluent Schema Registry default
compatibility type is BACKWARD .
Compatibility Type Changes allowed Check against which Upgrade first
schemas
BACKWARD Delete fields Last version Consumers
Add optional fields
BACKWARD_TRANSIT Delete fields All previous versions Consumers
IVE Add optional fields
FORWARD Add fields Last version Producers
Delete optional
fields
FORWARD_TRANSITI Add fields All previous versions Producers
VE Delete optional
fields
FULL Add optional fields Last version Any order
Delete optional
fields
FULL_TRANSITIVE Add optional fields All previous versions Any order
Delete optional
fields

| NONE | All changes are | Compatibility     |     | Depends |
| ---- | --------------- | ----------------- | --- | ------- |
|      | accepted        | checking disabled |     |         |
Schema and Dataset Violation
This mechanism technically enforces that your payload always matches the schema. Here after
is about outcomes from each scenario.
| Violation Case | Side | Desc |     | What happened? |
| -------------- | ---- | ---- | --- | -------------- |
Omit Required Producer When Producer produce Error: invalid "string": undefined
field  without required field
Omit Optional Producer When Producer produce Succeed Producer message. But
| field | without optional field |     | the omission replaced with |     |
| ----- | ---------------------- | --- | -------------------------- | --- |
default value.
Over Required Producer When Producer produce with Succeed Producer message. But
| field | more required field |     | without the new field |     |
| ----- | ------------------- | --- | --------------------- | --- |
Over Optional Producer When Producer produce with Succeed Producer message. But
| field | more optional field |     | without the new field |     |
| ----- | ------------------- | --- | --------------------- | --- |
Schema Evolution flow
The flow below describes the scenario where a Schema Evolution occurs on a schema that is
already in use.
| Type 1. Producer and Consumer              | application    |          |     |     |
| ------------------------------------------ | -------------- | -------- | --- | --- |
| Case) If you need to                       | Delete  field  |          |     |     |
| 1. The compatible strategy should choose:  |                | BACKWARD |     |     |

2. Update new revision of Schema to the om-schema-registry repository.
3. Consumer-side dataset upgrade. Consumer-side kafka client still listens for v1's schema-
formatted data ( producer still publish data based on v1 schema ).
→ But after deserialization, consumer-side dataset ignores the field which will be deleted in
the next step.
4. Producer-side dataset and schema file upgrade(v1 → v2). Then the producer sends a new
dataset with a new schema version(v2).
→ Consumer is matching format between schema and dataset.
Case) If you need to Add field
1. The compatible strategy should choose: FORWARD
2. Update new revision of Schema to the om-schema-registry repository.
3. Producer-side dataset and schema file upgrade (v1 → v2). Then the producer sends a new
dataset with a new schema version(v2). Consumer-side Kafka client listens to v2ʼs schema-
formatted data.
→ After deserialization, consumer-side dataset ignores the new field.

4. Consumer dataset upgrade
→ Consumer is matching format between schema and dataset
Case) If you need to Add , Delete Together
1. compatible strategy: None
2. Update new revision of Schema to the om-schema-registry repository.
3. Not available gracefully transition (Compatibility checking not supported)
Type 2. Producer or Consumer is Orkes
Orkes is only support to produce with last updated schema version. Please be careful when
uploading schemas to om-schema-registry . When a new schema is uploaded, it takes
effect immediately.
Since Orkes can only reference the latest schema version, it is correct to consider its
Compatibility Mode to be None . Not available gracefully transition.
Schema Rollback
A rollback is an incident state, not a schema evolution, and requires a quick and integrity-
preserving change. Therefore, it is best to simultaneously revert both the application and the
schema along with the necessary application downtime.
Case 1. For application
If you accidentally deploy a new revision of schema, but it has an issue, then we have to
promptly rollback. For the prompt rollback, please follow the guideline below.
Producer can revert the schema ID as last one. The following published message will
1.
include a pair with the previous schema ID and matched payload. → Consumer can listen
and deserialize the message.

Remove the latest schema which issue happened in the respective Confluent subject.
2.
Revert avro , asyncAPI file in the repository om-schema-registry as well. And confirm
3.
if the information on the Confluent side matches the information in the repository.
Case 2. For Orkes
Remove the latest schema which issue happened in the respective Confluent subject. →
1.
Orkes fetches the lastest schema which is registered in the respective subject immediately.
Without any separate action required on the Orkes side, the next message will be published
based on the newly updated, latest version of the schema.
Revert avro , asyncAPI file in the repository om-schema-registry as well. And confirm
2.
if the information on the Confluent side matches the information in the repository.

Version Management
Same as  protobuf , we also use a specific repository  om-schema-registry  to manage all
schemas of Chorus. We must preserve a certain number of past schema versions to allow for
backward transitions or rollbacks.
R&R
| Task    |                             | Description | Team       | PIC |
| ------- | --------------------------- | ----------- | ---------- | --- |
| General | Create/Delete Topic/Subject |             | Infra team |     |
Confluent
Control the broker/Partition setting
Platform
Confluent account and permission
Administration
management
| Topic -     | Possible to modify detailed   |     | Infra team |     |
| ----------- | ----------------------------- | --- | ---------- | --- |
| Maintenance | configurations at the topic.  |     |            |     |
Publish API Key Publishing API Key to both consumer Infra team
and producer side.
| Topic -   | Authority and responsibility for      |     | Producer | TA  |
| --------- | ------------------------------------- | --- | -------- | --- |
| Ownership | detailed configurations required for  |     | Scrum    |     |
|           | the stable operation of the topic. It |     | team     |     |
means that rather than modifying the
configuration by yourself, you should
be able to calculate the optimal
configurations and provide guidance
on the appropriate settings when
issues arise.
Obligation to share information
regarding topics and schemas with
other relevant teams.
Obligation to provide continuous
updates on the usage status.
| Subject -   | Possible to modify detailed      |     | Producer | TA  |
| ----------- | -------------------------------- | --- | -------- | --- |
| Maintenance | configurations at the subject of |     | Scrum    |     |
|             | Schema Registry.                 |     | team     |     |

- Update Compatibility mode
- Create/Update/Delete Schema
Topic - View Read-Only permission for detailed Producer TA and Dev
information on the topic including Scrum
configurations and metric. team
References
Schema Validation
Schema validation at the application
On the application side, schema validation is performed during the serialization of the input
dataset. The library for the serialization checks if the format of the dataset matches the
fetched schema. This process follows the rules : https://oneline.atlassian.net/wiki/spaces/B
TH/pages/3259203792 Request access
Schema validation at the broker Broker-Side Schema ID Validation on Confluent Cloud |
Confluent Documentation
Schema Validation does not perform data introspection, but rather checks that the schema ID
in the Wire Format is registered in Schema Registry under a valid subject. This option validate
if schema is existing at the corresponding topic.
