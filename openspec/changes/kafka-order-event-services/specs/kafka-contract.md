# Spec: Kafka event contract (topics, keys, codecs, error flow)

## ADDED Requirements

### Requirement: Topic set
The system SHALL use exactly four Kafka topics on the broker at `localhost:9092`, all with 1 partition and
replication factor 1, and SHALL NOT publish to or modify the pre-existing topic `orders`.

Topics: `order-created` (produced by order-service), `order-email-sent` and `order-persisted` (per-consumer ack
events), `order-failed` (poison sink).

#### Scenario: topology ready before services start
- **WHEN** the four topics have been created with the documented command
- **THEN** each service starts and reports the topics it reads/writes
- **AND IF** a topic a service requires is missing
- **THEN** that service MUST refuse to start and MUST log the missing topic name and the create command

#### Scenario: pre-existing topic untouched
- **WHEN** any of the three services runs, alone or together
- **THEN** the topic `orders` MUST receive no message from this project

### Requirement: Line-event payload (schemaVersion 1)
Every message published to `order-created` SHALL be JSON encoded by Jackson 3 with exactly these fields:
`schemaVersion`, `orderId`, `customerId`, `sku`, `qty`, `price`, `lineIndex`, `messageId`, `createdAt`.
The message KEY SHALL be the `orderId`.

#### Scenario: well-formed line event
- **WHEN** a consumer receives a message on `order-created`
- **THEN** it decodes to a line event with all nine fields present and `schemaVersion = "1"`
- **AND IF** the key is compared to the payload's `orderId`
- **THEN** they MUST be equal, and all lines of one order MUST land on the same partition

#### Scenario: foreign schema version
- **WHEN** a message decodes but `schemaVersion` is not `"1"`
- **THEN** it MUST be treated as a poison message (see Poison handling requirement) and MUST NOT be persisted or emailed

### Requirement: Ack events
Each consumer service SHALL publish one ack event to its own ack topic after it has handled a line event, with
KEY = `orderId` and fields `schemaVersion`, `orderId`, `messageId`, `sku`, `qty`, `result`, `at`, `service`.

#### Scenario: two independent handlers of one stream
- **WHEN** one line event is consumed by both `order-persistence-service` and `email-service`
- **THEN** `order-persisted` MUST gain exactly one record and `order-email-sent` MUST gain exactly one record, both with `messageId` equal to the source message's `messageId`
- **AND IF** one consumer fails
- **THEN** the other consumer's ack topic MUST still be written (no coupling)

### Requirement: Poison handling (fail-over sink)
When a consumer cannot decode or validate a message it SHALL publish a poison record to `order-failed` with
fields `schemaVersion`, `reason` (`json-decode` | `schema-mismatch`), `rawPayload`, `sourceTopic`,
`receivedAt`, `detectedBy`, and SHALL then ACK the source message.

#### Scenario: corrupt payload on order-created
- **WHEN** a non-decodable payload is published to `order-created`
- **THEN** `order-failed` MUST gain one record per consumer, each carrying the original payload text as `rawPayload` and its own `detectedBy`
- **AND THEN** both consumers MUST ACK so the record is not redelivered in a loop
- **AND IF** `rawPayload` exceeds 1000 characters
- **THEN** the record and the HTTP view MUST truncate it to 1000 characters and flag truncation

#### Scenario: poison observed over HTTP
- **WHEN** a consumer has written at least one poison record
- **THEN** `GET /orders/failed` on that consumer MUST return the poison records with `detectedBy` and truncated `rawPayload`

### Requirement: ACK and NACK semantics
A consumer SHALL NACK only for infrastructure failures (e.g. Postgres JDBC/IO error). Decodable-but-invalid
payloads SHALL go to `order-failed` and be ACKed.

#### Scenario: database write fails
- **WHEN** a line event is valid but the JDBC write throws
- **THEN** the consumer MUST NACK, and on the next poll the same message MUST be redelivered
- **AND WHEN** the database becomes reachable again
- **THEN** exactly one row for that `messageId` MUST exist in `order_line_events`

### Requirement: Replay tolerance (at-least-once)
The persistence consumer SHALL be idempotent: re-consuming a message with an already-persisted `messageId`
SHALL create no additional row.

#### Scenario: replay after restart
- **WHEN** the same `messageId` is consumed twice
- **THEN** `order_line_events` MUST contain exactly one row for it
