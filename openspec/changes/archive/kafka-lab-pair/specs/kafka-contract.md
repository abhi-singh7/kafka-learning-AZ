# Spec: Kafka event contract for the `lab-*` lanes (topics, keys, headers, codecs, error flow)

## ADDED Requirements

### Requirement: Topic namespace
The pair SHALL use only topics named with the `lab-` prefix on the existing broker at `localhost:9092`:
`lab-orders` (3 partitions), `lab-orders-avro` (1), `lab-ack-a` (1), `lab-ack-b` (1), `lab-pending` (1),
`lab-accepted` (1), `lab-rejected` (1), `lab-retry` (1), `lab-rebalance` (1) — replication factor 1 (KRaft
single node). The pair SHALL NOT publish to, subscribe as, or alter the consumer-group offsets of the
pre-existing topics `order-created`, `order-email-sent`, `order-persisted`, `order-failed`, `orders`,
`order-abc`, `order-jdbc-probe`.

#### Scenario: topology ready before either app starts
- **WHEN** the `lab-*` topics exist with the documented partition counts
- **THEN** each app logs the topics it reads and writes at startup
- **AND IF** a topic required by an app is missing
- **THEN** that app MUST refuse to start and MUST log the missing topic name plus the create command

#### Scenario: sibling topics untouched
- **WHEN** either app runs alone or alongside the sibling project
- **THEN** no non-`lab-*` topic MAY receive a message from this pair
- **AND THEN** the sibling consumer groups' partition-offsets MUST NOT be affected

### Requirement: Consumer group namespace
The pair SHALL use only the consumer groups `lab-consumer`, `lab-competitor`, and `lab-txn`.

#### Scenario: offsets are per group
- **WHEN** `GET /lab/diagnostics` is read while the sibling project is also consuming
- **THEN** each group's offsets MUST be reported separately, per partition
- **AND IF** this pair reused a group name from the sibling project
- **THEN** that MUST be flagged a design violation (foreign offsets would be inherited silently)

### Requirement: `lab-orders` line/order event (String codec, schemaVersion "1")
Every message on `lab-orders` SHALL be JSON text with exactly
`schemaVersion`, `orderId`, `customerId`, `sku`, `qty`, `price`, `messageId`, `traceId`, `deliveredAt`,
with **message key = `customerId`** and message headers `x-lab-trace` (the `traceId`),
`x-lab-correlation` (the `orderId`), `x-lab-schema` (`orders/1`), and the optional decision header
`x-lab-skip` (`true` = the filter lane must skip it).

#### Scenario: keyed lane is deterministic
- **WHEN** three messages with `customerId = cust-0` are pushed to `lab-orders`
- **THEN** all three MUST land on the same partition and their offsets MUST increase in push order
- **AND WHEN** the same payloads go through the unkeyed lane
- **THEN** their partitions MAY differ, and the consumer MUST log that per-customer ordering is not guaranteed there

#### Scenario: header carries decisions without reading the payload
- **WHEN** the filter must decide whether to process a message
- **THEN** the decision MUST be taken from headers/key before the payload is deserialized

### Requirement: Routing before deserialization
A skip decision SHALL be taken from the message header `x-lab-skip` before the payload is deserialized.

#### Scenario: skip wins over corruption
- **WHEN** a message carries `x-lab-skip: true` and its payload is not decodable
- **THEN** it MUST be filtered (acked, not quarantined) and no decode error MUST be raised
- **AND IF** the decision required reading the payload
- **THEN** that MUST be treated as a violation of the routing-before-deserialize rule

### Requirement: Foreign schema version
A message that decodes but whose `schemaVersion` is not `"1"` SHALL be quarantined with reason
`schema-mismatch`.

#### Scenario: foreign version quarantined once
- **WHEN** a v2 order event is pushed to `lab-orders`
- **THEN** `lab-rejected` MUST gain one record with `reason = schema-mismatch`
- **AND THEN** the source message MUST be ACKed so it is not redelivered in a loop

### Requirement: AVRO lane with schema registry and evolution
The `lab-orders-avro` lane SHALL serialize with an AVRO schema named `OrderEvent` resolved through the
schema registry, version 1 fields `orderId, customerId, sku, qty, price, messageId, traceId, createdAt`,
and a version 2 schema adding `discount` (default `0.0`) and `phoneNumber` (default `""`).

#### Scenario: registry holds both versions
- **WHEN** v2 messages are pushed after v1 messages exist on the topic
- **THEN** the registry MUST hold two versions of `OrderEvent`
- **AND THEN** the broker-owned `_schemas` topic MUST contain both
- **AND THEN** `GET /lab/diagnostics` MUST report both versions with their topic bindings

#### Scenario: old data still readable
- **WHEN** a partition holds both v1 and v2 messages and the reader is configured for the newest schema
- **THEN** the reader MUST decode v1 messages by applying the v2 defaults
- **AND IF** the registry is unreachable at startup
- **THEN** the AVRO lane MUST refuse to start, naming the registry URL and the command to re-point it (fail-fast, no silent downgrade)

### Requirement: Transaction pair lane
`lab-ack-a` and `lab-ack-b` SHALL advance only within a Kafka transaction whose id derives from `orderId`,
with a producer-side transaction manager and a consumer-side transaction manager chained as `@Primary`.

#### Scenario: all-or-nothing across two topics
- **WHEN** one transactional order is produced and consumed
- **THEN** `lab-ack-a` and `lab-ack-b` MUST each gain exactly one record keyed by `orderId`
- **AND WHEN** a side effect is forced to fail on either side
- **THEN** neither topic's offset MAY have advanced and the source offset MUST be unchanged

### Requirement: Quarantine sink `lab-rejected`
An unhandled message SHALL produce one quarantine record with KEY = the source key (or `corrupt` if no key
is available) and fields `schemaVersion`, `reason` ∈ (`json-decode`, `schema-mismatch`, `handler-throw`,
`filter`), `rawPayload` (truncated to 1000 chars with `truncated` flagged), `sourceTopic`,
`sourcePartition`, `sourceOffset`, `detectedBy`, `attempts`, `firstFailureAt`, `lastFailureAt`, and the
source message SHALL then be ACKed.

#### Scenario: coordinates make it re-drivable
- **WHEN** a non-decodable payload is published to `lab-orders`
- **THEN** `lab-rejected` MUST gain one record whose `rawPayload` equals the original text
- **AND THEN** `sourcePartition`/`sourceOffset` MUST identify where it came from, so a human can re-drive it
- **AND THEN** `GET /lab/rejected` MUST return that record

#### Scenario: quarantine is not a filter
- **WHEN** a message is rejected because of a header decision
- **THEN** it MUST NOT appear in `lab-rejected` (deliberate skip)
- **AND WHEN** a message fails during processing
- **THEN** it MUST appear in `lab-rejected` after the retry budget is exhausted

### Requirement: Retry budget before quarantine
A processing failure SHALL be retried at most `lab.retry.max-attempts` (default 3) times with exponential
backoff (base 100 ms, factor 2) before quarantine.

#### Scenario: injected failures inside the budget
- **WHEN** 30 % of processing attempts throw and the budget is 3
- **THEN** each affected message MUST eventually be handled or quarantined
- **AND THEN** per-message retry counts MUST be logged and MUST NOT exceed the budget
- **AND IF** an exception escapes the error handler
- **THEN** the listener MUST NOT stop and an offset MUST NOT silently advance past an unacked message
