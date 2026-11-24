# Spec: order-persistence-service (`:8082`) — Kafka in, Postgres out

## ADDED Requirements

### Requirement: Independent consumer
The `order-persistence-service` SHALL consume `order-created` with its own message classes and SHALL NOT import
code from the other services.

#### Scenario: independent of the email service
- **WHEN** `email-service` is stopped
- **THEN** persistence MUST still consume `order-created` and write rows

### Requirement: Persist one row per line event
Each successfully handled line event SHALL create exactly one row in table `order_line_events` of DB `ordersdb`
(credentials from config, default user `postgres`), with columns `id, orderId, customerId, sku, qty, price, lineIndex, messageId, createdAt`.

#### Scenario: order of three items
- **WHEN** three line events of one order are consumed
- **THEN** `order_line_events` MUST contain three rows sharing that `orderId`
- **AND THEN** `GET /orders/{orderId}/lines` MUST return those three rows

### Requirement: Idempotent persistence (replay tolerance)
`messageId` SHALL be unique and `(orderId, sku)` SHALL be unique; re-consuming a message SHALL create no extra row.

#### Scenario: replay after restart
- **WHEN** a message with an already-persisted `messageId` is consumed again
- **THEN** the row count MUST NOT increase
- **AND THEN** the ack topic MUST still be writable for that handling attempt

### Requirement: NACK on infrastructure failure
If the JDBC write throws, the service SHALL NACK so the message is redelivered, and SHALL NOT write a partial row.

#### Scenario: database write fails
- **WHEN** the JDBC insert throws
- **THEN** no row MUST be written for that attempt and the message MUST be redelivered
- **AND WHEN** the DB becomes reachable and handling succeeds
- **THEN** exactly one row MUST exist for that `messageId`

### Requirement: Ack event publication
After a successful insert the service SHALL publish one ack record to `order-persisted` (KEY = `orderId`,
`result = "persisted"`, `service = "order-persistence-service"`) and ACK the source message.

#### Scenario: ack after persist
- **WHEN** a line event is persisted
- **THEN** `order-persisted` MUST contain exactly one record with that `messageId`

### Requirement: Poison path
Undecodable or foreign-schema messages SHALL go to `order-failed` with `detectedBy = "order-persistence-service"`
and SHALL be ACKed.

#### Scenario: corrupt message
- **WHEN** a corrupt payload is consumed
- **THEN** no row MUST be written
- **AND THEN** `order-failed` MUST gain one poison record
- **AND THEN** `GET /orders/failed` MUST return it with `rawPayload` truncated to 1000 characters

### Requirement: Query surface
`GET /orders/{orderId}/lines` SHALL return persisted rows (`404` when the order has none) and `GET /health`
SHALL report broker and DB reachability.

#### Scenario: order not persisted yet
- **WHEN** the order's rows are not yet written
- **THEN** the endpoint MUST return `404`
- **AND IF** the collection route is used
- **THEN** it MUST return `200 []` rather than an error

### Requirement: Sibling data untouched
The service SHALL NOT create, modify, or delete the pre-existing table `order_lines` in `ordersdb`.

#### Scenario: regression check
- **WHEN** the persistence service starts and runs against `ordersdb`
- **THEN** `\dt` MUST show both `order_lines` and `order_line_events`
- **AND THEN** `order_lines` row count MUST be unchanged from before the run
