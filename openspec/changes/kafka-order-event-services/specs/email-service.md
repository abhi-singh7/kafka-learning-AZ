# Spec: email-service (`:8083`) — Kafka in, mock mail out

## ADDED Requirements

### Requirement: Consume line events independently
The `email-service` SHALL consume `order-created` with its own consumer classes and SHALL NOT import code from,
or share state with, `order-service` or `order-persistence-service`.

#### Scenario: same stream, different handler
- **WHEN** a line event is consumed by `email-service` at the same time as by `order-persistence-service`
- **THEN** the email handling MUST NOT depend on the persistence service's state or availability

### Requirement: Mock mail rendering (no SMTP)
For each consumed line event the email-service SHALL render a mock mail — a `subject`, a `body` naming
`customerId`, `sku`, `qty`, `price`, and `orderId` — record it in a bounded in-memory store (default capacity
1000) and log one line per mail. It SHALL NOT contact SMTP and SHALL NOT write to Postgres.

#### Scenario: mail for one order
- **WHEN** two line events of order `X` are consumed
- **THEN** `GET /orders/X/emails` MUST return the rendered mail(s) for `X`
- **AND THEN** `GET /emails` MUST include them
- **AND IF** more than 1000 mails are rendered
- **THEN** the store MUST keep the most recent 1000 and log the number evicted

#### Scenario: no events yet
- **WHEN** no line event has been consumed since start
- **THEN** `GET /emails` MUST return `200 []`
- **AND WHEN** a client asks for an unknown `orderId`
- **THEN** `GET /orders/{orderId}/emails` MUST return `404`

### Requirement: Ack event publication
After rendering a mock mail the email-service SHALL publish an ack record to `order-email-sent` with KEY =
`orderId` and fields `schemaVersion`, `orderId`, `messageId`, `sku`, `qty`, `result = "email-sent"`, `at`,
`service = "email-service"`, and SHALL ACK the consumed message.

#### Scenario: ack written
- **WHEN** one line event is handled successfully
- **THEN** `order-email-sent` MUST contain exactly one record with that `messageId`
- **AND THEN** the consumer's offset MUST advance (message not redelivered)

### Requirement: NACK on handling failure
If mail rendering throws (an infrastructure/bug failure, not a bad payload), the email-service SHALL NACK so
Kafka redelivers the message.

#### Scenario: injected rendering failure
- **WHEN** rendering throws for a valid line event
- **THEN** no ack record MUST be written for it at that attempt
- **AND THEN** the same message MUST be redelivered on a subsequent poll
- **AND WHEN** rendering later succeeds
- **THEN** exactly one `order-email-sent` record MUST exist for that `messageId`

### Requirement: Poison path
On an undecodable or foreign-schema message the email-service SHALL publish a poison record to `order-failed`
(`detectedBy = "email-service"`) and SHALL ACK the message.

#### Scenario: corrupt message
- **WHEN** a corrupt payload is on `order-created`
- **THEN** `order-failed` MUST gain one poison record from `email-service`
- **AND THEN** `GET /orders/failed` MUST expose it
- **AND THEN** the corrupt message MUST NOT be redelivered in a loop

### Requirement: Lifecycle
The email-service SHALL fail fast when `order-email-sent` or `order-created` is missing, unsubscribe its beans
and destroy its `KafkaTemplate` on shutdown, and log how many buffered mails were pending.

#### Scenario: missing ack topic
- **WHEN** `order-email-sent` does not exist at startup
- **THEN** the service MUST refuse to start and log the missing topic name plus the create command
