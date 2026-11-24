# email-service — Kafka in, mock mail out (:8083)

**Package root:** `com.example.orders.email` (Spring Boot 4.1.2, Spring Kafka 4.1.2)
**Module:** `email-service/`
**Consumer group:** `email-service`

This is consumer #2 of `order-created` — fully independent of the persistence service. It decodes each
line event, renders a **mock mail** (no SMTP, no database), stores it in a bounded in-memory deque,
publishes the `order-email-sent` ack, then ACKs the source message.

## Functionality

1. Consume `order-created` (MANUAL_IMMEDIATE ack, `auto.offset.reset=earliest`,
   `enable.auto.commit=false`).
2. Decode payload with an in-process Jackson 3 decoder (`LineEventDecoder`) — rejects (never throws)
   on blank/undecodable JSON or foreign `schemaVersion`.
3. If rejected → publish poison record to `order-failed` → ACK → continue.
4. Otherwise → `MockMailSink.record(event)`:
   - Renders `subject = "Order <orderId> — item <sku> (qty <qty>)"` and a plain-text body naming
     customer, SKU, qty, price, line index and message id.
   - Appends to a bounded deque (default capacity 1000); over capacity, oldest mails are evicted and
     the eviction counter increments. One log line per mail.
5. On success → publish `order-email-sent` ack (key = orderId) → ACK.
6. On failure (render/ack throws) → rethrow → NACK → redelivery on next poll; **no ack record** for that attempt.

## REST API

| Method & path | Success | Failure | Body / notes |
|---|---|---|---|
| `GET /emails` | `200` | — | `[{"orderId","subject","body","sentAt"}]` oldest first; `[]` when none |
| `GET /orders/{orderId}/emails` | `200` | `404` (unknown order) | mails for that order, oldest first |
| `GET /orders/failed` | `200` | — | poison records (`detectedBy`, `sourceTopic`, `reason`, truncated `rawPayload`, `receivedAt`) |
| `GET /health` | `200` | — | `{"status":"UP","mailsStored":N,"evicted":M}` |

## Mock mail store (`MockMailSink`)

- Bounded `ConcurrentLinkedDeque<MockMail>`, default capacity **1000** (constructor-injectable).
- Oldest-first eviction; evictions counted in an `AtomicLong` exposed via `/health` and the shutdown log.
- Rendering logs one line: `mock mail rendered for order <orderId> sku <sku> (store size N, evicted M)`.

## Kafka production (ack + poison)

| Concern | Target | Key | Serializer |
|---|---|---|---|
| Ack | `order-email-sent` | `orderId` | Jackson 3 `JacksonJsonSerializer<EmailSentAck>` |
| Poison | `order-failed` | none | `StringSerializer` (JSON string) |

Ack record: `{schemaVersion:"1", orderId, messageId, sku, qty, result:"email-sent", at, service:"email-service"}`.

## Lifecycle

- **Fail-fast**: `EmailLifecycle` probes `order-created` + `order-email-sent` at `ApplicationReady`
  (`TopicStartupProbe`); a missing topic logs the create command and rethrows → refuses to serve.
- **Shutdown**: flush + destroy `KafkaTemplate`, log mails pending + evicted count.

## Configuration

| Key | Default | Purpose |
|---|---|---|
| `server.port` | `8083` | HTTP |
| `spring.kafka.bootstrap-servers` | `${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}` | Broker |

## Components

| Class | Role |
|---|---|
| `Main` | Entry point |
| `EmailKafkaConfig` | Producer factories (ack + poison), consumer factory (manual ack), `AckPublisher` / `PoisonPublisher` beans |
| `EmailServiceConfig` | `MockMailSink` bean |
| `LineEventConsumer` | The listener: decode → poison or render → ack → ACK / NACK |
| `LineEventDecoder` | Jackson 3 decode + schemaVersion=1 + critical-field validation (never throws) |
| `MockMailSink` | Bounded in-memory store of rendered mails; eviction counting |
| `MailController` | Query API + health |
| `EmailLifecycle` | Fail-fast probe + graceful shutdown |
| `PoisonStore` / `PoisonPublisher` | In-memory poison store + `order-failed` writer |
| `TopicStartupProbe` | Fail-fast admin check with exact create command |
| `OrderLineEventRecord` / `LineEventDecodeResult` | Own private copy of the line-event schema |

## Tests (`src/test/java/com/example/orders/email/`)

- `BaselineTest`, `LineEventDecoderTest`, `MockMailSinkTest`, `MailControllerTest` — units
- `EmailConsumerKafkaTest`, `EmbeddedBrokerRoundTripTest` — in-process broker round-trip
- `EmailPoisonPathTest`, `FailedOrdersEndpointTest`, `FailFastTopicCheckTest` — poison, endpoints, fail-fast
- `KafkaTestHarness` — shared embedded-broker harness

Run: `cd email-service && mvn clean test`
