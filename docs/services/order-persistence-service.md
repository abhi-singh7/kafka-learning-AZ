# order-persistence-service — Kafka in, Postgres out (:8082)

**Package root:** `com.example.orders.persistence` (Spring Boot 4.1.2, Spring Kafka 4.1.2)
**Module:** `order-persistence-service/`
**Consumer group:** `order-persistence-service`

This is consumer #1 of `order-created`. It decodes each line event, persists one row per line into
Postgres table `order_line_events`, publishes the `order-persisted` ack, then ACKs the source message.
It is **idempotent** (replaying a `messageId` never duplicates a row) and **NACKs** on infrastructure
failure so Kafka redelivers.

## Functionality

1. Consume `order-created` (MANUAL_IMMEDIATE ack, `auto.offset.reset=earliest`,
   `enable.auto.commit=false`).
2. Decode payload with an in-process Jackson 3 decoder (`LineEventDecoder`) — rejects (never throws)
   on blank/undecodable JSON or foreign `schemaVersion`.
3. If rejected → publish poison record to `order-failed` → ACK → continue.
4. Otherwise → `PersistenceService.persist(event)`:
   - `existsByMessageId` → `ALREADY_PERSISTED` (idempotent skip, no INSERT)
   - else build `OrderLineEvent` entity (BigDecimal price) → `saveAndFlush`
   - `DataIntegrityViolationException` → `ALREADY_PERSISTED` (concurrent redelivery won the race —
     still a skip, not a failure)
   - any other JDBC error → propagates
5. On success → publish `order-persisted` ack (key = orderId) → ACK.
6. On failure → rethrow → NACK → redelivery on next poll; no partial row (transaction rolls back).

## REST API

| Method & path | Success | Failure | Body / notes |
|---|---|---|---|
| `GET /orders/{orderId}/lines` | `200` | `404` (order unknown/none) | `[OrderLineEvent…]` rows |
| `GET /orders/lines` | `200` | — | all rows (collection route); `[]` when none |
| `GET /orders/failed` | `200` | — | poison records (`detectedBy`, `sourceTopic`, `reason`, truncated `rawPayload`, `receivedAt`) |
| `GET /health` | `200` | — | `{"status":"UP"|"DEGRADED","broker":"reachable"|"unreachable","database":"reachable"|"unreachable","rows":N}` (`rows=-1` when DB down) |

## Persistence model

Table `order_line_events` in DB `ordersdb` (native Postgres 15.19, `127.0.0.1:5432`):

| Column | Type | Constraint |
|---|---|---|
| `id` | bigint identity | generated (`insertable=false`); NOT the PK |
| `order_id` | varchar | PK (composite), NOT NULL |
| `customer_id` | varchar | NOT NULL |
| `sku` | varchar | PK (composite), NOT NULL |
| `qty` | int | NOT NULL |
| `price` | numeric(18,4) | NOT NULL |
| `line_index` | int | NOT NULL |
| `message_id` | varchar | UNIQUE, NOT NULL |
| `created_at` | timestamp | NOT NULL |

- Composite PK: `(order_id, sku)` — enforced via `@IdClass(OrderLineEventId)`.
- `message_id` UNIQUE — hard idempotency guard (replays can never duplicate a row even if PK were missing).
- **Sibling table `order_lines` is untouched** (verified by `SchemaCreationTest`).
- JPA: `ddl-auto=update`, `PostgreSQLDialect`, `open-in-view=false`. Column names follow Spring Boot's
  default snake_case physical naming strategy.

## Kafka production (ack + poison)

| Concern | Target | Key | Serializer |
|---|---|---|---|
| Ack | `order-persisted` | `orderId` | Jackson 3 `JacksonJsonSerializer<PersistedAck>` |
| Poison | `order-failed` | none | `StringSerializer` (JSON string) |

Ack record: `{schemaVersion:"1", orderId, messageId, sku, qty, result:"persisted", at, service:"order-persistence-service"}`.

## Lifecycle

- **Fail-fast**: `PersistenceLifecycle` probes `order-created` + `order-persisted` at `ApplicationReady`
  (`TopicStartupProbe`); a missing topic logs the create command and rethrows → refuses to serve.
- **Shutdown**: flush + destroy `KafkaTemplate`, log persisted row count.

## Configuration

| Key | Default | Purpose |
|---|---|---|
| `server.port` | `8082` | HTTP |
| `spring.kafka.bootstrap-servers` | `${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}` | Broker |
| `spring.datasource.url` | `jdbc:postgresql://${DB_HOST:127.0.0.1}:${DB_PORT:5432}/${DATABASE_NAME:ordersdb}` | DB |
| `spring.datasource.username` | `${DB_USER:postgres}` | DB |
| `spring.datasource.password` | `${DB_PASSWORD:postgres}` | DB |

## Components

| Class | Role |
|---|---|
| `Main` | Entry point |
| `PersistenceKafkaConfig` | Producer factories (ack + poison), consumer factory (manual ack), `AckPublisher` / `PoisonPublisher` beans |
| `LineEventConsumer` | The listener: decode → poison or persist → ack → ACK / NACK |
| `LineEventDecoder` | Jackson 3 decode + schemaVersion=1 + critical-field validation (never throws) |
| `PersistenceService` | `@Transactional` idempotent insert (insert-or-skip) |
| `OrderLineEventRepository` | Spring Data JPA: `findByOrderId`, `existsByMessageId`, `existsByOrderIdAndSku` |
| `OrderLineEvent` / `OrderLineEventId` | JPA entity + composite PK |
| `PersistedAck` | Ack record |
| `PersistenceController` | Query API + health (broker + DB reachability) |
| `PersistenceLifecycle` | Fail-fast probe + graceful shutdown |
| `BrokerReachability` | AdminClient probe for `/health` (never throws) |
| `PoisonStore` / `PoisonPublisher` | In-memory poison store + `order-failed` writer |
| `TopicStartupProbe` | Fail-fast admin check with exact create command |

## Tests (`src/test/java/com/example/orders/persistence/`)

- `BaselineTest`, `LineEventDecoderTest`, `SchemaCreationTest` — units
- `PersistenceConsumerKafkaTest`, `EmbeddedBrokerRoundTripTest` — in-process broker round-trip
- `PersistenceIdempotencyTest`, `PersistencePoisonPathTest`, `LinesEndpointTest`, `FailedOrdersEndpointTest`, `FailFastTopicCheckTest`, `JdbcFailureProbeTest` — idempotency, poison, NACK/redelivery, endpoints, fail-fast
- `KafkaTestHarness` — shared embedded-broker harness

Run: `cd order-persistence-service && mvn clean test` (needs native Postgres at `127.0.0.1:5432`)
