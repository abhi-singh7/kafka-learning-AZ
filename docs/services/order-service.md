# order-service — REST in, Kafka out (:8081)

**Package root:** `com.example.orders.api` (Spring Boot 4.1.2, Spring Kafka 4.1.2, Jackson 3)
**Module:** `order-service/` (`order-service` Maven artifact, no wrapper — use `mvn`)

The order-service is the only service that accepts client traffic. It validates an incoming order,
generates the `orderId` itself, and publishes one Kafka message per line item to the `order-created`
topic. It has no database and no consumer; it is a pure producer.

## Functionality

1. Accept `POST /orders` with `{"customerId", "items": [{"sku","qty","price"}…], "orderId"?}`.
   A client-supplied `orderId` is accepted on the wire (old clients don't get 400s) but is **ignored** —
   the service generates its own UUID (spec decision A3).
2. Validate the payload **before touching Kafka** (pure, static validation — no I/O).
3. Split the validated order into N `OrderLineEvent` records (one per item) with a shared `orderId`,
   one `messageId` (UUID) per line, and a shared `createdAt`.
4. Publish each event to `order-created`, **key = `orderId`** (one partition per order).
5. Return `201 Created` with `{"orderId", "lineCount", "kafkaKey", "topic"}` where
   `kafkaKey == orderId` (the partitioning key).

## REST API

| Method & path | Success | Failure | Body |
|---|---|---|---|
| `POST /orders` | `201 Created` | `422 Unprocessable Entity` (validation) / `422` (malformed JSON) | body: `{"orderId","lineCount","kafkaKey","topic"}` or `{"errors":[{"field","reason"}]}` |
| `GET /orders` | `200` | — | `[{"orderId","lineCount"}]` (in-memory, oldest first; `[]` when none) |
| `GET /health` | `200` | — | `{"status":"UP","ordersAccepted":N}` |

Validation errors are collected **non-fatally** (all rules checked, not just the first failure) and
returned in one `422` body with `{"field","reason"}` entries. Malformed JSON → `422` with
`{"errors":[{"field":"body","reason":"request body is not valid JSON"}]}`.

## Validation rules (`OrderValidator`)

Pure static function; no Spring, no I/O.

- `customerId`: required, non-blank, ≤ 200 chars
- `items`: required (non-null), non-empty, ≤ 100 items
- Per item (index `i`): `sku` non-blank, ≤ 200 chars; `qty` > 0 (Integer wrapper — null ⇒ error);
  `price` ≥ 0 (Double wrapper — null ⇒ error). Null item → `items[i]` error.

Errors point at the precise field, e.g. `items[0].qty`.

## Components

| Class | Role |
|---|---|
| `Main` | Spring Boot entry point |
| `OrderController` | `POST /orders`, `GET /orders`, `GET /health`; owns the in-memory accepted-order list |
| `OrderValidator` | Pure validation contract |
| `OrderToLineEventsMapper` | Pure mapper: order → N line events; sets `messageId`/`createdAt` |
| `KafkaConfig` | Single `ProducerFactory<String, OrderLineEvent>` + single `KafkaTemplate` (Jackson 3 `JacksonJsonSerializer`) |
| `OrderProducerShutdown` | `DisposableBean`: flush + destroy KafkaTemplate, log pending count |
| `OrderExceptionHandler` | `@RestControllerAdvice`: maps validation failure → 422 shape |
| `OrderRequestDto` / `OrderItemDto` / `OrderLineEvent` / `OrderAcceptedResponse` / `HealthDto` / `AcceptedOrderDto` / `OrderValidationError` | Immutable value types |

## Kafka production

- **Topic**: `order-created` (hardcoded `TOPIC` constant in `OrderController`)
- **Key**: `orderId` (String serializer) — all lines of one order land on one partition
- **Value**: `OrderLineEvent` (nine schemaVersion-1 fields, Jackson 3 JSON)
- **Serialization**: `JacksonJsonSerializer` (Jackson 3); `StringSerializer` for the key
- **Lifecycle**: one `KafkaTemplate` per process; flushed + destroyed on shutdown via `DisposableBean`
  (`OrderProducerShutdown`), with one log line confirming shutdown and the pending count

## Configuration

| Key | Default | Purpose |
|---|---|---|
| `server.port` | `8081` | HTTP |
| `spring.kafka.bootstrap-servers` | `${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}` | Broker |

## Error handling summary

| Failure | Response | Published to Kafka? |
|---|---|---|
| Validation errors | `422` with all errors | No |
| Malformed JSON body | `422` | No |
| Valid order | `201` | Yes — N messages, one per item |

## Tests (`src/test/java/com/example/orders/api/`)

- `BaselineTest`, `WireFormatTest`, `OrderResponseContractTest` — contract checks
- `OrderControllerTest`, `OrderValidatorTest`, `OrderToLineEventsMapperTest` — pure units
- `OrderProducerKafkaTest`, `EmbeddedBrokerRoundTripTest` — in-process broker (`EmbeddedKafkaKraftBroker` via `KafkaTestHarness`)

Run: `cd order-service && mvn clean test`
