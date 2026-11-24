# System Architecture — Kafka Order Event Services

## Topology

```
                        ┌──────────────────────────┐
   POST /orders         │      order-service       │
  ─────────────────────▶│          :8081           │
                        │  REST → validate →       │
                        │  split order into        │
                        │  N line-event messages   │
                        └────────────┬─────────────┘
                                     │ produce (key = orderId)
                                     ▼
                              ┌───────────────┐
                              │ order-created │   Kafka topic (1 partition)
                              └───────┬───────┘
                    one stream, two   │
                    independent       │
                    consumers         │
                 ┌─────────────────────┴──────────────────────┐
                 ▼                                            ▼
      ┌──────────────────────────┐              ┌──────────────────────────┐
      │     email-service        │              │ order-persistence-service│
      │         :8083            │              │          :8082           │
      │  consume → decode →      │              │  consume → decode →      │
      │  render mock mail        │              │  insert into Postgres    │
      │  → bounded in-memory     │              │  (order_line_events)     │
      │  store (1000, evict oldest)│              │  → ack event           │
      └────────────┬─────────────┘              └────────────┬─────────────┘
                   │ produce (key = orderId)                  │ produce (key = orderId)
                   ▼                                          ▼
          ┌───────────────────┐                      ┌───────────────────┐
          │ order-email-sent  │                      │   order-persisted │
          └───────────────────┘                      └───────────────────┘

        undecodable / foreign-schema payloads (from either consumer):
                   ┌───────────────┐
                   │  order-failed │   poison sink (+ raw payload + reason)
                   └───────────────┘
```

## Components

### order-service (producer, :8081)
The only service that accepts inbound client traffic. It receives an order over REST, validates it,
generates the `orderId` itself (any client-supplied value is ignored), and publishes one message per
line item to `order-created` with `key = orderId`. It keeps exactly one `KafkaTemplate<String,
OrderLineEvent>` for its lifetime and destroys it on shutdown.

### order-persistence-service (consumer #1, :8082)
Consumes `order-created` (consumer group `order-persistence-service`). Decodes each payload, persists
one row per line event into Postgres `ordersdb.order_line_events`, publishes the `order-persisted` ack,
then ACKs the source message. On infra failure it NACKs (rethrow → redelivery). On poison payloads it
publishes `order-failed` and ACKs.

### email-service (consumer #2, :8083)
Consumes `order-created` (consumer group `email-service`) — a fully independent consumer. Renders a
mock mail per line event, stores it in a bounded in-memory deque (default capacity 1000, oldest evicted,
evictions counted), publishes the `order-email-sent` ack, then ACKs. On infra failure it NACKs. On poison
payloads it publishes `order-failed` and ACKs.

### Kafka broker
Single-node KRaft broker (`order-kafka2`, `apache/kafka:4.0.0`) at `localhost:9092`, PLAINTEXT,
1 partition per topic, replication factor 1. All four topics (`order-created`, `order-email-sent`,
`order-persisted`, `order-failed`) are created explicitly via `kafka-topics.sh`; no topic auto-creation
is relied on. The pre-existing topic `orders` is never touched.

### PostgreSQL
Native Postgres 15.19 at `127.0.0.1:5432`, database `ordersdb`. The persistence service owns the
`order_line_events` table (Hibernate `ddl-auto=update`, never touches the sibling `order_lines` table).

## Data flow (one order, two line items)

1. Client `POST /orders` → order-service validates.
2. order-service creates a UUID `orderId`, builds 2 `OrderLineEvent` records (`lineIndex` 0 and 1,
   `messageId` = UUID per line, `createdAt` = now), and sends them to `order-created`, key = orderId.
3. Both consumer services poll `order-created` independently (separate consumer groups).
4. Persistence service: decodes → inserts row(s) → publishes `order-persisted` (key = orderId) → ACKs.
5. Email service: decodes → renders mock mail → publishes `order-email-sent` (key = orderId) → ACKs.
6. If a payload is corrupt/foreign-schema, whichever consumer sees it publishes a poison record to
   `order-failed` (`detectedBy`, `sourceTopic`, `reason`, truncated `rawPayload`, `receivedAt`) and ACKs.
7. On redelivery (e.g. DB hiccup), the persistence consumer's idempotency guards prevent duplicate rows.

## Coupling rules (deliberate design)

- **No shared module, no shared classes.** Each consumer service carries its **own private copy** of the
  `OrderLineEventRecord` record and its decoder ("deliberate duplication"). A schema change must be
  applied in both services by hand.
- **No direct HTTP/RPC between services.** The only channel is Kafka topics. Order-service never calls
  the consumers; consumers never call order-service.
- **Consumers are independent.** Email handling does not depend on persistence and vice versa: one
  consumer failing never blocks the other (separate consumer groups).
- **No shared database.** Only order-persistence-service talks to Postgres.

## Lifecycle

- **Startup (fail-fast)**: on `ApplicationReadyEvent`, each consumer probes the topics it requires
  (`order-created` + its own ack topic) via a Kafka AdminClient; if any is missing, the service logs a
  readable error naming the topic and the exact create command, then rethrows and refuses to serve.
- **Shutdown (graceful)**: each service flushes and destroys its `KafkaTemplate` (via
  `DisposableBean`), logging pending/buffered counts (e.g. pending mails, persisted rows).
- **Health**: each service exposes `GET /health`. The persistence service reports broker reachability +
  DB reachability + row count; email reports mails stored + evicted; order-service reports orders
  accepted this run.

## What is intentionally NOT here

Kafka Streams, Avro/schema registry, SMTP, metrics scrape, CQRS/event sourcing — all deliberately
declined to keep the reference implementation focused on Kafka semantics.
