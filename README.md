# Kafka Order Event Services — Spring Boot 4.1.2 + Spring Kafka 4.1.2

Three independent microservices that communicate **only** through Apache Kafka to
demonstrate event-driven architecture and the core Kafka concepts (partitioning,
ack/redelivery, two independent consumers of one topic, poison-message handling,
at-least-once idempotency, graceful shutdown, in-process broker testing).

> Pure Spring Boot — no Quarkus, no Camel. Java 25. Maven (no wrapper).

---

## Kafka learning pair (`lab-producer` / `lab-consumer`)

This repo **also** hosts a self-contained Kafka learning pair that reuses the same broker
(`order-kafka2`, `localhost:9092`) but only ever touches its own `lab-*` topics and consumer groups:

| App | Port | Role |
|-----|------|------|
| `lab-producer` | 8091 | Producer lanes (sync/async, throttle, txn), Kafka Streams lane, AVRO lane, diagnostics |
| `lab-consumer` | 8092 | Consumer lanes (ack dial, filter, quarantine, rebalance, idempotency), diagnostics |

Everything is documented in [`docs/`](./docs/) — start with the hands-on guide:
- **[`docs/hands-on-guide.md`](./docs/hands-on-guide.md)** — *start here*: step-by-step setup + every API call as a copy-paste `curl`, each with what it returns and **what that one call teaches** about Kafka.
- **[`docs/experiments.md`](./docs/experiments.md)** — one row per concept: command → observable result (topic/partition/offset) → enterprise consequence.
- **[`docs/enterprise/`](./docs/enterprise/)** — one page per concept with the property block that changes in a real deployment.
- **[`docs/glossary.md`](./docs/glossary.md)** — don't-lose vs don't-do-twice vs the loss window.

Quick start (broker already up via `docker compose up -d`):
```bash
cd lab-producer && mvn spring-boot:run          # :8091
cd ../lab-consumer && mvn spring-boot:run       # :8092
```
These are **separate Maven modules** (no aggregator). The 3-service run book below is unchanged and
independent of this pair.

---

## Architecture

```
                        ┌──────────────────────────┐
   POST /orders         │      order-service       │
  ─────────────────────▶│          :8081           │
                        │  REST → split order into  │
                        │  N line-event messages    │
                        └────────────┬─────────────┘
                                     │ produce (key = orderId)
                                     ▼
                              ┌───────────────┐
                              │ order-created │   Kafka topic (1 partition)
                              └───────┬───────┘
                    one stream, two    │
                    independent        │
                    consumers          │
                 ┌─────────────────────┴──────────────────────┐
                 ▼                                            ▼
      ┌──────────────────────────┐              ┌──────────────────────────┐
      │     email-service        │              │ order-persistence-service│
      │         :8083            │              │          :8082           │
      │  consume → mock mail     │              │  consume → Postgres row   │
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

## Service documentation

The three order services are documented in detail under [`docs/services/`](./docs/services/) — architecture, Kafka contract, per-service functionality, operations:

- **[`docs/services/README.md`](./docs/services/README.md)** — index + big picture
- **[`docs/services/architecture.md`](./docs/services/architecture.md)** — components, data flow, coupling rules, lifecycle
- **[`docs/services/kafka-contract.md`](./docs/services/kafka-contract.md)** — topics, payload schema, keys, ack/NACK, poison handling
- **[`docs/services/order-service.md`](./docs/services/order-service.md)** — producer: REST API, validation, mapping, shutdown
- **[`docs/services/order-persistence-service.md`](./docs/services/order-persistence-service.md)** — consumer #1: decode, Postgres idempotency, query API
- **[`docs/services/email-service.md`](./docs/services/email-service.md)** — consumer #2: decode, mock mail store, query API
- **[`docs/services/operations.md`](./docs/services/operations.md)** — environment, run order, config, testing, troubleshooting

Only coupling between services is the **Kafka topics** — no shared module, no
direct HTTP/RPC. Each consumer owns its own copy of the message record class
(deliberate duplication).

## Prerequisites / environment

- **Docker socket** (for the Kafka broker container): `DOCKER_SOCKET=/home/abhi/.docker/desktop/backend.sock`
- **Kafka broker**: existing KRaft single-broker container `order-kafka2` (`apache/kafka:4.0.0`) at `localhost:9092` (PLAINTEXT)
- **Postgres**: native 15.19, host `127.0.0.1:5432`, user/password `postgres`, DB `ordersdb`
- **Java 25**, **Maven 3.9.x** (use `mvn` directly — there is no `./mvnw` wrapper)

## Create the topics

```bash
BS=127.0.0.1:9092
kafka-topics.sh --create --if-not-exists --topic order-created    --bootstrap-server $BS
kafka-topics.sh --create --if-not-exists --topic order-email-sent --bootstrap-server $BS
kafka-topics.sh --create --if-not-exists --topic order-persisted  --bootstrap-server $BS
kafka-topics.sh --create --if-not-exists --topic order-failed     --bootstrap-server $BS
```

Verify: `kafka-topics.sh --describe --topic order-created --bootstrap-server $BS`

## Run order

1. **order-service** (`:8081`) — the producer; must be up to accept orders.
2. **email-service** (`:8083`) — consumer #1.
3. **order-persistence-service** (`:8082`) — consumer #2 (needs Postgres reachable).

Each service refuses to start if a required topic is missing (fail-fast, with the
exact create command in the log).

```bash
# order-service
(cd order-service && mvn spring-boot:run)

# email-service
(cd email-service && mvn spring-boot:run)

# order-persistence-service
(cd order-persistence-service && mvn spring-boot:run)
```

## curl walkthrough

```bash
# 1. Place an order (2 line items → 2 messages on order-created, key = orderId)
curl -s -X POST http://localhost:8081/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"acme","items":[{"sku":"A-100","qty":2,"price":19.90},
                                      {"sku":"B-200","qty":1,"price":5.50}]}'
# → 201 {"orderId":"…","lineCount":2,"kafkaKey":"…","topic":"order-created"}

# 2. The email service rendered a mock mail per line:
curl -s http://localhost:8083/emails
# → [{"orderId":"…","subject":"…","body":"…","deliveredAt":"…"}, …]

# 3. The persistence service wrote rows to Postgres:
curl -s http://localhost:8082/orders/<orderId>/lines
# → [{"orderId":"…","sku":"A-100","qty":2,"price":19.90,"lineIndex":0,"messageId":"…"}, …]

# 4. Health on all three:
curl -s http://localhost:8081/health
curl -s http://localhost:8083/health
curl -s http://localhost:8082/health

# 5. Poison sink (only populated if a corrupt/foreign payload was consumed):
curl -s http://localhost:8082/orders/failed
```

## psql checks

```bash
export PGPASSWORD=postgres
psql -h 127.0.0.1 -p 5432 -U postgres -d ordersdb

# both tables present (sibling order_lines is untouched)
\dt
--  order_line_events   ← new, owned by this service
--  order_lines         ← pre-existing sibling, row count unchanged

# rows for one order
SELECT order_id, sku, qty, price, line_index, message_id FROM order_line_events
WHERE order_id = '<orderId>' ORDER BY line_index;
```

## Kafka concepts covered (from `design.md`)

| # | Kafka concept | Where demonstrated | Where observable |
|---|---|---|---|
| 1 | Topic + partition config (KRaft, 1 partition, RF 1) | 4 topics created by explicit command | `kafka-topics.sh --describe` |
| 2 | Message **key** → deterministic partitioning | key = `orderId`; one order's lines share a partition | consumer logs print the key |
| 3 | Payload **codec** (Jackson 3 JSON, `schemaVersion`) | `KafkaTemplate`/consumer factory with Jackson 3 serializers | malformed JSON → poison path |
| 4 | **One stream, two independent consumers** | `order-created` consumed by both email + persistence | ack topics gain records |
| 5 | **ACK** semantics | success and poison paths both ACK | offsets advance; no redelivery loop |
| 6 | **NACK / redelivery** | infra failure → NACK → redelivered next poll | row appears exactly once after DB recovers |
| 7 | **Poison-message handling** (fail-over sink) | undecodable → `order-failed` + raw payload + reason, then ACK | `GET /orders/failed` |
| 8 | **At-least-once** ⇒ **idempotent** consumer | PK `(orderId, sku)` insert-or-skip | row count stays exact after replay |
| 9 | Producer **lifecycle** (`KafkaTemplate.destroy()`) | graceful shutdown on SIGINT | log line with pending count |
| 10 | Broker-side **offset/seek & polling** | consumer factory props (manual ack, auto.offset.reset) | manual run |
| 11 | **Fail-fast** on wrong topology (missing topic) | startup topic check | service refuses to start |
| 12 | **In-process broker for tests** (`EmbeddedKafkaBroker`) | per-service `KafkaTestHarness` | `mvn clean test` gate |

## Testing

Each service is self-contained with an in-process KRaft broker (`spring-kafka-test`
`EmbeddedKafkaKraftBroker`). The persistence service tests run against the real
native Postgres (`ordersdb`) and write only to the new `order_line_events` table.

```bash
(cd order-service && mvn clean test)
(cd email-service && mvn clean test)
(cd order-persistence-service && mvn clean test)
```

## docker-compose (topology documentation)

`docker-compose.yml` at the repo root documents the broker + 4-topic topology for
reference. The test gate uses in-process brokers and does **not** depend on it.
