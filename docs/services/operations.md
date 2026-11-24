# Operations — environment, run order, configuration, testing, troubleshooting

## Environment prerequisites

- **Docker socket** (for the broker container): `DOCKER_SOCKET=/home/abhi/.docker/desktop/backend.sock`
  (only needed if running docker; the broker is already running in dev as `order-kafka2`)
- **Kafka broker**: `order-kafka2` (`apache/kafka:4.0.0`) at `localhost:9092`, PLAINTEXT, KRaft, 1 partition
- **Postgres**: native 15.19 at `127.0.0.1:5432`, user/password `postgres`, DB `ordersdb`
- **Java 25**, **Maven 3.9.x** (no `./mvnw` wrapper — use `mvn`)

## Topics (create once)

```bash
BS=127.0.0.1:9092
kafka-topics.sh --create --if-not-exists --topic order-created    --bootstrap-server $BS
kafka-topics.sh --create --if-not-exists --topic order-email-sent --bootstrap-server $BS
kafka-topics.sh --create --if-not-exists --topic order-persisted  --bootstrap-server $BS
kafka-topics.sh --create --if-not-exists --topic order-failed     --bootstrap-server $BS
```

Verify: `kafka-topics.sh --describe --topic order-created --bootstrap-server $BS`

## Run order

1. **order-service** (`:8081`) — must be up to accept orders.
2. **email-service** (`:8083`) — consumer #1.
3. **order-persistence-service** (`:8082`) — consumer #2 (needs Postgres reachable).

```bash
(cd order-service && mvn spring-boot:run)
(cd email-service && mvn spring-boot:run)
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

# 2. Email service rendered a mock mail per line
curl -s http://localhost:8083/emails

# 3. Persistence service wrote rows
curl -s http://localhost:8082/orders/<orderId>/lines

# 4. Health on all three
curl -s http://localhost:8081/health
curl -s http://localhost:8083/health
curl -s http://localhost:8082/health

# 5. Poison sink (populated only on corrupt/foreign payloads)
curl -s http://localhost:8082/orders/failed
curl -s http://localhost:8083/orders/failed
```

## psql checks

```bash
export PGPASSWORD=postgres
psql -h 127.0.0.1 -p 5432 -U postgres -d ordersdb
\dt
--  order_line_events   ← owned by this project (new)
--  order_lines         ← pre-existing sibling, row count unchanged

SELECT order_id, sku, qty, price, line_index, message_id FROM order_line_events
WHERE order_id = '<orderId>' ORDER BY line_index;
```

## Configuration reference

| Service | Env overrides | Defaults |
|---|---|---|
| order-service | `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| email-service | `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| order-persistence-service | `KAFKA_BOOTSTRAP_SERVERS`, `DB_HOST`, `DB_PORT`, `DATABASE_NAME`, `DB_USER`, `DB_PASSWORD` | `localhost:9092`, `127.0.0.1`, `5432`, `ordersdb`, `postgres`, `postgres` |

Ports: 8081 / 8082 / 8083. All services use `application.properties` with env-variable overrides — no
hosts, ports or credentials are hardcoded in Java code.

## Testing

Each service is self-contained with an **in-process KRaft broker** (`spring-kafka-test`
`EmbeddedKafkaKraftBroker`, via `KafkaTestHarness`) for the Kafka paths. The persistence service tests
run against the **real native Postgres** and write only to the new `order_line_events` table. Sibling
`order_lines` is never touched or deleted.

```bash
(cd order-service && mvn clean test)
(cd email-service && mvn clean test)
(cd order-persistence-service && mvn clean test)
```

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Service refuses to start | Required topic missing | Run the `kafka-topics.sh --create --if-not-exists …` for the named topic; the error log shows the exact command |
| `/health` shows `broker: unreachable` | Broker down/misconfigured | Check `order-kafka2` container + `KAFKA_BOOTSTRAP_SERVERS` |
| `/health` shows `database: unreachable` | Postgres down | Check native Postgres; verify `DB_*` env vars and `ordersdb` exists |
| Row never appears for an order | Consumer group stuck / NACK loop | Check the consumer logs; ACK only happens after successful handling — see the redelivery contract |
| `order-failed` grows | Poison messages on `order-created` | `GET /orders/failed` to inspect `reason` + `rawPayload`; fix the producer payload, not the sink |
| Duplicate rows in `order_line_events` | Should not happen (PK + UNIQUE guards) | Inspect `messageId`; a genuine logic bug — do not "fix" by dropping the guards |
| Artifacts missing from `/emails` | Eviction | Store is capped at 1000; `/health` reports `evicted` |

## docker-compose

`docker-compose.yml` at the repo root documents the broker + four-topic topology for reference. The
test gate uses in-process brokers and does **not** depend on it. The lab-producer/lab-consumer pair
reuses the same broker; **do not** run `docker compose up` while doing lab work (it would recreate
`order-kafka2` and wipe broker state — the broker is already running).
