# Design: Kafka Order Event Services (Spring Boot 4.1.2 + Spring Kafka 4.1.2)

## Technical Approach

Three **separate applications**, each its own Maven project and process, coupled **only** through the
existing Kafka broker (`order-kafka2`, `apache/kafka:4.0.0`, `localhost:9092`). No shared module, no shared
JDBC connection, no shared class: the same event is consumed twice by two codebases that never import each
other. This follows the pattern Phase 1 recorded from `../../Quarkus` ("The two services share no code and
no database connection — they only exchange JSON messages over Kafka"), deliberately re-implemented on pure
Spring Boot per the user's Phase 0 answer 5.

```
  client: POST /orders  :8081
        │
        ▼
 ┌────────────────┐  produce key=orderId   ┌───────────────────────────┐
 │ order-service  │ ─────────────────────► │ topic: order-created      │
 │ (producer)     │                        └───────────┬───────────────┘
 └────────────────┘                                    │ consumed twice, independently
                        ┌──────────────────────────────┴───────────────────────────┐
                        ▼                                                          ▼
              ┌──────────────────────────┐                        ┌───────────────────────────┐
              │ order-persistence-service│  :8082                 │ email-service      :8083   │
              │ Kafka → Postgres table   │                        │ Kafka → mock mail (mem+log)│
              └───────────┬──────────────┘                        └───────────┬───────────────┘
                          │ produce ack event                                 │ produce ack event
                          ▼                                                   ▼
                 topic: order-persisted                          topic: order-email-sent
                          └───────────────┬───────────────────────────────────┘
                                          ▼
                             topic: order-failed  (poison sink, both consumers write)
                                          │
                                          ▼
                                   Postgres ordersdb / order_line_events
```

Conventions carried from Phase 1 (not invented): package root `com.example.orders.<service>`,
`application.properties` config, `mvn` (no `./mvnw`), JDK-source-level pinning at 17, ports 8081/8082/8083.

## Kafka concepts covered (explicit, testable — replaces "all the important concepts")

| # | Kafka concept | Where demonstrated | Where observable |
|---|---|---|---|
| 1 | Topic + partition configuration (KRaft single broker, 1 partition, RF 1) | 4 new topics created by explicit command; matches measured broker layout | `kafka-topics.sh --describe` |
| 2 | Message **key** → deterministic partitioning | key = `orderId`; all lines of one order share a partition, keeping per-partition order | consumer logs print the key |
| 3 | Payload **codec** (Jackson 3 JSON round-trip, `schemaVersion`) | `KafkaProducerFactory`/`KafkaTemplate` + `KafkaConsumerFactory` with Jackson 3 encoders/decoders | malformed JSON → A1 poison path |
| 4 | **One stream, two independent consumers** | `order-created` consumed by persistence service and by email service | `--count` on both ack topics |
| 5 | **ACK** semantics (message considered handled) | success and poison paths both ACK | offsets advance; no redelivery loop |
| 6 | **NACK / message redelivery** | infrastructure failure (Postgres IO) → NACK → redelivered next poll | row appears exactly once once DB recovers |
| 7 | **Poison-message / error handling** (fail-over sink) | undecodable → `order-failed` + raw payload + reason, then ACK | `GET /orders/failed` + `order-failed` topic |
| 8 | **At-least-once** replay ⇒ **idempotent** consumer | PK `(orderId, sku)` insert-or-skip | row count stays exact after restart |
| 9 | Producer **lifecycle** (`KafkaTemplate.destroy()`, unsubscribe on shutdown) | graceful shutdown on SIGINT | log line with pending count |
| 10 | Broker-side **offset/seek & polling** (consumer loop, block/timeout config) | consumer `KafkaConfig` props (`block.polling`, timeouts) | README manual run |
| 11 | **Fail-fast when the topology is wrong** (missing topic) | startup topic check, readable failure | service refuses to start |
| 12 | **In-process broker for tests** (`EmbeddedKafkaBroker`) | per-service test harness | `mvn clean test` gate |

Declined on purpose (Phase 2 A6/B7/C12): Kafka Streams, Avro + schema registry, Micrometer metrics, SMTP.

## Version / API claims — each carries a citation

- **Spring Boot `4.1.2`** is the newest **GA** (4.2.0 exists only as `-M1/-M2/-RC1/-RC2`) — Maven Central
  directory probe: https://repo1.maven.org/maven2/org/springframework/boot/spring-boot/
- **Java baseline 17, compatible through 27** → installed OpenJDK 25 is supported:
  https://docs.spring.io/spring-boot/system-requirements.html (Boot 4.0 migration guide also states "requires
  Java 17 or later", Jakarta EE 11 / Servlet 6.1 baseline: https://github.com/spring-projects/spring-boot/wiki/Spring-Boot-4.0-Migration-Guide)
- **Spring for Apache Kafka 4.1.x is the Spring Boot 4 generation**: https://spring.io/blog/2026-06-09/spring-kafka-4
- The **4.1** API surface uses `KafkaListener` + `KafkaTemplate` with a listener/producer **container factory**
  bean into an `AnnotationConfigApplicationContext` — fetched live from
  https://docs.spring.io/spring-kafka/reference/4.1/quick-tour.html (docs root `/reference/4.1/index.html` → HTTP 200)
- **`EmbeddedKafkaBroker`, `EmbeddedKafkaKraftBroker`, `EmbeddedKafkaHolder`** exist in the testing chapter —
  fetched live from https://docs.spring.io/spring-kafka/reference/testing.html
- Boot 4 supports `application.properties` and YAML: https://docs.spring.io/spring-boot/how-to/properties-and-configuration.html
- **`spring-boot-data-jpa`** exists in the 4.1.x line: https://central.sonatype.com/artifact/org.springframework.boot/spring-boot-data-jpa/4.1.0
- Boot 4 module **names verified present at `4.1.1`** in the local `~/.m2` cache (`spring-boot`,
  `spring-boot-autoconfigure`, `spring-boot-starter`, `spring-boot-starter-web`, `spring-boot-starter-jackson`,
  `spring-boot-starter-data-jpa`, `spring-boot-starter-jdbc`, `spring-boot-starter-test`, `spring-boot-test`,
  `spring-boot-test-autoconfigure`, `spring-boot-web-server`, `spring-boot-webmvc`, `spring-boot-reactor-netty`,
  `spring-boot-jackson`) — resolution at `4.1.2` is proven by the Phase 6 build, not assumed.
- **Boot-4 web-route annotation syntax is a Phase 5 research point**: the Boot 4 web reference
  (https://docs.spring.io/spring-boot/reference/web/index.html) documents `spring-boot-webmvc`,
  `spring-boot-webflux`, `graceful-shutdown.html`, `reactive.html`; before writing the first HTTP test I will
  fetch the exact `webmvc` route/controller syntax from that page and use it (research rule, not a user
  ambiguity). Confidence for the *shape* of our endpoints: high; for the exact Boot-4 annotation spelling: medium until fetched.

## Data Model

### Kafka message contract (v1, `schemaVersion: "1"`)

Produced by `order-service` to `order-created`, consumed unchanged by both consumers (each service declares
its **own** copy of the record class — duplication is the point, see below):

```json
{
  "schemaVersion": "1",
  "orderId": "6f0f…uuid",
  "customerId": "acme",
  "sku": "A-100",
  "qty": 2,
  "price": 19.90,
  "lineIndex": 0,
  "messageId": "…uuid",
  "createdAt": "2026-11-10T12:37:09Z"
}
```

Key = `orderId`. One message **per line item** (Phase 1 confirmation 3).

Ack message contract (both consumers → their ack topic), key = `orderId`:

```json
{ "schemaVersion":"1","orderId":"…","messageId":"…","sku":"…","qty":2,
  "result":"persisted|email-sent","at":"…","service":"order-persistence-service|email-service" }
```

Poison message contract (both consumers → `order-failed`), key = `orderId`:

```json
{ "schemaVersion":"1","reason":"json-decode|schema-mismatch","rawPayload":"<original bytes as text>",
  "sourceTopic":"order-created","receivedAt":"…","detectedBy":"order-persistence-service|email-service" }
```

`messageId` and `createdAt` are produced by the *producing* service so replay can be detected without
broker metadata (Assumption 5).

### Postgres (existing `ordersdb`, credentials `postgres:postgres`)

New table (created by Spring Data JPA startup from committed SQL, idempotent, does **not** touch `order_lines`):

```sql
CREATE TABLE order_line_events (
  id          BIGSERIAL   PRIMARY KEY,
  orderId     TEXT NOT NULL,
  customerId  TEXT NOT NULL,
  sku         TEXT NOT NULL,
  qty         INT NOT NULL,
  price       NUMERIC(10,2) NOT NULL,
  lineIndex   INT NOT NULL,
  messageId   TEXT UNIQUE,           -- replay guard (A2)
  createdAt   TIMESTAMPTZ NOT NULL,
  UNIQUE (orderId, sku)              -- duplicate-line guard (A2)
);
```

Modelled with Spring Data 4 JPA (`@Entity`), mirroring the sibling's `OrderLine.java` shape but with the
extra `lineIndex/messageId/createdAt` fields and the two uniqueness guards. `email-service` has **no** DB
dependency (D15).

## API Changes (Backend) — all new; nothing exists in this repo

| Method | Path | Service | Request body | Response |
|---|---|---|---|---|
| `POST` | `/orders` | order-service `:8081` | `{"customerId":"acme","items":[{"sku":"A-100","qty":2,"price":19.90}]}` | `201` `{"orderId":"…","lineCount":2,"kafkaKey":"…","topic":"order-created"}` |
| `GET` | `/orders` | order-service | – | `200` list of accepted orders (`orderId` + `lineCount`) — added for demo navigation (assumption, see below) |
| `GET` | `/orders/{orderId}/lines` | order-persistence-service `:8082` | – | `200` `[{"orderId":…,"sku":…,"qty":…,"price":…,"lineIndex":…,"messageId":…,"createdAt":…}]`, `404` if none persisted yet |
| `GET` | `/orders/failed` | order-persistence-service | – | `200` poison records received from `order-failed` (A1 observability, Assumption 12) |
| `GET` | `/emails` | email-service `:8083` | – | `200` `[{"orderId":…,"subject":"…","body":"…","deliveredAt":…,"lines":n}]`, `[]` when nothing received yet |
| `GET` | `/orders/{orderId}/emails` | email-service | – | `200` the mail(s) for that order, `404` if none |
| `GET` | `/health` | all three | – | `200` `{"status":"ok","topic":"…","broker":"localhost:9092"}` |

Validation: `422` `{"errors":[{"field":"items[0].qty","reason":"must be > 0"}]}` and **no Kafka publish**
(B10). Empty-collection responses are `200 []`, not `404` (B9/A: consumer "not yet" cases use `404` for a
specific `orderId`, collections use `[]`).

## Frontend Changes

None — no frontend exists in this repo (no `package.json`/`angular.json`, measured). Observability is via the
HTTP endpoints above plus `curl` and `kafka-console-consumer` commands in `README.md`.

## Testing strategy

- **Gate (hermetic):** `mvn clean test` inside each project. Per-service tests run against an
  `EmbeddedKafkaBroker` (Spring Kafka testing chapter; `EmbeddedKafkaKraftBroker` matches the real KRaft
  broker) so the Phase 6 gate never needs the Docker daemon (which on this machine is only reachable through
  `DOCKER_SOCKET=/home/abhi/.docker/desktop/backend.sock` — measured).
- **Assertions come from `specs/*.md` scenarios**, never from implementation details: poison → `order-failed`
  then ACK; NACK → redelivery; duplicate `messageId` → exactly one row; key = `orderId` → same partition;
  invalid REST body → `422` **and** no message on `order-created`.
- **Manual e2e (not part of the gate):** README recipe — create topics, `curl POST /orders`,
  `kafka-console-consumer` on all four topics, `psql` count on `order_line_events`, `curl GET /emails`.
- **No test touches the sibling's `order_lines`** or the `orders` topic; that is the regression check.

## Security considerations

- No authentication (not requested). Keep bodies bounded: reject oversized `items[]` (limit `100`) and
  oversize strings (`sku`/`customerId` ≤ 200) before publishing, so a client cannot push a huge payload onto
  the stream; reject unknown fields with `422` rather than ignoring them.
- `rawPayload` in `order-failed` is **not** echoed unbounded on `GET /orders/failed` — truncate to 1000 chars
  and log at WARN with the truncation flagged (A1 + boundary-case rule from `AND IF` clauses).
- Postgres URL is assembled from config, never interpolated from a Kafka message.

## Performance considerations

- No throughput target was requested (C12 declined metrics). Keep consumers block-poll with a documented
  timeout rather than busy-looping, and use one `KafkaTemplate` per process (not per message).
- Persistence inserts are one JDBC round-trip per Kafka message, with a per-message transaction — the honest
  (slow) shape, kept visible for the lesson rather than batched behind a cache.
- Mock-mail store is a bounded in-memory list (default 1000 entries) so a long demo run cannot grow memory
  unboundedly.

## Deliberate duplication (not an oversight)

`OrderLineEventRecord` (decode/validate) and the ack/poison payload classes are **written twice**, once in
the persistence service and once in the email service. A shared module would make the two services
collaborate, which is exactly what "the only link is Kafka" forbids (Q-0.6). Phase 7 must not "fix" this by
extracting a commons package; the duplication is a documented design decision.
