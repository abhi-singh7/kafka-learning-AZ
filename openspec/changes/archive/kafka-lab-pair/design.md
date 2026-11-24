# Design: Kafka learning pair — `lab-producer` (`:8091`) + `lab-consumer` (`:8092`)

## Technical Approach

Two **separate applications**, each its own Maven project and process, coupled **only through Kafka topics**
on the broker that already runs on this machine — container `order-kafka2` (`apache/kafka:4.0.0`, KRaft single
node, `0.0.0.0:9092->9092/tcp`, PLAINTEXT). No shared Java module, no HTTP call between the two apps, no
shared consumer group: the coupling is the wire. That is the enterprise lesson — services never call each
other, they exchange events.

One **lane per concept** so a demo never shares a topic or a group with another demo:

```
 POST /lab/orders  :8091                    order-kafka2 · localhost:9092
      │
      ▼
 ┌──────────────┐  key=customerId  ┌───────────────────────────────┐
 │ lab-producer │ ───────────────► │ lab-orders        (3 parts)   │
 │              │ ───────────────► │ lab-orders-avro   (AVRO/registry)
 │              │ ──┬─ txn ─────► │ lab-ack-a , lab-ack-b         │  (all-or-nothing)
 │              │   └─ Streams ─► │ lab-pending → lab-accepted    │  (stateless + stateful)
 └──────────────┘                └───────┬─────────────────────────┘
                                         │ popped non-stop, three groups
                                         ▼
                                ┌────────────────┐   error handler / quarantine
                                │ lab-consumer   │ ────────────────────────────► lab-rejected
                                │    :8092       │ ── rollback/retry ─────────► lab-retry
                                └────────────────┘ ── txn ack ───────────────► (lab-ack-a/b advance atomically)
   broker-owned, read-only: __consumer_offsets (per group) · _schemas (AVRO versions)
```

Conventions carried from the sibling project in this repo (not invented): package root `com.example.lab.<app>`,
`application.properties`, Boot `4.1.2`, `<source>17</source>/<target>17</target>`, `mvn` (no `./mvnw`),
`@EmbeddedKafka` for the test gate, `GET /health` per app, one app per port.

## Kafka concepts — explicit and testable (replaces "all the important concepts")

| # | Concept | Where demonstrated | Observable at |
|---|---|---|---|
| B1 | Topic + partition config (KRaft, RF 1, `lab-orders` = 3 partitions) | `NewTopic` beans per lane | `GET /lab/diagnostics`, `kafka-topics.sh --describe` |
| B2 | Key → deterministic partition (key = `customerId`) | `/lab/orders` keyed vs `/lab/orders/unkeyed` | consumer lifecycle log `partition@offset / key / headers / payload / decision` |
| B3 | Key, headers, payload separation | `x-lab-trace`, `x-lab-correlation`, `x-lab-schema` headers | consumer log prints three parts; rule "route before deserialize" |
| B4 | Codec by properties (String lane vs AVRO lane) | `spring.kafka.producer.value-serializer` / `consumer.value-deserializer` | `_schemas` gains a version on schema evolution |
| B5 | Topic creation at startup; "topic exists ⇒ bean ignored" | startup logs | second start logs nothing new |
| B6 | Sync vs async ack (two beans + one toggle property) | `/lab/orders` (async, 202+Location) vs `/lab/orders/sync` | async vs blocking timing; `// DO-NOT-SHIP` class for the blocking shape |
| B7 | Producer flow control (`acks`, `queue-size`, `max.blocking`, linger/batch, fast-producer/slow-consumer) | `/lab/throttle` | `429` + `Retry-After` + rejected-push counter; no OOM |
| B8 | Flush on shutdown | SIGTERM on producer | `flushed N` log line |
| B9 | Don't-lose ≠ do-once (`acks=all`, idempotency key, replay window) | both apps | `docs/experiments.md` row + consumer dedup ledger |
| B10 | Health = broker reachable | `GET /health` both apps | Actuator health + admin bean |
| A11 | Correlation/trace id end-to-end | both | trace id in consumer log equals the push's trace id |
| A12 | Back-pressure 429 + `Retry-After` | `/lab/throttle` | see B7 |
| A13 | Push-time logging on completion (async done-branch) | producer | log shows metadata after the broker acked |
| A14 | Atomic multi-topic push (transaction manager, `transaction-id-prefix`) | `/lab/orders/transactional` | forced failure ⇒ neither `lab-ack-a` nor `lab-ack-b` advanced |
| A15 | PoS endpoints + metrics + audit trail | both | Actuator + Boot metrics/audit |
| A16 | Profiles dev vs prod (TLS/SASL/multi-host block) | both | `application-prod.properties` + `docs/properties-prod.md` |
| C1 | Pop non-stop, forever | `@KafkaListener` beans | JVM alive after drain; cold-start backlog processed |
| C2 | Backlog: cold start vs inflow-while-slow | `/lab/backlog`, `/lab/diagnostics` | in-flight queue count vs offset gap |
| C3 | Ack dial: immediate / after-processing / batch checkpoint | `lab.ack.strategy` property | offsets before/after; loss window stated per mode |
| C4 | Sync vs async ack on the pull side | toggle property | offset-commit timing in the log |
| C5 | Two listeners, one topic, one group | `lab-rebalance`, groups `lab-consumer`+`lab-competitor` | total processing count = message count |
| C6 | Filter bean by header ⇒ filtered-out **is** acked | `x-lab-skip` | filter rejects do not advance offsets (not re-read) |
| C7 | Offset/partition truth | `GET /lab/diagnostics` | per partition: group offset + next offset |
| C8 | Per-message lifecycle log | consumer | one line per message with decision |
| C9 | PoS endpoints, metrics, health | consumer | `/lab/events`, `/lab/metrics`, `/health` |
| A10 | Bounded retry + exponential backoff (error-handler bean) | `POST /lab/inject-failures {"percentage":30}` | retry cap then quarantine |
| A11b | `AfterRollbackProcessor` both modes | `lab.rollback.mode` property | offsets before/after in diagnostics |
| A12b | Transactional pop (KTM + `ChainedKafkaTransactionManager` `@Primary`) | `lab-ack-a`/`lab-ack-b` | forced failure ⇒ source offset unchanged |
| A13b | Graceful shutdown on the pull side | SIGTERM on consumer | exit 0, backlog preserved, acked segment not re-processed |
| A14b | Rebalance + seek (`ConsumerAwareRebalanceListener`), `GROUP_INITIAL_REBALANCE_DELAY_MS=0` | `POST /lab/listeners/competitor/off` | survivor re-reads from offsets |
| A15b | Quarantine lane ≠ filter | error handler → `lab-rejected` | `GET /lab/rejected` shows raw + reason + `partition@offset` |
| A16b | Idempotent processing under crash/retry | dedup ledger keyed `orderId` | count exact after restart |
| A17 | Kafka Streams: stateless + **stateful** per-customer running total | `lab-pending` → `lab-accepted` | `GET /lab/accepted` |
| A18 | Metrics: Boot metrics + Micrometer + `KafkaListenerObservationConvention` | consumer | per-listener label in `/lab/metrics` |
| A19 | Hot reload while messages flow | Boot DevTools (D-9 degrade if JDK-25 blocks) | filter behaviour changes without drain |
| A20 | Broker-owned state read-only (`__consumer_offsets`, `_schemas`) | admin bean | diagnostics shows both; no write path touches them |
| A21 | Diagnostics & tuning (JVM/OS + Boot metadata; where the backlog shows in memory) | consumer | `docs/experiments.md` row |
| X | Concept-only: replication loss, TLS/SASL, ACLs, cluster sizing, Zookeeper/KRaft | `docs/enterprise/*.md` + prod properties | labelled **not demonstrated** (single broker, RF 1, PLAINTEXT) |

## Decisions (recommended answers; confirm in Phase 4)

| # | Question | Answer |
|---|---|---|
| D-1 | AVRO needs a schema registry; none is up | **Start `cp-schema-registry:7.9.0` re-pointed at `PLAINTEXT://localhost:9092`** (config exists at `~/IdeaProjects/kafka-02/docker-compose.yml`, but its `SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS` points at `kafka:19092` — must be overridden). Fallback: Boot's `JacksonJson*` lane only, AVRO as inline-schema, registry read of `_schemas` documented. |
| D-2 | Boot version | **`4.1.2`** — "3.5+" cannot use the Kafka module: `spring-boot-kafka`/`spring-boot-starter-kafka` first exist at `4.0.0-M1`; the 3.5 line ends at `3.5.16` with no Kafka module. 4.2.x are `-M/-RC` only. |
| D-3 | Broker | **Reuse `order-kafka2`, never recreate.** Do not run the repo's compose file (it documents a topology; recreation resets topics and `__consumer_offsets`). Poll `docker logs` for `Successfully registered broker … epoch N`; never `sleep`. |
| D-4 | Namespaces | topics `lab-*`, groups `lab-consumer`/`lab-competitor`/`lab-txn`, ports **8091/8092**. Sibling owns `order-*` and 8081/8082/8083; offsets in `__consumer_offsets` are per group so a shared group name would inherit foreign offsets. |
| D-5 | HTTP layer | Boot Web/WebMVC (as in the sibling) + Boot Webflux for the producer's async push lane. |
| D-6 | JDBC/Postgres in these two apps | **No** — persistence stays in the sibling project; a JDBC lane would blur the Kafka lesson. |
| D-7 | Failure injection | one property-driven `POST /lab/inject-failures {"percentage":N,"lane":"lab-orders"}` driving retry/backoff/quarantine/rollback. |
| D-8 | Partitions | `lab-orders` = **3** (key math visible), every other lane = **1** (RF-1 single broker). |
| D-9 | Hot reload on JDK 25 | attempt Boot DevTools; if blocked, degrade to **restart without drain** and record why in `docs/experiments.md`. |
| D-10 | Streams placement | **inside `lab-producer`** — keeps exactly two apps. |
| D-11 | Doc depth for concept-only items | write-up + commented prod property block, no simulation. |

## Claims — each with a citation

- **Boot `4.1.2` is the newest GA**; 4.2.0 exists only as `-M1/-M2/-RC1/-RC2`; the 3.5 line ends at `3.5.16`:
  `https://repo1.maven.org/maven2/org/springframework/boot/spring-boot/`
- **The Boot Kafka module exists only from 4.0.0-M1** (no 3.5 line):
  `https://repo1.maven.org/maven2/org/springframework/boot/spring-boot-kafka/`
- **Modules verified present at `4.1.2`** (directory listings of `spring-boot-messaging/` and
  `spring-boot-web/`): `spring-boot-starter-kafka`, `spring-boot-kafka`, `spring-boot-micrometer-metrics`,
  `spring-boot-actuator`, `spring-boot-webflux`, `spring-boot-webmvc`, `spring-boot-test-mvc`,
  `spring-boot-testcontainers`, `spring-boot-devtools`.
- **Boot Kafka reference (4.1) verified content**: `spring.kafka.bootstrap-servers`,
  `spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JacksonJsonSerializer`,
  `spring.kafka.consumer.value-deserializer=…JacksonJsonDeserializer` with
  `properties[spring.json.value.default.type]` / `properties[spring.json.trusted.packages]`,
  `spring.kafka.streams.application-id` (defaults to `spring.application.name`),
  `spring.kafka.producer.transaction-id-prefix` ⇒ a `KafkaTransactionManager` is auto-configured,
  a `RecordMessageConverter` bean is auto-associated with the `KafkaTemplate`, `spring.kafka.admin.properties[...]`,
  `@EnableKafkaStreams` + `StreamsBuilder` + `Produced.with(Serdes…, new JacksonJsonSerde<>())`,
  `KafkaListenerObservationConvention`, `KafkaTemplateObservationConvention`, `NewTopic`, `KafkaBroker`:
  `https://docs.spring.io/spring-boot/reference/messaging/kafka.html`
- **Autoconfig classes at 4.1.2**: `KafkaAutoConfiguration`, `KafkaMetricsAutoConfiguration` —
  `https://docs.spring.io/spring-boot/appendix/auto-configuration-classes/spring-boot-kafka.html`
- **Spring Kafka 4.1 surface**: `@KafkaListener`, `KafkaTemplate`, `KafkaProducerFactory`,
  `KafkaConsumerFactory`, `KafkaListenerContainerFactory` —
  `https://docs.spring.io/spring-kafka/reference/4.1/quick-tour.html`
- **Test harness**: `@EmbeddedKafka` (Boot Testcontainers) / `EmbeddedKafkaBroker`, KRaft variant, RF 1 —
  `https://docs.spring.io/spring-kafka/reference/testing.html`
- **Phase 5 research point (fetch before writing the first test)**: exact Boot-4 bean spellings for
  `RecordFilterStrategy`, `CommonErrorHandler`, `AfterRollbackProcessor`, `ConsumerAwareRebalanceListener`,
  `ChainedKafkaTransactionManager` (`@Primary`), and the Boot-4 Web route/controller annotation syntax.
  These are named in the Boot 3.5 Kafka page (fetched: bean auto-association onto the listener factory) and
  must be re-confirmed at 4.1. Confidence: shape high, exact spelling medium until fetched.
- **JVM**: Boot 4 requires Java 17+; source/target pinned 17 like the sibling apps (installed JDK 25).

## Data model

`lab-orders` event (String codec, `schemaVersion` `"1"`), **KEY = `customerId`**:

```json
{ "schemaVersion":"1","orderId":"o-1","customerId":"cust-0","sku":"A-100","qty":2,"price":19.90,
  "messageId":"…uuid","traceId":"…uuid","deliveredAt":"2026-11-11T…Z" }
```

Headers on the same message: `x-lab-trace` (= `traceId`), `x-lab-correlation` (= `orderId`),
`x-lab-schema` (= `orders/1`), optional `x-lab-skip` (`true` ⇒ filter lane).

Quarantine record on `lab-rejected` (KEY = source key, or `corrupt`):
`{schemaVersion, reason ∈ (json-decode|schema-mismatch|handler-throw|filter), rawPayload (truncated 1000),
truncated, sourceTopic, sourcePartition, sourceOffset, detectedBy, attempts, firstFailureAt, lastFailureAt}`.

AVRO `OrderEvent` v1: `orderId, customerId, sku, qty, price, messageId, traceId, createdAt`.
v2 adds `discount` (default `0.0`) and `phoneNumber` (default `""`) — schema evolution with defaults, reusing
the semantics already learned in `~/IdeaProjects/kafka-02/src/main/resources/OrderEvent.avsc`.

Transaction lane: `lab-ack-a` + `lab-ack-b`, transaction id = `orderId`.
Streams lane: `lab-pending` → `lab-accepted` (stateless map/filter + stateful per-customer running total).

## Bean graph (sketch; exact Boot-4 spellings fetched in Phase 5)

```
lab-producer : KafkaBrokerConfiguration → KafkaBroker(producer) → KafkaTemplate
             → NewTopic(lab-orders parts=3) · NewTopic(lab-*) ×7 → JacksonJsonSerializer via properties
             → PoS Web endpoints (/lab/orders…) → metrics + health + audit trail
lab-consumer : KafkaBroker → KafkaListenerContainerFactory[ lab-consumer | lab-txn | lab-competitor ]
             → @KafkaListener × lanes (String, AVRO, txn, rebalance, retry)
             beans auto-associated onto the factory:
               RecordFilterStrategy · CommonErrorHandler · AfterRollbackProcessor
             · ConsumerAwareRebalanceListener · KafkaTransactionManager · ChainedKTM(@Primary)
             · RecordMessageConverter · KafkaListenerObservationConvention
             → StreamsBuilder + @EnableKafkaStreams + KafkaStreamsConfiguration (producer app)
```

## Deliberate choices

- **Duplication, not abstraction.** Each app declares its own record class for the same wire event (as the
  sibling project does) — the two services are coupled only by the topic, so duplication *is* the lesson.
- **Namespaced everything (`lab-*`)** because the sibling project and this pair share one broker and one
  `__consumer_offsets`.
- **`acks=all` everywhere except the one lane that teaches the loss window**, which is labelled in the log
  so the difference is attributable to a property, not to two different code bases.

## Risks and degradation

- **Broker restart** (policy `always`): epoch increments, topics may be re-created — the run book re-probes
  topics before each smoke block.
- **Registry absent**: AVRO lane fails fast naming the registry URL and the re-point command (no silent
  downgrade).
- **AVRO serializer dependency availability** (Confluent `kafka-avro-serializer` 7.9.0 vs Maven availability):
  resolve in Phase 5 by mirroring `~/IdeaProjects/kafka-02/pom.xml`; fallback = inline-schema AVRO + documented
  read of `_schemas`.
- **Streams on a single broker + JDK 25**: if Boot 4.1 Streams wiring proves unusable, degrade to a documented
  stateless+stateful pass with the Streams row labelled *degraded* in `docs/experiments.md` — never silently faked.
- **Hot reload on JDK 25**: degrade to restart-without-drain (D-9), recorded.
