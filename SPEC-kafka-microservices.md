# SPEC — Kafka learning pair: `lab-producer` + `lab-consumer` (Spring Boot 4.1.2)

Status: **draft v2 — awaiting review** · Location: `~/DEV-WORK/Kafka` · Date: 2026-11-11

> This document is the human-facing narrative. The **OpenSpec expansion** of the same change lives in
> `openspec/changes/kafka-lab-pair/` (`proposal.md`, `design.md`, `tasks.md`, `specs/{kafka-contract,
> lab-producer,lab-consumer,lifecycle-and-config}.md`) — same decisions (D-1…D-11), testable requirement/scenario
> form. Keep the two in sync; the OpenSpec files are what Phase 5 implementation follows.

Target: two new Spring Boot microservices, built for *learning* every basic and advanced Kafka
concept, each concept mapped to what it means in an enterprise application. They reuse the Kafka
container that is already on the box.

---

## 0. Environment facts (verified on this box, 2026-11-11)

| Item | Verified value | Evidence |
|---|---|---|
| Reusable Kafka container | **`order-kafka2`**, image **`apache/kafka:4.0.0`**, **Up**, port map `0.0.0.0:9092->9092/tcp` | `docker ps -a` |
| Topology | KRaft single node (`KAFKA_PROCESS_ROLES=broker,controller`, `NODE_ID=1`, no Zookeeper) | repo `docker-compose.yml` (topology doc) + image defaults |
| Entry point for host-run JVMs | **`localhost:9092`** / `127.0.0.1:9092`, PLAINTEXT | README of this project, port binding |
| Restart policy of that container | `always` (`RestartCount: 0`) — it self-restarts, so a crash is not fatal to us | `docker inspect 12f22ddaa5cd` |
| Client config actually in the container | **no `KAFKA_*` overrides in `Env`** → the image's own `config.yml` defaults apply; the repo `docker-compose.yml` documents the *intended* topology (listener `PLAINTEXT://0.0.0.0:9092`, controller `19093`, `__consumer_offsets` RF 1, txn-state-log RF 1 / MIN_ISR 1, `GROUP_INITIAL_REBALANCE_DELAY_MS=0`) | same inspect |
| Java / Maven | JDK 25 runtime, `javac` release 21, **Maven 3.9.x**, no `./mvnw` wrapper; existing apps build with `<source>17</source>/<target>17</target>` | `--version` checks, existing poms |
| Spring Boot already used here | Boot **`4.1.2`** with **`spring-boot-starter-kafka`**, `spring-boot-starter-web`, `spring-boot-starter-test`, `spring-boot-webmvc-test`, `spring-kafka`/`spring-kafka-test` at `4.1.2` | `order-service/pom.xml`, `email-service/pom.xml` |
| Maven Central availability | Boot core up to `3.5.16` and `4.0.0 → 4.2.0-RC2`; **`spring-boot-kafka`/`spring-boot-starter-kafka` exist only from `4.0.0-M1`** | `repo1.maven.org` listings |
| AVRO / schema registry | A `cp-schema-registry:7.9.0` container **is not up**; AVRO learning code exists at `~/IdeaProjects/kafka-02` (`OrderEvent.avsc`, `ProducerAvro/ConsumerAvro/AvroResolving`, `kafka-avro-serializer:7.9.0`) | `docker ps -a`, file read |
| Postgres (not required by these two apps) | native, `127.0.0.1:5432`, db `ordersdb` (used by the existing persistence service) | README + `psql` probe (auth required) |
| **Sibling project already in this repo** | 3 services: `order-service` `:8081` → `order-created` → `email-service` `:8083` / `order-persistence-service` `:8082` → `order-email-sent` / `order-persisted`, poison sink `order-failed`; OpenSpec change `openspec/changes/kafka-order-event-services/` (tasks `[x]`) | README, `openspec/` tree, tasks.md |
| Topics already in use | `order-created`, `order-email-sent`, `order-persisted`, `order-failed` (+ stray `order-abc`, `order-jdbc-probe`) | grep over `*/src` |

### Reuse decision — **YES, reuse `order-kafka2`. Do not install or start a new Kafka broker.**

Rules the project must obey:

1. Bootstrap servers for both apps = **`localhost:9092`** (single broker, PLAINTEXT).
2. **Never** `docker compose up` / recreate the broker: recreation resets cluster state and topic data. `order-kafka2` was not started by this repo's compose file, so **do not run that compose file at all** — treat it as documentation only (it says so in its own header).
3. **Poll, never `sleep`** for readiness: `docker logs order-kafka2` → `Successfully registered broker … epoch N`; after any broker restart, re-probe topics before the smoke run.
4. **Namespace everything to avoid the existing project** (single box, shared broker, shared `__consumer_offsets`):
   - topics: **`lab-*`** prefix (§2);
   - HTTP ports: producer **`8091`**, consumer **`8092`** (existing app set owns 8081/8082/8083; Kafka owns 9092);
   - consumer group ids: **`lab-*`** — reusing an existing group name would inherit that group's saved offsets and make the "backlog" behaviour unexplainable.
5. Broker is **single-node, PLAINTEXT, RF 1**: replication/TLS/SASL/ACL/Zookeeper/cluster topics are **concept-only** (§4.3) — documented, not simulated as if demonstrated.
6. AVRO lane needs a schema registry: **decision D-1 in §9** (see §2 row `lab-orders-avro`).

---

## 1. What the project must produce

Two independent Spring Boot applications, **coupled only through Kafka topics** (no shared Java module, no HTTP between them) — that coupling-through-the-wire is itself the enterprise lesson: services never call each other, they exchange events.

| App | Package | Port | Role |
|---|---|---|---|
| **`lab-producer`** | `com.example.lab.producer` | **8091** | HTTP → **push** into Kafka with correct key/partition/headers/codec/serialization, ack only when the broker accepted it; owns the Kafka Streams lane. |
| **`lab-consumer`** | `com.example.lab.consumer` | **8092** | **Pops continuously, forever**; filter, process, ack, retry, roll back, quarantine, expose metrics — the advanced half lives here. |

Both are **Spring Boot 4.1.2** applications (Boot `spring-boot-starter-kafka`, Boot `Web`/WebMVC endpoints, Boot metrics/Actuator, Boot `@EmbeddedKafka` for the test harness), mirroring the sibling project's toolchain so the learning pair builds with the same `mvn` invocation.

### 1.1 Version decision (flag for confirmation)

The request says "Spring Boot 3.5+". **`spring-boot-starter-kafka` does not exist below Spring Boot 4.0** (3.5.x tops at `3.5.16`, no Kafka module). So "3.5+" resolves to **4.1.2** — which is exactly what the sibling project here already uses (stable; 4.2.0-RC2 is pre-release).

**Recommendation:** pin Boot `4.1.2` + `spring-kafka` `4.1.2` (pulls `kafka-clients 4.2.x`), against the `4.0.0` broker — clients negotiate the wire protocol down, and the sibling project proves this combination works on this box. Confirm during the §6.1 smoke run before treating it as final; the strict-3.5 fallback (Boot `3.5.16` + hand-wired `org.apache.kafka` clients, no `@KafkaListener` beans) loses most of the framework value and is documented, not chosen.

### 1.2 Spring Boot feature matrix (what we use, and why)

| Spring Boot feature | Where | Enterprise value |
|---|---|---|
| Application + Beans (`KafkaTemplate`, `NewTopic`, `KafkaListenerContainerFactory`, `KafkaBootstrapConfiguration`) wired by autowire | both | wiring lives in the object graph; per-env config is properties, not constructor args |
| Boot properties `spring.kafka.*` (`bootstrap-servers`, `consumer.group-id`, `producer.transaction-id-prefix`, `producer.value-serializer`, `consumer.value-deserializer`, `listener.*`, `admin.*`, `streams.application-id`, `streams.auto-startup`, `properties[...]`) | both | every Kafka knob externalized → tune per environment, never recompile |
| Beans auto-associated onto the listener factory: `RecordFilterStrategy`, `CommonErrorHandler`, `AfterRollbackProcessor`, `ConsumerAwareRebalanceListener`, `KafkaTransactionManager`, `ChainedKafkaTransactionManager` (`@Primary`), `RecordMessageConverter`/`BatchMessageConverter` | consumer | retry/rollback/filter/seek policy as objects → testable and swappable |
| `@KafkaListener` on application methods | consumer | "pop side" is a method; the framework owns the polling loop and its lifecycle |
| Boot **Web**/WebMVC **PoS endpoints** with **metrics**, **health**, **audit trail**, **exchange record** | both | a push is a 202 + Location (topic/partition/offset), never a blocking `send().get()` in a route |
| **Graceful shutdown** (top-level and per-`Web` bean) | both | on SIGTERM: producer flushes, consumer stops pulling with offsets committed |
| **Actuator** (health, metrics, metadata) + Boot **metrics**/Micrometer + `KafkaListenerObservationConvention`/`KafkaTemplateObservationConvention` | both | per-listener/per-topic throughput & timing in the metrics graph; supervisor-visible liveness |
| **Testcontainers** `@EmbeddedKafka` (KRaft, partitions 1–3) | tests | Kafka in-process for CI; the docker broker only for the smoke run |
| **Hot reload** (Boot DevTools) | both | change a filter bean/serializer while messages keep flowing ("patch without drain") |
| Boot **configuration profiles** | both | `dev` = localhost:9092 PLAINTEXT; `prod` profile documented (multi-host, SASL/TLS, `retries`, `stopOnReject`) |
| Boot application metadata + JVM/OS diagnostics | consumer | explain where the Kafka backlog shows up in memory (queue size vs offset gap) |

---

## 2. Architecture and topic inventory

```
  POST /lab/orders                KAFKA  order-kafka2 · localhost:9092              GET /lab/events
     lab-producer  ──push──►  lab-orders            (String, key = customerId)  ──►   lab-consumer
                  ──push──►  lab-orders-avro       (AVRO + schema registry)     ──►
                  ──push──►  lab-ack-a + lab-ack-b (one Kafka transaction, all-or-nothing)
                  ──push──►  lab-pending ──(Kafka Streams: stateless + stateful)──► lab-accepted
                               lab-rejected  ◄──(error handler / quarantine lane)──┘
                               __consumer_offsets · _schemas        (broker-owned: read, never write)
```

| Topic | Producer app | Consumer app | Purpose |
|---|---|---|---|
| `lab-orders` | producer | consumer | basics: push/pop, String codec, key = `customerId`, partition math |
| `lab-orders-avro` | producer | consumer | AVRO, schema registry, **schema evolution** (reuse `OrderEvent.avsc` semantics: `discount`/`phoneNumber` added with defaults) |
| `lab-ack-a`, `lab-ack-b` | producer | consumer | **atomic multi-topic** push/pop via transaction manager |
| `lab-pending` → `lab-accepted` | producer (Kafka Streams) | — | Streams: stateless operator + **stateful** per-customer running counter |
| `lab-rejected` | consumer (error handler) | — | **quarantine lane**: raw payload + failure reason + partition/offset in headers |
| `lab-retry` | producer | consumer | bounded retry + exponential backoff via failure injection |
| `lab-rebalance` | producer | consumer | rebalance/seek behaviour with two competing listeners |

Consumer group ids: `lab-consumer`, `lab-competitor` (for the two-listener race), `lab-txn` (transactional lane).

**Why not the existing `order-*` topics:** the sibling project already consumes them with its own groups on this same broker; sharing them would mix two consumer groups' offsets and make the "what happened to the backlog" lesson unanswerable. Keep them untouched, and list them once in `GET /lab/diagnostics` so the contrast is visible.

---

## 3. Deliverables — `lab-producer` (push side)

### 3.1 Basics (each must be observable, not just coded)

1. **Push, don't block**: async `KafkaTemplate#send` → the HTTP reply is derived from the push result (`202` + `Location: lab-orders/partition@offset` + `X-Lab-Message-Id`), never `send(rec).get()` inside the route. Ship a `// DO-NOT-SHIP` blocking variant (the existing `Producer.java` style) as an explicitly labelled learning artifact.
2. **Key → partition**: key = `customerId`; show `hash(key) % numPartitions` stability (same customer always same partition) and the failure mode of an unkeyed lane (order per customer is lost). Two endpoints: `/lab/orders` (keyed), `/lab/orders/unkeyed`.
3. **Header / key / payload separation**: `orderId`, `customerId`, codec version, trace id, schema version in **headers**; payload carries only the order record. README: enterprise apps route and dedup from headers *before* deserializing.
4. **Codec choice via properties**: String for `lab-orders`, AVRO registry-backed for `lab-orders-avro`, driven by `spring.kafka.producer.value-serializer` / `spring.kafka.producer.properties[...]` — no serializer hardcoded.
5. **Topic creation on startup**: `NewTopic` beans (partition counts: `lab-orders` = 3 so the key math is visible; others = 1) and the "topic exists → bean ignored" rule.
6. **Producer flow control** (through `spring.kafka.producer.properties[...]`): `acks`, `queue-size`, `block-longer`/`max.blocking`, linger/batch, and a deliberate **fast-producer / slow-consumer** run: what the throttle/queue settings do, why they are mandatory in enterprise (a producer that outruns the consumer must apply back-pressure, not OOM).
7. **Sync vs async ack**, two beans + one toggle property, app logs which one is active at startup.
8. **Flush on shutdown**: graceful shutdown drains the queue before exit (no silent loss) — visible as "flushed N" in the log.
9. **Idempotency**: `orderId` as key + documented **replay window**; separate the two guarantees ("don't lose" = `acks=all`, "don't do twice" = correlation id + replay window).
10. **Health & broker truth**: health endpoint = "can I reach the broker"; a `GET /lab/diagnostics` bean asks the broker with `spring.kafka.admin.*` (list topics, partitions, per-partition next offset).

### 3.2 Advanced (producer half)

11. **Correlation/trace id** in headers → end-to-end id so a downstream consumer failure traces back to the originating HTTP request.
12. **Back-pressure**: bounded queue → HTTP **429 + `Retry-After`**, plus a metrics counter for rejected pushes.
13. **Push-time logging**: log metadata when the push **completes** (async `.done`), not when the request arrives.
14. **Atomic multi-topic push**: transaction manager (`spring.kafka.producer.transaction-id-prefix` → `KafkaTransactionManager` bean) across `lab-ack-a` + `lab-ack-b`; forced side-effect failure ⇒ nothing lands (all-or-nothing).
15. **PoS endpoints per lane** with metrics + audit trail: `POST /lab/orders`, `/lab/orders/unkeyed`, `/lab/orders/avro`, `/lab/orders/transactional`, `POST /lab/throttle`.
16. **Profiles**: `dev` (this box) vs `prod` (documented `application-prod.properties`: multi-host, TLS/SASL, retries, `stopOnReject`) — and a paragraph on what changes when the broker becomes a real cluster.

---

## 4. Deliverables — `lab-consumer` (pull side)

### 4.1 Basics

1. **Pop non-stop, forever**: `@KafkaListener` beans; the JVM never exits after draining the backlog; a cold start with existing messages must process them.
2. **Backlog handling**: (a) cold-start backlog, (b) steady inflow while processing is slower than production — show the offset gap and where it lives in memory.
3. **Ack strategies demonstrated separately**: (i) ack immediately, (ii) ack after processing, (iii) batch ack at a checkpoint boundary — README states the loss-vs-duplication trade-off for each (`ack` = "don't lose", never "fully processed").
4. **Sync vs async ack** toggle, plus what the ack means on the pull side (offset commit to `__consumer_offsets`).
5. **Two listeners, one topic, one group**: `lab-consumer` + `lab-competitor` on `lab-rebalance` → each message processed exactly once total; offsets shared. Show the same topic with two different group ids = each group keeps its own offsets.
6. **Filter strategy bean**, header-based (accept/reject) → filtered-out messages **are** acked (skipped on purpose, not left for later); contrast with the quarantine lane.
7. **Offset/partition truth**: `GET /lab/diagnostics` → per partition: partition-offset for our group + next offset (Spring Boot admin bean + properties).
8. **Per-message lifecycle log**: `partition@offset → key → headers → payload → decision (accept|skip|reject|retry)`.
9. **PoS endpoints**: `GET /lab/events` (recently popped payloads), `GET /lab/metrics`, health; and a `GET /lab/backlog` that reports "messages in flight" (in-process queue) — the two different "where is my data" questions.

### 4.2 Advanced

10. **Bounded retry + exponential backoff** (`CommonErrorHandler` bean, retry knobs through `spring.kafka.consumer.properties[...]`): `POST /lab/inject-failures` (percentage) makes processing throw; observe the retry cap, then the message goes to `lab-rejected` (never swallowed by a `catch`).
11. **After-rollback processor**: two modes through one property — rollback then retry on the next poll cycle, vs discard; show offsets before/after.
12. **Transactional pop**: `KafkaTransactionManager` bean + `ChainedKafkaTransactionManager` marked `@Primary`: acking the message and pushing side-effects are **atomic** — a forced side-effect failure leaves the source offset unchanged (rollback visible in `/lab/diagnostics`).
13. **Graceful shutdown on the pull side**: `SIGTERM` → stop pulling, commit, exit 0; after restart the backlog is intact and already-acked segments are not re-processed (state the exact window and why).
14. **Rebalance + seek** (`ConsumerAwareRebalanceListener`, `GROUP_INITIAL_REBALANCE_DELAY_MS=0` on the box): close one listener, watch the survivor re-read from the offsets; document the loss window with `acks=0`-style ack-before-processed on a *multi*-broker (labelled concept-only, RF is 1 here).
15. **Quarantine lane vs filter**: `lab-rejected` payload keeps raw bytes + reason + source partition/offset in headers, so a human can re-drive it (enterprise: a queue you can inspect, not a lost stack trace).
16. **Idempotent processing**: keyed ledger + `orderId` dedup so a crash-retry doesn't double-count the same order; README makes "ack = don't lose" vs "process = do once" explicit.
17. **Kafka Streams** (`@EnableKafkaStreams`, `StreamsBuilder` bean, `spring.kafka.streams.application-id`, `streams.auto-startup`): stateless `.map`/`.filter` → `lab-accepted`, **stateful** per-customer running count (windowed/reduce), and a paragraph on what Streams is in enterprise terms (an in-process, stateful replay engine over the same wire format).
18. **Metrics**: per-listener throughput/timing through Boot metrics (`KafkaListenerObservationConvention` bean) + Micrometer → visible via Actuator.
19. **Hot reload while flowing**: swap a filter bean / serializer without draining (Boot hot reload / DevTools); if JDK-25 tooling blocks it, degrade to "restart without drain" and record why (see D-9).
20. **Broker-owned state, read-only**: inspect `__consumer_offsets` (compacted) and `_schemas` (AVRO) through the admin bean — the two topics you never write to in enterprise code but must be able to read during an incident.
21. **Diagnostics & tuning**: JVM/OS + Boot metadata; explain how the Kafka backlog appears in memory (in-flight queue vs offset gap) and which Spring Boot property maps to each.

### 4.3 Concept-only on this box (label, never fake as demonstrated)

Multi-broker replication (`acks=all` + `request.timeout` + ISR loss), **TLS/SASL**, Kafka 4 **data plane / ACLs**, **Zookeeper vs KRaft control plane**, cluster sizing/partition count tuning, **record/segment replay & retention** tooling, **schema propagation across services**. Each gets `docs/enterprise/<name>.md` written against *this* codebase (with the exact property block to flip in a real deployment), and a commented `application-prod.properties`.

---

## 5. Enterprise mapping (the "why it matters" layer)

| Concept in the pair | Enterprise meaning |
|---|---|
| key = customer → stable partition | per-entity ordering guarantees (ledger, delivery, payments) |
| `acks=all` / broker confirms before ack | don't-lose-data contract; the only honest way to tell the customer "accepted" |
| async push + PoS 202 + Location | offload: customer waits ms, work continues in the JVM |
| bounded queue + 429/Retry-After | back-pressure to the customer and upstream services |
| dedup key + replay window | idempotency after crash/retry |
| consumer group, one owner per message | single ownership of an order across services |
| ack mode dial | the loss-vs-duplication trade-off chosen at the data edge |
| filter + quarantine lane | routing decisions before deserialization; a re-drivable queue |
| error handler + backoff + rollback | bounded retry, then human inspection — never a swallowed catch |
| transaction across topics | all-or-nothing side effects across services ("debit ledger, credit queue") |
| graceful shutdown | no loss / no double-processing on deploy and crash |
| metrics + audit + exchange record | the incident trail: pushed, acked, which lane is stuck |
| Kafka Streams (stateless + stateful) | stateful replay/transformation of the same wire format |
| hot reload | patch logic without draining the backlog |
| `_schemas` + schema evolution | add a field (with default) without breaking existing readers |
| single broker here vs cluster | everything replication/ACL/TLS-related is documented, not simulated |

Each row expands to one paragraph in `docs/enterprise/README.md`, with the property block that would change in a real deployment.

---

## 6. Testing plan (runnable, not aspirational)

### 6.1 Smoke — against the **existing** `order-kafka2`

```bash
# 0) readiness by polling (broker may have restarted; policy is `always`)
docker exec order-kafka2 sh -c 'true' 2>/dev/null || docker start order-kafka2
until docker logs --tail 400 order-kafka2 2>&1 | grep -q "Successfully registered broker"; do sleep 2; done

# 1) push lanes (producer :8091)
curl -si -XPOST localhost:8091/lab/orders   -H 'content-type: application/json' \
  -d '{"orderId":"o-1","customerId":"cust-0","amount":42.0}'
curl -si    localhost:8091/lab/orders/unkeyed -d '{"orderId":"o-2","customerId":"cust-0","amount":1.0}'
curl -si    localhost:8091/lab/orders/avro    -d '{"orderId":"o-3","customerId":"cust-1","amount":10.0,"discount":0.1,"phoneNumber":"988-0000"}'
curl -si -XPOST localhost:8091/lab/orders/transactional -d '{"orderId":"o-4","customerId":"cust-1","amount":7.0}'
curl -si -XPOST localhost:8091/lab/throttle  -d '{"count":2000,"customerId":"cust-2"}'

# 2) pop side (consumer :8092) — must be popping non-stop
curl -s localhost:8092/lab/events | head
curl -s localhost:8092/lab/diagnostics | python3 -m json.tool   # per partition: our group offset + next offset
curl -s localhost:8092/lab/metrics

# 3) advanced lanes
curl -si -XPOST localhost:8092/lab/inject-failures -d '{"percentage":30}'
curl -s localhost:8092/lab/rejected | python3 -m json.tool       # quarantine: raw + reason + partition@offset
curl -s localhost:8091/lab/accepted                              # Streams lane result
kill -TERM <consumer-pid>; docker logs --tail 40 order-kafka2     # backlog still present after clean exit
```

Each box below must be **observed in the logs/endpoints**, and pasted into `docs/experiments.md`:

- [ ] keyed lane: same `customerId` → one partition; unkeyed lane → spread (perceive lost ordering)
- [ ] next-offset grows monotonically per partition; group-offset moves only after ack
- [ ] cold-start backlog processed; restart after SIGTERM: no loss (ack-after-processed) and the documented duplication window
- [ ] ack-mode dial: three runs (immediate / after-processing / batch) with their loss windows stated
- [ ] two listeners, one group: total processing count = message count (no double processing); two groups on one topic: independent offsets
- [ ] filter lane: rejected-by-filter messages **are** acked (not re-read)
- [ ] failure injection: retries capped with backoff, then `lab-rejected` carries raw payload + reason + source `partition@offset`
- [ ] transaction lane: forced side-effect failure → neither `lab-ack-a` nor `lab-ack-b` advanced (rollback)
- [ ] rollback processor both modes; offsets before/after recorded
- [ ] AVRO lane: `_schemas` version bumps on the field-addition schema; old reader still reads (defaults)
- [ ] Streams lane: `lab-accepted` reflects the stateless + stateful outputs
- [ ] metrics/health/audit present on both apps; per-listener observation visible
- [ ] `SIGTERM` on producer: queue flushed before exit
- [ ] no new Kafka container created by any command in the run book (regression check: `docker ps -a` identical before/after)

### 6.2 Unit / test level (Spring Boot test + Testcontainers)

- `@EmbeddedKafka` (KRaft variant, 1–3 partitions, RF 1) — the CI harness, mirroring the sibling project's seam; docker broker only for §6.1.
- Spring Boot bean-graph tests: `assertThat(properties).containEntry("spring.kafka.bootstrap-servers", …)`, `@Bean` resolution tests for every Kafka bean, `@KafkaListener` bean unit tests against a `KafkaTemplate` bean, Boot **Web** endpoint tests (PoS 202/Location/429), Boot metrics/health assertions.
- Property-level tests for every decision: filter accept/reject, error-handler retry counts, quarantine payload shape, KTM rollback, Streams operator outputs, header/payload split.

### 6.3 Learning-lab harness

`docs/experiments.md`: for each concept — *run command → what you should see → why it matters in enterprise* — and reproducing the aha-moments of `~/IdeaProjects/kafka-02` inside the new pair (partitioning by key, offsets, ack modes, rebalance re-read, retry/backoff, rollback, transaction rollback, schema evolution).

---

## 7. Repo layout (target; coexisting with the sibling project)

```
~/DEV-WORK/Kafka
├── README.md                       # existing 3-service run book (keep) + link to the new pair's run book
├── docker-compose.yml              # topology doc only — MUST NOT be started (§0 rule 2)
├── openspec/changes/kafka-lab-pair/  # proposal.md, design.md, tasks.md, specs/*.md (repo convention)
├── docs/                           # labs: experiments.md · enterprise/*.md · glossary.md
├── lab-producer/  ├── pom.xml (Boot 4.1.2, starter-kafka, starter-web, test)
│                  ├── src/main/resources/application.properties      # spring.kafka.bootstrap-servers=localhost:9092, NewTopic beans, …
│                  ├── src/main/resources/application-prod.properties # commented TLS/SASL/multi-host block (§4.3)
│                  └── src/main/java/com/example/lab/producer/        # KafkaTemplate/NewTopic beans, Web PoS endpoints
└── lab-consumer/  ├── pom.xml (+ spring-boot-metrics / micrometer, spring-kafka-test)
                   ├── src/main/resources/application.properties      # consumer.group-id=lab-consumer, filter/eh/rollback/ktm beans by properties
                   └── src/main/java/com/example/lab/consumer/        # @KafkaListener beans, error handler, rollback, KTM, Streams, diagnostics
```

Ports: `lab-producer` **8091**, `lab-consumer` **8092**, Kafka **9092**. Existing 8081/8082/8083 stay assigned to the sibling project.

---

## 8. Acceptance criteria

1. Two Spring Boot 4.1.2 applications, independently runnable, `mvn clean test` green in each.
2. Consumer pops continuously; JVM alive after the backlog is drained.
3. **Zero** Kafka client construction through hand-built client classes — every client comes from Spring Boot beans + `spring.kafka.*` properties.
4. **No blocking `send().get()` in a request path** (the one blocking variant exists only as a labelled `DO-NOT-SHIP` learning artifact).
5. Every §3.1 (1–10) and §4.1 (1–9) basics item demonstrable in one run, captured in `docs/experiments.md` with observed output.
6. Every §3.2 (11–16) and §4.2 (10–21) advanced item implemented; §4.3 items implemented as documented property blocks + write-ups, explicitly labelled "not demonstrated on this box".
7. `SIGTERM` on either app → clean exit, backlog preserved, offsets as documented.
8. Health + metrics + audit trail on both apps; listener observation convention registered on the consumer.
9. Tests: unit (`@EmbeddedKafka`) + §6.1 smoke checklist green against **`order-kafka2`**; regression check proves no additional Kafka container was created.
10. All failure paths land in `lab-rejected`; no swallowed `catch`.
11. Namespaces clean against the sibling project: `lab-*` topics, `lab-*` group ids, ports 8091/8092; existing `order-*` topics and 808x ports untouched.
12. `docs/enterprise/` maps every concept row of §5 to real code in this pair.

---

## 9. Decisions to confirm (recommended answers)

| # | Question | Recommendation |
|---|---|---|
| D-1 | AVRO lane needs a schema registry — none is up. Options: (a) start `cp-schema-registry:7.9.0` pointing at `localhost:9092`, (b) inline-schema AVRO (no registry), (c) drop AVRO | **(a)** — reuse `~/IdeaProjects/kafka-02`'s registry container but re-point `SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS` to `PLAINTEXT://localhost:9092`; it teaches `_schemas` + schema evolution, which is the enterprise lesson. Keep (b) as fallback. |
| D-2 | Boot version | **4.1.2** (Kafka module requires ≥ 4.0; matches sibling project). Confirm before Phase 3. |
| D-3 | Broker | **reuse `order-kafka2`**, never recreate; the repo's compose file stays documentation-only. |
| D-4 | Ports / topics / group ids | **8091 / 8092**, `lab-*` topics, `lab-*` groups (avoid the sibling project). Confirm. |
| D-5 | HTTP layer | Boot **Web**/WebMVC (as the sibling project) + Boot Webflux for the async push lane on the producer only; consumer stays Webflux-neutral. Confirm. |
| D-6 | Persistence inside these two apps | **No** (Postgres stays with the sibling project). Confirm — a JDBC lane would blur the Kafka lesson. |
| D-7 | Failure injection | one property-driven `POST /lab/inject-failures` (percentage) driving retry/rollback/quarantine. Confirm. |
| D-8 | Partitions | `lab-orders` = 3 (key math visible), all other lanes = 1 (matches RF-1 broker). Confirm. |
| D-9 | Hot reload on JDK 25 | attempt Boot DevTools/hot reload; if blocked, degrade to "restart without drain" and record the reason (see §4.2 item 19). |
| D-10 | Two apps or also a third for Streams? | **Two** — Streams lane lives inside `lab-producer` (§2 row), keeping the pair as requested. |
| D-11 | Should this pair be OpenSpec'd like the sibling? | **Yes** — `openspec/changes/kafka-lab-pair/{proposal,design,tasks,specs/*}` following the repo's existing convention; I'll expand this SPEC into those files on your go-ahead. |

---

## 10. Phase plan (this repo's workflow)

0 preflight (container reuse check, versions) → 1 requirement analysis (done: §0/§1) → 2 clarification (ask D-1…D-11, with these recommendations) → 3 OpenSpec `kafka-lab-pair` → 4 review gate → 5 TDD implementation (test-first, red-green-refactor; beans first, then endpoints) → 6 build (`mvn clean test` per app) → 7 self-review → 8 verification + §6.1 smoke + final report.
