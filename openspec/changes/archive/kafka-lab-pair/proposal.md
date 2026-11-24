# Proposal: Kafka learning pair — `lab-producer` + `lab-consumer` (Spring Boot 4.1.2 + Spring Kafka 4.1.2)

## Why (Motivation)

The user wants **two** microservices that *teach* Apache Kafka — every basic and every advanced concept
observable on one machine — while also understanding **how each concept appears in an enterprise
application**. The sibling change already in this repo (`kafka-order-event-services`, 3 services, tasks
`[x]`) demonstrates only a narrow slice of the concept list (keyed produce, one stream consumed twice,
poison sink, at-least-once/idempotency, NACK/redelivery) and explicitly **declined** Streams, AVRO,
metrics, and the ack-mode dial (its design.md "Declined on purpose": A6/B7/C12). A second, deliberately
*wider* pair is therefore the right shape: two apps whose only coupling is Kafka, with one lane per concept
so nothing cross-talks.

The learning pair is also the artifact that answers "how does Kafka work in enterprise applications": every
concept row maps to a production consequence (back-pressure, quarantine lane, transaction, shutdown contract,
schema evolution, per-listener metrics) in `docs/enterprise/`.

## What Changes

### Added
- Two **independent** Maven projects, one git repo, **no shared module** between them:
  `lab-producer/` (REST → Kafka push, `:8091`) and `lab-consumer/` (Kafka pop, non-stop, `:8092`).
- **Seven `lab-*` Kafka topics** on the **existing** broker (`order-kafka2`, `apache/kafka:4.0.0` @ `localhost:9092`):
  `lab-orders` (3 partitions, key = `customerId`), `lab-orders-avro`, `lab-ack-a` + `lab-ack-b` (transaction pair),
  `lab-pending` → `lab-accepted` (Kafka Streams lane), `lab-rejected` (quarantine sink), `lab-retry`,
  `lab-rebalance`. **`lab-*` consumer groups only** (`lab-consumer`, `lab-competitor`, `lab-txn`) — `__consumer_offsets`
  is per group, so reusing a sibling group name would silently inherit that group's saved offsets.
- A **schema-registry-backed AVRO lane** reusing the existing `cp-schema-registry:7.9.0` setup, re-pointed at
  `PLAINTEXT://localhost:9092` (decision D-1) — this is what teaches `_schemas` + schema evolution.
- HTTP surface: `POST /lab/orders`, `/lab/orders/unkeyed`, `/lab/orders/avro`, `/lab/orders/transactional`,
  `POST /lab/throttle` (producer); `GET /lab/events`, `/lab/diagnostics`, `/lab/backlog`, `/lab/metrics`,
  `/lab/rejected`, `POST /lab/inject-failures` (consumer); `GET /lab/accepted` (Streams result); `GET /health`
  on both.
- **Advanced behaviours that the sibling pair does not have**: ack-mode dial (immediate / after-processing /
  batch checkpoint), bounded retry with exponential backoff via `CommonErrorHandler`, `AfterRollbackProcessor`
  both modes, **atomic** multi-topic transaction (KTM + `ChainedKafkaTransactionManager` `@Primary`), Kafka
  Streams (stateless + **stateful** per-customer running counter), two listeners on one topic in one group,
  per-listener metrics via `KafkaListenerObservationConvention`, `SIGTERM` flush contract on both sides,
  hot-reload-with-flow, header/key/payload separation with correlation+trace ids, 429+`Retry-After` back-pressure.
- Docs artifacts: `docs/experiments.md` (run → expected observation → why), `docs/enterprise/<concept>.md`
  (one file per concept row, with the property block that changes in a real deployment), `docs/glossary.md`,
  `docs/properties-prod.md` (TLS/SASL/multi-host block, commented).

### Modified
- `README.md` (repo root): add a section pointing at this pair and its ports/topics; the existing 3-service
  run book stays intact.
- `docker-compose.yml`: **comment-only** addition recording that this pair reuses `order-kafka2` and that
  starting this compose file is forbidden (it documents a topology, it is not a launcher).

### Removed
- Nothing.

### Explicitly out of scope (agreed, not silent)
- **No new Kafka broker, no recreation of `order-kafka2`** — the container is reused; the compose file is never
  run (recreation would reset cluster state, topics, and `__consumer_offsets`).
- **No Postgres/JDBC lane in these two apps** (decision D-6): persistence stays in the sibling project; a JDBC
  lane here would blur the Kafka lesson.
- **Concept-only** on this box (single broker, RF 1, PLAINTEXT): replication/`acks=all` loss, TLS/SASL, Kafka 4
  data plane/ACLs, cluster sizing, Zookeeper-vs-KRaft control plane, retention/replay tooling. Implemented as
  `application-prod.properties` blocks + `docs/enterprise/` write-ups — **never** presented as demonstrated.
- No frontend/UI (N/A), no SMTP, no Quarkus/Camel.
- The `order-*` topics, ports 8081/8082/8083 and the sibling's OpenSpec change stay untouched.

## Impact Assessment

| External state (load-bearing, mostly outside the repo) | Measured fact | How this change interacts |
|---|---|---|
| Kafka broker | container **`order-kafka2`**, `apache/kafka:4.0.0`, **running**, `0.0.0.0:9092->9092/tcp`, KRaft single node, **restart policy `always`** | reused as-is; we only add `lab-*` topics; poll `docker logs` for `Successfully registered broker … epoch N` instead of sleeping |
| Topics already on the broker | `order-created`, `order-email-sent`, `order-persisted`, `order-failed` (+ stray `order-abc`, `order-jdbc-probe`), plus broker-owned `__consumer_offsets`, `_schemas` | untouched; listed read-only in `GET /lab/diagnostics` so the namespace contrast is visible |
| Schema registry | **nothing serving on `:8081`** (`curl` refused); a `cp-schema-registry:7.9.0` configuration exists at `~/IdeaProjects/kafka-02/docker-compose.yml` pointing at `kafka:19092` | D-1: start one registry re-pointed at `PLAINTEXT://localhost:9092`; AVRO lane degrades to inline-schema if refused |
| Ports | sibling owns 8081/8082/8083; broker 9092; also occupied per sibling README: 8080, 9000, 5432 | this pair takes **8091** (producer), **8092** (consumer); re-probe with `ss -ltn` in Phase 0 before starting |
| Sibling project in the same repo | `order-service/`, `email-service/`, `order-persistence-service/` built at Boot `4.1.2`, packages `com.example.orders.{api,email,persistence}` | untouched; regression check in Phase 8: `mvn clean test` green in all three, no source edits |
| Toolchain | JDK 25 runtime, `javac` release 21, Maven **3.9.x**, **no `./mvnw`**, Boot source/target **17** | same invocation shape as the sibling (`mvn clean test` per project, no aggregator pom) |
| Docker access | sibling README documents `DOCKER_SOCKET=/home/abhi/.docker/desktop/backend.sock` (default context socket missing) | documented in this pair's run book too; the test gate uses an **in-process** broker instead |

Reference-only (convention source, not a dependency): the sibling's OpenSpec change
`openspec/changes/kafka-order-event-services/`, `order-service/src/main/resources/application.properties`,
`email-service/src/main/resources/application.properties`, and
`~/IdeaProjects/kafka-02/src/main/java/com/example/kafka/*.java` (the aha-moments this pair must reproduce).

## Environment Baseline

- **Trunk branch / baseline:** `main`, existing commits present (sibling project committed/working tree); the
  change set for this spec is computed from `git status --porcelain --untracked-files=all` limited to
  `lab-producer/`, `lab-consumer/`, `docs/`, `openspec/changes/kafka-lab-pair/` and the two documented edits
  (`README.md`, `docker-compose.yml`).
- **Pre-flight gate (Phase 0):** broker reachable at `localhost:9092` (`order-kafka2` state `running`), ports
  8091/8092 free, Boot `4.1.2` resolvable, no `lab-*` topic present yet.
