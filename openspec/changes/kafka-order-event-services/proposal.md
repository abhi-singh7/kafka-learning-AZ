# Proposal: Kafka Order Event Services (pure Spring Boot 4 + Spring Kafka)

## Why (Motivation)

The user wants a working, buildable demonstration of **event-driven architecture over Apache Kafka**: one
service turns a REST request into Kafka **events**, and two **independent** services handle the same event
stream in completely different ways (mock mail vs Postgres persistence). The point is not throughput or
business value — it is that the *only* coupling between the three services is the Kafka topic, so the
Kafka mechanics (topics, partitions, keys, codecs, ACK/NACK, poison messages, replay) are visible, testable,
and reproducible on the broker that already runs on this machine.

The same requirement already exists on this machine in Quarkus + Eclipse Camel form
(`../../Quarkus/{order-api,order-persistence}`, topic `orders`). The user explicitly asked for **pure
Spring Boot** instead (Phase 0, answer 5), so this project re-implements the idea on the Spring Boot 4 stack
rather than extending or reusing that project.

## What Changes

### Added
- Three **independent** Maven projects in one git repo — `order-service/` (REST → Kafka producer, `:8081`),
  `order-persistence-service/` (Kafka → Postgres consumer, `:8082`), `email-service/` (Kafka → mock mail,
  `:8083`) — with **no shared code module** between them.
- Four **new Kafka topics** on the existing broker (`apache/kafka:4.0.0` @ `localhost:9092`):
  `order-created` (produced by `order-service`, consumed by both consumers), `order-email-sent` and
  `order-persisted` (per-consumer ack events), `order-failed` (poison-pilot sink).
- A **new Postgres table** `order_line_events` in the existing `ordersdb` (credentials `postgres:postgres`),
  leaving the sibling's `order_lines` untouched.
- HTTP surface: `POST /orders` (order-service), `GET /orders/{orderId}/lines` (persistence),
  `GET /emails` + `GET /orders/{orderId}/emails` (mock mail), `GET /health` on all three, `GET /orders/failed`
  (poison diagnostics, see design).
- A **hermetic test suite per service** driven by `mvn clean test` using `spring-kafka-test`'s
  `EmbeddedKafkaBroker`, so the build gate does not depend on the Docker broker.
- Repo-level documentation and setup artifacts: `README.md` (curl + `kafka-topics.sh` + `psql` walkthrough),
  `docker-compose.yml` for the broker/topology, and `.gitignore`.

### Modified
- Nothing — the repo baseline is an **empty tree** (0 commits, verified in Phase 0), so every file is new.

### Removed
- Nothing.

### Explicitly out of scope (agreed, not silent)
- No Quarkus, no Eclipse Camel, no `camel-quarkus-kafka` (user decision, Phase 0 answer 5).
- No Kafka Streams, no Avro/schema-registry codec (A6, B7 — see Decisions).
- No Micrometer/Prometheus metrics (C12), no frontend/UI (E — N/A).
- No SMTP (mock mail only, Phase 1 confirmation 4).

## Impact Assessment

Greenfield repo: there is no existing code, test, or config to touch. The only pre-existing state that must
stay untouched is **outside the repo** and is load-bearing for the demo:

| External state | Measured fact | How this change interacts |
|---|---|---|
| Kafka broker | container `order-kafka2`, image `apache/kafka:4.0.0`, `0.0.0.0:9092->9092/tcp`, KRaft leader partition 1; existing topic: `orders` (`PartitionCount: 1`, `ReplicationFactor: 1`) | we add 4 new topics, never write to `orders` |
| Postgres | native **15.19** (`/usr/lib/postgresql/15`), `127.0.0.1:5432`, user `postgres`/password `postgres`; DB `ordersdb` contains exactly one app relation: `order_lines` | we add `order_line_events` in `ordersdb`; `order_lines` must remain unchanged (regression check) |
| HTTP ports | `ss -ltn` occupied: `8080`, `9000`, `9092`, `5432`; `8081`, `8082`, `8083` free | our three services take 8081/8082/8083 |
| Docker CLI | daemon reachable only via `DOCKER_SOCKET=/home/abhi/.docker/desktop/backend.sock` (default `desktop-linux` context socket missing) | README documents that; test gate uses an in-process broker instead |

Reference-only (conventions source, not a dependency): `../../Quarkus/README.md`,
`order-api/src/main/java/com/example/orders/api/Order.java`, `OrderItemDto.java`,
`order-persistence/src/main/java/com/example/orders/persistence/OrderLine.java`,
`.../resources/application.properties`.

## Environment Baseline

- **Trunk branch / baseline commit:** `main` @ **no commits** — baseline is the **empty tree**
  `4b825dc642cb6eb9a060e54bf8d69288fbee4904` (`git mktree </dev/null`; `git rev-list --all --count` = `0`).
  No initial commit was created (not instructed). The change set is therefore computed from
  `git status --porcelain --untracked-files=all`, and any later diff uses the empty-tree baseline.
- `git init -b main .` was run **at the user's explicit instruction** (Phase 0 answer 1). No commit,
  stash, branch switch, rebase, or push has been performed by the agent.
- **Project kind:** Java 17 + Maven (greenfield). No `./mvnw` wrapper exists locally.
- **Declared vs installed:**
  - Declared: **Spring Boot `4.1.2`** (user accepted recommendation, Q-0.2), `<source>17</source>` /
    `<target>17</target>`, **Spring Kafka `4.1.2`** (Q-0.3).
  - Installed: Maven **3.9.12**, **OpenJDK 25.0.5** (`java -version` prints to STDERR), Node 26.8.2/npm 11.19.1
    (not used), Postgres client **15.19**.
- **Toolchain check verdict:** **no mismatch** — Spring Boot 4.1.2 "requires at least Java 17 and is
  compatible with versions up to and including Java 27" (system requirements page cited below), so OpenJDK 25
  is supported and pinning source/target at 17 costs nothing.
- **Mismatches accepted by user:** `4` → **`4.1.2`** pinned (Q-0.2: Maven Central shows `4.1.2` as newest GA;
  `4.2.0` exists only as `-M1/-M2/-RC1/-RC2`, and release candidates are not a stable dependency).
- **Branch choice:** on trunk `main` (the only branch; no remote, no upstream).
- **Test runner:** `mvn clean test` per project (F16); nothing pre-existing to reuse.
- **Research tooling caveat (carried, not swallowed):** the configured **Firecrawl MCP** server
  (`FIRECRAWL_API_URL=http://localhost:3002`) is unreachable — `firecrawl_search`/`firecrawl_scrape`
  failed with `ECONNRESET`/`ECONNREFUSED`, and `curl`/`urllib` to `:3002` get `Connection reset by peer`
  while the `firecrawl-api-1` container is `Up`. Research in Phases 0/2 therefore ran through the
  ddg-search MCP and direct `docs.spring.io` fetches; every claim in `Sources Consulted` was obtained
  that way.

## Assumptions Made

Every item below was **not** stated by the user; each must be reviewed at Phase 4.

1. `orderId` is a **UUID** generated by `order-service`; the response returns `201 Created` with
   `{orderId, lineCount, kafkaKey, topic}` (B8, B3 — shape of our own invention).
2. Message key is `orderId` and every topic has **1 partition / replication factor 1**, matching the
   measured KRaft broker settings.
3. Message field names are exactly: `schemaVersion`, `orderId`, `customerId`, `sku`, `qty`, `price`,
   `lineIndex`, `messageId`, `createdAt` (B7 — field set is our choice; the *domain* vocabulary
   `orderId/customer/sku/qty/price` came from the sibling's `Order.java`/`OrderItemDto.java`).
4. New table `order_line_events` columns: `id, orderId, customerId, sku, qty, price, lineIndex, messageId,
   createdAt`; PK on `(orderId, sku)` for idempotency (A2), table name ours (D15).
5. `createdAt` and `messageId` are produced by the producing service (not read from Kafka headers), so a
   consumer can detect replay without relying on broker metadata.
6. Config keys in `application.properties`: `kafka.server`, `kafka.topic.*`, `database.host/port/name/user/
   password`, and Boot 4 HTTP port properties — names follow the sibling's properties style
   (`camel.kafka.brokers=localhost:9092`, `quarkus.datasource.*`) translated to Spring Boot equivalents;
   the Spring Boot property names themselves are verified at Phase 6, not assumed.
7. `email-service` holds mock mail **in memory** (ordered list) and logs one line per mail; it writes nothing
   to Postgres (D15) and never contacts SMTP (Phase 1 confirmation 4).
8. Boot 4 module set used per service is taken from modules **verified present at `4.1.1` in the local
   `~/.m2` cache** (`spring-boot`, `spring-boot-autoconfigure`, `spring-boot-starter`, `spring-boot-starter-web`,
   `spring-boot-starter-jackson`, `spring-boot-starter-data-jpa`, `spring-boot-starter-jdbc`,
   `spring-boot-starter-test`, `spring-boot-test`, `spring-boot-test-autoconfigure`,
   `spring-boot-starter-reactor-netty`); exact `4.1.2` resolution is proven by the Phase 6 build.
9. `spring-kafka` + `spring-kafka-test` are added at `4.1.2` directly (no Boot "kafka starter" exists in the
   cache at 4.1.1; the 4.1 quick tour says the dependency is "Spring for Apache Kafka").
10. Root-level artifacts (`README.md`, `docker-compose.yml`, `.gitignore`) are **documentation/setup
    artifacts with no test criterion** — they are not `TDD-OFF` opt-outs (the user declared none) and Phase 8
    reports them as such.
11. Topic creation is an explicit `docker exec … kafka-topics.sh --create --if-not-exists` step in the README;
    services fail fast if a required topic is absent (D14).
12. `GET /orders/failed` returns poison payloads received by consumers from `order-failed` — an addition I
    chose for observability of the A1 concept; if unwanted, drop it and keep the log line only.

## Decisions From Clarification

| # | Question | Chosen by | Decision | Grounding |
|---|---|---|---|---|
| Q-0.1 | Git setup for an empty non-repo dir | user (own instruction) | agent runs `git init` (user's instruction); trunk `main`, no commits | measured `git init -b main` output |
| Q-0.2 | Boot minor version + source/target | user (kept rec.) | **4.1.2**, source/target **17** | Maven Central probe + system-requirements page |
| Q-0.3 | Kafka client library | user (kept rec.) | **spring-kafka 4.1.2** only | spring.io 4.1 GA announcement + 4.1 quick-tour API |
| Q-0.4 | Kafka endpoint | user (asked to check docker) | broker `order-kafka2`, `apache/kafka:4.0.0`, **`localhost:9092`**, existing topic `orders` | `docker ps`/`kafka-topics.sh --list --bootstrap-server` |
| Q-0.5 | Postgres credentials | user | **`postgres` / `postgres`** (DB `ordersdb` holds `order_lines`) | `psql` run, rc=0; `\dt` listing |
| Q-0.6 | Package layout | user (kept rec.) | 3 sibling Maven projects, one repo, no shared module | sibling `Quarkus/{order-api,order-persistence}` pattern |
| Q-0.7 | New topic names | user (kept rec.) | `order-created`, `order-email-sent`, `order-persisted`, `order-failed` | recommendation on accepted Q-0.4 |
| P1-1 | Ports | user confirmed | 8081 order / 8082 persistence / 8083 email | `ss -ltn` free |
| P1-2 | DB strategy | user confirmed | reuse `ordersdb`, new tables | `\dt` |
| P1-3 | One message per line item | user confirmed | split | sibling README semantics |
| P1-4 | Mock mail = no SMTP | user confirmed | memory + log + HTTP | — |
| A1 | Poison handling | user (kept rec.) | publish to `order-failed`, then ACK | Kafka ACK contract |
| A2 | Replay tolerance | user (kept rec.) | idempotent PK `(orderId, sku)` | Kafka is at-least-once; sibling table is not idempotent (deliberate improvement) |
| A3 | `orderId` ownership | user (kept rec.) | server-generated UUID, client value ignored | our own contract; sibling's client-supplied shape deliberately not copied |
| A4 | Consumer ack topics | user (kept rec.) | `order-email-sent`, `order-persisted` published | requirement's "handled by two different services" must be visible on broker |
| A5 | Business failure → NACK | user (kept rec.) | NACK on infrastructure failure; decodable-invalid → `order-failed` | 4.1 API surface (quick-tour) |
| A6 | Kafka Streams | user (kept rec.) | **no** | not part of spring-kafka; adds a dependency axis |
| B7 | Message schema + key | user (kept rec.) | JSON w/ `schemaVersion`, key `orderId`, Jackson 3 | Boot 4 JSON lib; ankurm migration note (secondary → medium) |
| B8 | `POST /orders` shapes | user (kept rec.) | request w/o `orderId`; `201` + `{orderId,lineCount,kafkaKey,topic}` | sibling domain minus client id |
| B9 | Consumer query endpoints | user (kept rec.) | `GET /orders/{orderId}/lines`, `GET /emails`, `GET /orders/{orderId}/emails`, `GET /health` | sibling route path convention |
| B10 | Validation failure | user (kept rec.) | `422` + field errors, **no publish** | stream must carry well-formed events only |
| B11 | Config channel | user (kept rec.) | `application.properties` + env overrides | Boot 4 docs (both formats supported) |
| C12 | Metrics module | user (kept rec.) | **none** | Rule 4 scope; sibling added it for its own reasons |
| C13 | SIGINT behaviour | user (kept rec.) | graceful shutdown, unsubscribe + `destroy()`, flush + count log | 4.x surface includes `destroy()` |
| D14 | Who creates topics | user (kept rec.) | explicit `kafka-topics.sh --create` command; services fail fast | 4.1 client surface has no create-topic API |
| D15 | Table naming | user (kept rec.) | `order_line_events` in `ordersdb`; email service writes no DB | `\dt` shows `order_lines` only; keeps DB/email coupling lesson |
| E | UI/UX | n/a | no frontend in this repo | Phase 0/1: no `package.json`/`angular.json` |
| F16 | Build/test gate command | user (kept rec.) | `mvn clean test` per project, **no** aggregator pom | Q-0.6 layout; Maven 3.9.12, no `./mvnw` |
| F17 | Kafka in tests | user (kept rec.) | `EmbeddedKafkaBroker` in test gate + manual e2e recipe in README | testing chapter names `EmbeddedKafkaBroker`, `EmbeddedKafkaKraftBroker`, `EmbeddedKafkaHolder` |
| F18 | Test-first mode | user (kept rec.) | TDD for every task, **no opt-outs** | workflow default |

## Open Questions Resolved

- "Kafka is already running locally in docker" → verified: `order-kafka2` / `apache/kafka:4.0.0` / `:9092`;
  docker CLI needs `DOCKER_SOCKET=/home/abhi/.docker/desktop/backend.sock` on this machine.
- "cover all the important concepts of kafka" → pinned to an explicit, testable set (design.md → "Kafka
  concepts covered") instead of an open-ended list; the heavy options (Streams, Avro, metrics) were declined
  via A6/B7/C12.
- "three applications" → three sibling Maven projects with no shared code (Q-0.6), which is what keeps the
  Kafka-only coupling honest.
- Postgres: **native 15.19, not Docker** as the sibling README assumed; credentials `postgres:postgres`.

## Sources Consulted

- Spring Boot system requirements — "Spring Boot 4.1.2 requires at least Java 17 and is compatible with
  versions up to and including Java 27": https://docs.spring.io/spring-boot/system-requirements.html
- Spring Boot 4.0 Migration Guide — "Spring Boot 4.0 requires Java 17 or later… based on Jakarta EE 11 and
  requires a Servlet 6.1 baseline"; config lives in `application.properties`/`application.yml`:
  https://github.com/spring-projects/spring-boot/wiki/Spring-Boot-4.0-Migration-Guide
- Spring for Apache Kafka 4.1.0 GA — "first GA release of the 4.1.x generation":
  https://spring.io/blog/2026-06-09/spring-kafka-4
- Spring Kafka 4.1 Quick Tour (API surface for the pinned version: `KafkaListener`, `KafkaTemplate`,
  `AnnotationConfigApplicationContext`; "Spring for Apache Kafka" as the dependency) — fetched live from
  https://docs.spring.io/spring-kafka/reference/4.1/quick-tour.html (docs root for 4.1 confirmed HTTP 200)
- Spring Kafka Testing chapter (test-side classes `EmbeddedKafkaBroker`, `EmbeddedKafkaKraftBroker`,
  `EmbeddedKafkaHolder`) — fetched live from https://docs.spring.io/spring-kafka/reference/testing.html
- Boot 4 config formats — "binds external properties from `application.properties` (or YAML files…)"
  https://docs.spring.io/spring-boot/how-to/properties-and-configuration.html
- Jackson 3 as the Boot-4-era JSON/serializer base for spring-kafka 4.1 (secondary source, confidence medium):
  https://ankurm.com/spring-kafka-3-to-4-migration-guide/
- `spring-boot-data-jpa` exists in the 4.1.x line: https://central.sonatype.com/artifact/org.springframework.boot/spring-boot-data-jpa/4.1.0
- Maven Central directory listings (probed with `curl`, newest GA / pre-release status for `spring-boot`,
  `spring-kafka`): https://repo1.maven.org/maven2/org/springframework/boot/spring-boot/
- Local `~/.m2` cache inventory (Boot 4.1.1 module vocabulary + `spring-kafka 3.2.4` present, i.e. older
  generation): measured `ls`/`find` output.
- Firecrawl MCP consulted but **failed** (no content obtained): `ECONNRESET`/`ECONNREFUSED`.
