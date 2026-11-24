# Implementation Tasks: Kafka Order Event Services

**Test-first mode:** **TDD (RED → GREEN → REFACTOR) for every task** — Java and any frontend alike.
**User-declared opt-outs (`TDD-OFF`): none.** The user accepted the TDD default for all tasks (Phase 2, F18).
Non-behavioural artifacts (repo docs, `docker-compose.yml`, `.gitignore`, topic-creation script) are **documentation/setup
artifacts with no test criterion** — they are *not* `TDD-OFF` opt-outs and Phase 8 must report them in that category.

Ordering rule: infrastructure → producer service → consumer services (simpler first: email, then persistence) →
repo artifacts → build (Phase 6) → self-review (Phase 7). Each task begins by writing a failing test from a
`specs/*.md` scenario.

## Task 0.1: [Infra] Create the three Maven skeletons and prove they compile empty
- [x] `order-service/pom.xml`, `email-service/pom.xml`, `order-persistence-service/pom.xml` — Boot `4.1.2`,
      `<source>17</source>/<target>17</target>`, packages `com.example.orders.{api,email,persistence}`
- [x] `spring-kafka` + `spring-kafka-test` at `4.1.2` in all three; `spring-boot-starter-data-jpa`/`-jdbc` only in persistence
- [x] Root `.gitignore` (`target/`, `*.iml`, `.idea/`, `openspec/` excluded only if user says so — default: tracked)
- [x] Prove: `mvn -q clean test` in each project passes with zero test classes (empty-suite baseline) and resolves `4.1.2`

## Task 0.2: [Infra] Test harness seam: `EmbeddedKafkaBroker` shared setup *per project* (no shared module)
- [x] RED: a trivial test that starts `EmbeddedKafkaBroker` (KRaft variant, 1 partition / RF 1) and asserts the
      topic list contains the service's required topic; confirm it FAILS because the harness/setup is absent, not because Kafka is broken
- [x] GREEN: minimal `TestKafkaEnvironment` helper class inside **each** project's test tree (duplicated on purpose — see design "Deliberate duplication")
- [x] Gate: same helper present in all three projects, none of them importing another project

## Task 1.1: [Test→Impl] order-service: validate the REST request before anything is published
- [x] RED: tests for `422` cases from `specs/order-service.md` — empty `items[]`, `qty <= 0`, `price < 0`,
      blank/oversized `sku`/`customerId`, `items[] > 100`, malformed JSON → assert error shape `{"errors":[{"field","reason"}]}`
- [x] GREEN: `OrderRequestDto` + validator; **no Kafka side effect on invalid input**
- [x] REFACTOR only with tests GREEN

## Task 1.2: [Test→Impl] order-service: `orderId` ownership (UUID) + accepted-order bookkeeping
- [x] RED: `POST /orders` without `orderId` → response carries a UUID `orderId`; request **with** an `orderId` →
      response carries the generated one, client value ignored (A3)
- [x] GREEN: id generation + `GET /orders` list
- [x] Confirm the 4.1.2 API for Boot-4 HTTP routes by fetching the Boot 4 web reference before writing the first route test (research rule)

## Task 1.3: [Test→Impl] order-service: split one order → N line-event messages (P1-3)
- [x] RED: given `{"customerId":"acme","items":[A,B]}` → exactly 2 produced messages, `schemaVersion="1"`,
      fields per `specs/kafka-contract.md`, `lineIndex` 0..N-1, `messageId`/`createdAt` set by producer
- [x] GREEN: mapper from order → line events (pure function; no I/O)

## Task 1.4: [Test→Impl] order-service: produce to `order-created` with **key = `orderId`**
- [x] RED: with `EmbeddedKafkaBroker`, after `POST /orders` the `order-created` topic contains N records whose
      **key equals `orderId`** and whose payload round-trips through the Jackson 3 codec
- [x] GREEN: `KafkaProducerFactory` + `KafkaTemplate` beans; single template per process
- [x] AND IF topic missing at startup → service fails fast with a readable message (D14)

## Task 1.5: [Test→Impl] order-service: `201` response contract + graceful shutdown
- [x] RED: `201` body `{orderId,lineCount,kafkaKey,topic}`; `GET /health` shape
- [x] RED: shutdown signal → consumers unsubscribed, `KafkaTemplate.destroy()` called, log line with pending count (C13)
- [x] GREEN: response mapping + `@Component`-level shutdown hook

## Task 2.1: [Test→Impl] email-service: decode + validate consumed line events (own record class)
- [x] RED: decodable payload → record; **undecodable/`schemaVersion`-foreign payload → rejection path**, not an exception out of the listener
- [x] GREEN: `OrderLineEventRecord` (email-service copy) + Jackson 3 decode

## Task 2.2: [Test→Impl] email-service: one mock mail per line event, in-memory bounded store + log
- [x] RED: consume 2 events of one order → `GET /emails` has the mail for that order, `GET /orders/{orderId}/emails` returns it,
      unknown `orderId` → `404`, no events yet → `[]`
- [x] GREEN: `MockMailSink` (bounded, default 1000) + log line per mail; **no SMTP, no Postgres**

## Task 2.3: [Test→Impl] email-service: publish `order-email-sent` ack event, key = `orderId`, then ACK (A4/A5)
- [x] RED: after handling, `order-email-sent` gains a record `{orderId,messageId,sku,qty,result,at,service}`; offset advances (ACK)
- [x] GREEN: produce ack from the listener, return ACK
- [x] AND IF mail rendering throws (bug/injected failure) → return NACK and the **same** record is redelivered on next poll

## Task 2.4: [Test→Impl] email-service: poison path → `order-failed` + raw payload + reason, then ACK (A1)
- [x] RED: publish a corrupt record into `order-created` → `order-failed` gains `{reason,rawPayload,sourceTopic,receivedAt,detectedBy}`,
      the corrupt record is **not** redelivered in a loop, and `GET /orders/failed` shows it (rawPayload truncated to 1000 chars)
- [x] GREEN: catch decode/validate failure → produce to `order-failed` → ACK

## Task 2.5: [Test→Impl] email-service: graceful shutdown flush + fail-fast topic check
- [x] RED: missing `order-email-sent` topic at startup → refuse to start; SIGINT → unsubscribe + `destroy()` + pending count logged
- [x] GREEN: startup topic probe + shutdown hook

## Task 3.1: [Infra→Test] persistence-service: `order_line_events` table created idempotently, sibling `order_lines` untouched
- [x] RED: startup creates `order_line_events` with the design's columns/guards; assert `order_lines` is **not** modified
- [x] GREEN: Spring Data 4 JPA model (`@Entity`, `PK (orderId,sku)`, `messageId` UNIQUE) + committed SQL/mapping
- [x] Config from `application.properties` w/ env overrides: `DATABASE_NAME=ordersdb`, user/password `postgres`

## Task 3.2: [Test→Impl] persistence-service: decode + validate consumed line events (own record class)
- [x] RED: as in 2.1 but in the persistence project (duplicate class — intentional)

## Task 3.3: [Test→Impl] persistence-service: persist exactly once per `messageId` (A2, at-least-once → idempotent)
- [x] RED: consume the same record twice (replay) → **exactly one** row; consume 3 records of one order → 3 rows;
      count query after a simulated restart stays exact
- [x] GREEN: insert-or-skip on the uniqueness guards, one transaction per Kafka message
- [x] AND IF JDBC failure (infrastructure) → NACK → redelivered → row appears exactly once when the DB recovers (A5)

## Task 3.4: [Test→Impl] persistence-service: publish `order-persisted` ack event (A4) + `GET /orders/{orderId}/lines` (B9)
- [x] RED: ack record on `order-persisted` with key `orderId`; `GET` returns rows for that order, `404` when none, `[]` for the collection route
- [x] GREEN: ack produce after successful insert; query endpoint from JPA

## Task 3.5: [Test→Impl] persistence-service: poison path → `order-failed` + `GET /orders/failed` (A1)
- [x] RED: corrupt payload → `order-failed` record with `detectedBy = order-persistence-service`, then ACK; no redelivery loop
- [x] GREEN: same shape as 2.4, persistence copy

## Task 3.6: [Test→Impl] persistence-service: graceful shutdown flush + fail-fast topic check
- [x] RED/GREEN as in 2.5 for this service

## Task 4.1: [Artifact] Repo `README.md` + `docker-compose.yml` + topic-creation commands
- **Documentation/setup artifact — no test criterion (not a TDD opt-out)**
- [x] `README.md`: architecture diagram, the `DOCKER_SOCKET=/home/abhi/.docker/desktop/backend.sock` note,
      `kafka-topics.sh --create --if-not-exists --bootstrap-server 127.0.0.1:9092` commands, run order,
      curl walkthrough, `psql` checks, concept-mapping table from design.md
- [x] `docker-compose.yml` describing the 4 topics/broker (documentation of topology, not used by the test gate)

## Task 6.0: [Build] Phase 6 — full build & auto-fix
- [x] `mvn clean test` per project (no aggregator pom), one first-error-at-a-time minimal fix loop, re-run to clean

## Task 7.0: [Review] Phase 7 — self code-review of the built diff (against the empty-tree baseline)
- [x] Findings graded Critical/Important/Minor; fix one at a time, narrowest test re-run after each; full build re-run if any fix changed code/behaviour
