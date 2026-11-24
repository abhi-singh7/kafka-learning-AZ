# Spec: configuration, coupling rules, lifecycle, and the docs/learning artifacts

## ADDED Requirements

### Requirement: Externalized configuration
Both apps SHALL read every Kafka coordinate from Boot `application.properties` (with env overrides) and SHALL
NOT hardcode hosts, ports, topic names, partition counts, group names, retry budgets, or ack modes in Java code.
Keys SHALL include: `spring.kafka.bootstrap-servers` (default `localhost:9092`),
`spring.kafka.consumer.group-id`, `spring.kafka.producer.transaction-id-prefix`,
`spring.kafka.producer.value-serializer` / `consumer.value-deserializer`
(`org.springframework.kafka.support.serializer.JacksonJsonSerializer` / `…JacksonJsonDeserializer`),
`spring.kafka.producer.properties[...]` (`acks`, `queue-size`, `max.attempts`, linger/batch, `max.blocking`),
`spring.kafka.consumer.properties[...]`, `spring.kafka.listener.*`, `spring.kafka.admin.*`,
`spring.kafka.streams.application-id`, plus lab keys `lab.ack.strategy`, `lab.ack.batch-size`,
`lab.retry.max-attempts`, `lab.retry.backoff.base.ms`, `lab.rollback.mode`, `lab.topics.*`, `lab.http.port.*`.

#### Scenario: dial without recompiling
- **WHEN** `lab.ack.strategy=batch` and `lab.retry.max-attempts=5` are set at start
- **THEN** both apps MUST apply them without a code change
- **AND THEN** the startup log MUST name the resulting mode and budget

#### Scenario: no broker literal in code
- **WHEN** either app's sources are read
- **THEN** no `localhost:9092`, topic name, or group name literal MAY appear in Java code

### Requirement: Profile separation (dev vs prod)
Both apps SHALL carry an `application-prod.properties` documenting the enterprise block (multi-host
`bootstrap-servers`, SASL/`auth.*`, `service.*` TLS, `retries`, `stopOnReject`, `acks=all`) — commented, and
SHALL state that this box runs the PLAINTEXT dev profile.

#### Scenario: prod profile is documentation on this box
- **WHEN** `application-prod.properties` is read
- **THEN** every enterprise key MUST be commented with the reason it changes in a real deployment
- **AND THEN** the file MUST carry a header naming what this box cannot demonstrate (single broker, RF 1, PLAINTEXT)

### Requirement: No coupling between the two apps
The two Maven projects SHALL be independent: no project MAY import a class from the other, and no shared module
MAY exist. The only communication channel SHALL be the `lab-*` topics. Each app SHALL declare its own copy of
the wire event classes — duplication is the point.

#### Scenario: build isolation
- **WHEN** one app is built
- **THEN** the build MUST NOT require the other app
- **AND IF** a shared module is introduced
- **THEN** that MUST be treated as a design violation and reverted

### Requirement: Reuse the existing broker, never recreate it
Neither the run book nor any test MAY create, start, stop-and-start-via-recreate, or `docker compose up` a Kafka
broker. The run book SHALL poll `docker logs order-kafka2` for `Successfully registered broker … epoch N`
instead of sleeping, and SHALL re-probe topics before each smoke block (restart policy is `always`).

#### Scenario: no new broker
- **WHEN** the full smoke run completes
- **THEN** `docker ps -a` MUST show the same set of containers as before the run
- **AND IF** any command in the run book recreates a broker
- **THEN** that command MUST be removed and the run re-executed

### Requirement: Graceful shutdown on both apps
On a stop signal each app SHALL unsubscribe its Kafka beans, destroy its factories, flush in-flight work, and log
one line with pending counts; the consumer SHALL commit legitimately ACKed offsets before exit.

#### Scenario: SIGTERM mid-flow
- **WHEN** either app receives SIGTERM with work in flight
- **THEN** it MUST exit within its shutdown timeout without a stack trace escaping a poll loop
- **AND THEN** the log MUST show pending counts and (producer) `flushed <n>`
- **AND THEN** no message MAY be silently lost

### Requirement: Fail-fast on wrong topology
If a required `lab-*` topic is missing, or the schema registry is unreachable when the AVRO lane is enabled, the
app SHALL refuse to start with a message naming the missing thing and the command to create/re-point it.

#### Scenario: missing topic
- **WHEN** `lab-retry` does not exist and the retry lane is enabled
- **THEN** startup MUST fail with the topic name and the documented create command
- **AND THEN** the app MUST NOT start a listener that silently sees an empty stream

### Requirement: Learning artifacts (documentation, no test criterion)
`docs/experiments.md` SHALL exist with, for each concept: the exact run command, the expected observation, and
the enterprise consequence. `docs/enterprise/<concept>.md` SHALL exist for each concept row, including the
concept-only rows, and SHALL carry the property block that changes in a real deployment. `docs/glossary.md`
SHALL separate **don't lose** (`acks`, offsets, flush), **don't do twice** (idempotency key, replay window), and
**the loss window** (ack-before-processing on a multi-broker).

#### Scenario: an aha-moment is reproducible
- **WHEN** a reader follows one `docs/experiments.md` row
- **THEN** the command MUST be copy-pasteable against `localhost:9092`
- **AND THEN** the expected observation MUST name the topic/partition/offset values to look for
- **AND IF** a concept-only row is followed
- **THEN** it MUST be labelled NOT demonstrated on this box

### Requirement: Concept-only honesty
Rows for replication loss, TLS/SASL, ACLs/data plane, cluster sizing, Zookeeper/KRaft, retention/replay SHALL be
labelled "concept-only" everywhere they appear (docs, endpoints, logs) and SHALL NOT be simulated as if
demonstrated (single broker, RF 1, PLAINTEXT).

#### Scenario: no fake demonstration
- **WHEN** `GET /lab/diagnostics` reports replication
- **THEN** it MUST report the measured RF 1 and state that replication loss cannot be shown here
