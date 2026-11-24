# Spec: configuration, coupling rules, and lifecycle

## ADDED Requirements

### Requirement: Externalized configuration
Each service SHALL read its configuration from `application.properties` with environment-variable overrides, and
SHALL NOT hardcode hosts, ports, or credentials in Java code.

Keys: `kafka.server` (default `localhost:9092`), `kafka.topic.*` per service, HTTP port per service
(`8081`/`8082`/`8083`), and for the persistence service `database.host/port/name/user/password`
(defaults `localhost/5432/ordersdb/postgres/postgres`, env-overridable).

#### Scenario: env override
- **WHEN** `DATABASE_NAME=kafkademo` is exported at start
- **THEN** the persistence service MUST use that database
- **AND IF** the value is unusable (DB missing / not reachable)
- **THEN** the service MUST refuse to start with a message naming the key and value

#### Scenario: no secrets in code
- **WHEN** the source of any of the three projects is read
- **THEN** no host, port, or credential literal for the broker or Postgres MAY appear in Java code

### Requirement: No coupling between services
The three Maven projects SHALL be independent: no project MAY import a class from another project, and no
shared/workspace Maven module MAY exist. The only communication channel SHALL be Kafka topics.

#### Scenario: build isolation
- **WHEN** any single project is built
- **THEN** the build MUST NOT require either of the other two projects
- **AND IF** a `commons` module is introduced
- **THEN** that MUST be treated as a design violation and reverted

### Requirement: Graceful shutdown
On a stop signal each service SHALL unsubscribe its Kafka beans, destroy its `KafkaTemplate`, flush in-flight
work, and log one line reporting pending counts.

#### Scenario: SIGINT during a demo run
- **WHEN** a running service receives SIGINT
- **THEN** it MUST stop within its shutdown timeout without stack traces leaking from a Kafka poll loop
- **AND THEN** the log MUST show how many messages/mails were pending
- **AND THEN** no order acknowledged before the signal may be silently lost (replay is covered by idempotency)

### Requirement: Fail-fast startup checks
Each service SHALL verify at startup that the topics it needs exist and (persistence service) that the database
is reachable, and SHALL refuse to start otherwise.

#### Scenario: missing topic
- **WHEN** a required topic is absent
- **THEN** the service MUST exit with a non-zero status and log the exact `kafka-topics.sh --create --if-not-exists --bootstrap-server 127.0.0.1:9092` command

### Requirement: Repository artifacts
The repo SHALL contain `README.md` (concept map, docker/broker commands including the
`DOCKER_SOCKET=/home/abhi/.docker/desktop/backend.sock` requirement, curl + `psql` walkthrough, run order),
`docker-compose.yml` documenting the broker/topology and the four topics, and `.gitignore` excluding
`target/`, IDE files, and build output.

#### Scenario: docs match reality
- **WHEN** a README command is executed verbatim
- **THEN** it MUST address the measured broker at `localhost:9092` / container `order-kafka2` and the measured
  Postgres at `127.0.0.1:5432` (native 15.19), not the sibling project's `postgres:16` Docker setup
