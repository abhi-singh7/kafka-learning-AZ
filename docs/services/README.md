# Kafka Order Event Services — Service Documentation

This directory is the authoritative, human-readable reference for the three microservices in this
repository: **order-service** (:8081), **order-persistence-service** (:8082) and **email-service** (:8083).
It explains the system architecture, the functionality of each service, the Kafka contract between
them, and the operational details needed to run and operate them.

> Note: the repo also hosts a separate Kafka learning pair (`lab-producer` / `lab-consumer`) documented
> under `docs/` (hands-on guide, experiments, enterprise notes, glossary). This folder covers the
> **order services** only.

## Documentation map

| Document | What it covers |
|---|---|
| [architecture.md](./architecture.md) | System architecture, components, data flow, coupling rules, lifecycle |
| [kafka-contract.md](./kafka-contract.md) | The four topics, payload schema, keys, codecs, ack/NACK semantics, poison handling |
| [order-service.md](./order-service.md) | Producer: REST API, validation, mapping, Kafka production, shutdown |
| [order-persistence-service.md](./order-persistence-service.md) | Consumer #1: JSON decode, Postgres persistence, idempotency, query API |
| [email-service.md](./email-service.md) | Consumer #2: JSON decode, mock mail rendering, bounded store, query API |
| [operations.md](./operations.md) | Environment, run order, configuration, testing, troubleshooting |

## The big picture in one paragraph

A REST client posts an order to **order-service**. That service validates the payload, generates an
`orderId` (UUID), and publishes **one Kafka message per line item** to the `order-created` topic with
the `orderId` as the message key. The `order-created` stream is consumed **independently** by two
separate services: **order-persistence-service** (writes one row per line to Postgres table
`order_line_events`) and **email-service** (renders a mock mail per line, kept in a bounded in-memory
store). Each consumer publishes its own ack event (`order-persisted` / `order-email-sent`) and its own
poison record (`order-failed`) when a message cannot be decoded. The services share **no code, no
database connection and no classes** — Kafka topics are the only coupling.

## Tech stack

- Java 25 (toolchain), Spring Boot **4.1.2**, Spring for Apache Kafka **4.1.2**, Jackson 3
- Apache Kafka 4.0.0 (KRaft single broker, `order-kafka2`, `localhost:9092`, PLAINTEXT)
- PostgreSQL 15.19 (native, `127.0.0.1:5432`, DB `ordersdb`, user/password `postgres`)
- Maven (no wrapper — use `mvn` directly), three self-contained Maven modules, no aggregator

## Quick reference

| Service | Port | Role | Consumes | Produces (ack) | Persists |
|---|---|---|---|---|---|
| order-service | 8081 | REST in, Kafka out | — | `order-created` | — (in-memory accepted-order list) |
| order-persistence-service | 8082 | Kafka → Postgres | `order-created` | `order-persisted`, `order-failed` | `order_line_events` |
| email-service | 8083 | Kafka → mock mail | `order-created` | `order-email-sent`, `order-failed` | in-memory mail store (bounded 1000) |

## Core guarantees

- **At-least-once delivery**: consumers ACK only after successful handling; infra failures NACK so the
  message is redelivered.
- **Idempotent persistence**: re-consuming a message never creates a second row
  (`messageId` UNIQUE + PK `(orderId, sku)`, insert-or-skip).
- **Poison isolation**: undecodable or foreign-schema payloads go to `order-failed` and are ACKed,
  so one bad message never blocks the stream.
- **Fail-fast startup**: a service refuses to start if a required topic is missing (logs the exact
  `kafka-topics.sh` command to run).
- **Graceful shutdown**: each service flushes and destroys its `KafkaTemplate`, logging pending counts.
