# Kafka Contract — topics, payloads, keys, semantics

This is the single contract all three services honor. Violating it is a design bug.

## Topics

| Topic | Producer | Consumer(s) | Purpose | 1 partition, RF 1 |
|---|---|---|---|---|
| `order-created` | order-service | email-service, order-persistence-service | Payload: one message per line item | yes |
| `order-email-sent` | email-service | — (ack sink) | Ack record per handled line event | yes |
| `order-persisted` | order-persistence-service | — (ack sink) | Ack record per persisted line event | yes |
| `order-failed` | email-service, order-persistence-service | — (poison sink) | Undecodable/foreign-schema payloads | yes |

The pre-existing topic `orders` is **never** read or written.

### Topic creation

```bash
BS=127.0.0.1:9092
for T in order-created order-email-sent order-persisted order-failed; do
  kafka-topics.sh --create --if-not-exists --topic "$T" --bootstrap-server $BS
done
```

Services **fail fast** at startup if a required topic is missing and log the exact create command.

## Line-event payload (`order-created`, schemaVersion 1)

Jackson 3 JSON, exactly nine fields:

| Field | Type | Notes |
|---|---|---|
| `schemaVersion` | string | `"1"` — anything else ⇒ poison |
| `orderId` | string | service-generated UUID; also the message KEY |
| `customerId` | string | ≤ 200 chars, non-blank |
| `sku` | string | ≤ 200 chars, non-blank |
| `qty` | int | > 0 |
| `price` | double | ≥ 0 |
| `lineIndex` | int | 0-based, contiguous `0..N-1` |
| `messageId` | string | UUID per line item — idempotency key |
| `createdAt` | instant (ISO-8601) | producer clock at publish time |

**Key = `orderId`.** All lines of one order land on one partition, preserving per-order order and
keeping per-partition ordering intact.

**Codec**: order-service produces with `JacksonJsonSerializer` (Jackson 3). Consumers deserialize with
`StringDeserializer` and decode JSON manually with a `JsonMapper`, then validate against the schema.

## Ack records (`order-email-sent`, `order-persisted`)

Published after successful handling, **KEY = `orderId`**:

| Field | Value |
|---|---|
| `schemaVersion` | `"1"` |
| `orderId` | the order's UUID |
| `messageId` | the line event's `messageId` |
| `sku` | line item SKU |
| `qty` | line item qty (email: as sent; persistence: as persisted) |
| `result` | `"email-sent"` / `"persisted"` |
| `at` | timestamp of the ack |
| `service` | `"email-service"` / `"order-persistence-service"` |

## Poison records (`order-failed`)

Published when a payload cannot be decoded or fails validation, **no key** (keyless send):

| Field | Value |
|---|---|
| `detectedBy` | `"email-service"` / `"order-persistence-service"` |
| `sourceTopic` | `"order-created"` |
| `reason` | `"json-decode"` \| `"schema-mismatch"` |
| `rawPayload` | original payload, **truncated to 1000 chars** |
| `receivedAt` | ISO-8601 timestamp |

Each consumer writes its own poison record (one per consumer per bad payload) and ACKs the source
message so it is not redelivered in a loop.

## ACK / NACK semantics

Both consumers use `MANUAL_IMMEDIATE` ack mode with `enable.auto.commit=false` and
`auto.offset.reset=earliest`:

| Situation | Action | Result |
|---|---|---|
| Payload undecodable or schemaVersion ≠ "1" | publish to `order-failed`, then **ACK** | message not redelivered; poison visible via `GET /orders/failed` |
| Handling works (mail rendered / row inserted) | publish ack, then **ACK** | offset advances |
| Infra failure (mail sink throws / JDBC error / ack publish throws) | **NACK** (rethrow) | container redelivers the same record on the next poll |

Rules:

- NACK is only for **infrastructure failures**, never for bad payloads (those go to `order-failed` + ACK).
- The persistence consumer is **idempotent**: `messageId` UNIQUE + PK `(orderId, sku)` +
  insert-or-skip means redelivery never creates a duplicate row.
- The email consumer is **not** idempotent by nature (mails are copied); its bounded store + eviction
  counter keep the demo observable, and a redelivered message may render a second mail instance — the
  ack-after-render order means a re-render only happens after the first attempt failed, never on success.

## Consumers

| Consumer group | Topic | Class copy | Deserializer | Listener |
|---|---|---|---|---|
| `email-service` | `order-created` | `com.example.orders.email.*` | `StringDeserializer` | `LineEventConsumer.onLineEvent` |
| `order-persistence-service` | `order-created` | `com.example.orders.persistence.*` | `StringDeserializer` | `LineEventConsumer.onLineEvent` |

## Deliberate duplication

Each consumer service carries its own copy of `OrderLineEventRecord`, `LineEventDecoder`,
`LineEventDecodeResult`, `PoisonStore`, `PoisonPublisher`, `TopicStartupProbe` and its own Kafka beans.
This is a deliberate design choice (same-repo-but-separated services, zero shared module). Any schema
change must be applied by hand to both copies — the `schemaVersion` field protects against silent
mismatch.
