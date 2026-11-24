# Spec: `lab-producer` (`:8091`) — HTTP in, Kafka out, plus the Kafka Streams lane

## ADDED Requirements

### Requirement: Application wiring
`lab-producer` SHALL be a Spring Boot `4.1.2` console application whose Kafka clients come only from Boot
Kafka beans and `spring.kafka.*` properties (`KafkaBroker`/`KafkaTemplate`, `NewTopic` per lane, Boot
Jackson serializers via `spring.kafka.producer.value-serializer`). The app SHALL NOT construct an
`org.apache.kafka` client directly and SHALL NOT hardcode hosts, ports, topic names, or partition counts in
Java code.

#### Scenario: no hand-wired producer
- **WHEN** the source of `lab-producer` is read
- **THEN** no `KafkaProduceRequest`/raw `ProduceRequest` construction MAY appear
- **AND THEN** every topic name and partition count MUST come from properties or `NewTopic` beans

### Requirement: Async push with a PoS endpoint
`POST /lab/orders` SHALL validate the request, push asynchronously, and answer from the push result:
`202` with body `{orderId, topic, partition, offset, messageId, traceId}` and a `Location` header of the
form `lab-orders/<partition>@<offset>`. A synchronous variant `POST /lab/orders/sync` SHALL exist to
demonstrate the blocking `send().await` shape, and a class labelled **DO-NOT-SHIP** SHALL carry a comment
explaining why blocking is wrong in an enterprise app.

#### Scenario: 202 carries broker coordinates
- **WHEN** a valid order is POSTed to `/lab/orders`
- **THEN** the response MUST be `202` and MUST name topic, partition, offset
- **AND WHEN** the same order is POSTed to the unkeyed lane `/lab/orders/unkeyed`
- **THEN** the partition value MAY differ from the keyed lane's

#### Scenario: validation before publish
- **WHEN** a request has `qty <= 0`, blank `sku`, blank `customerId`, or `price < 0`
- **THEN** the response MUST be `422` with `{errors:[{field, reason}]}`
- **AND THEN** no message MAY be pushed to any topic

### Requirement: Key, headers, and payload separation
The push SHALL set key = `customerId`, headers `x-lab-trace`, `x-lab-correlation`, `x-lab-schema`, and a
payload carrying only the order fields.

#### Scenario: headers readable without the payload
- **WHEN** a consumer logs a message
- **THEN** key, headers, and payload MUST be logged as three separate parts
- **AND THEN** a routing/dedup decision MUST be possible from headers alone

### Requirement: Producer flow control
Producer properties SHALL include `acks`, a bounded `queue-size` (default 1000), `max.attempts`, linger/batch
tuning, and `max.blocking` for the sync lane — all via `spring.kafka.producer.properties[...]`.

#### Scenario: fast producer meets bounded queue
- **WHEN** `POST /lab/throttle` pushes more messages than the queue can hold and the consumer is slow
- **THEN** the producer MUST answer `429` with `Retry-After` and MUST NOT terminate with OOM
- **AND THEN** a metrics counter MUST record rejected pushes
- **AND THEN** the log MUST show the producer throttled, not lost

### Requirement: Sync vs async acknowledgment
Two beans SHALL provide sync and async templates, selected by one property, and the app SHALL log at startup
which is active.

#### Scenario: chosen mode is visible
- **WHEN** the app starts
- **THEN** the log MUST name the active acknowledgment mode and its `acks` value

### Requirement: Flush on shutdown
On a stop signal the app SHALL unsubscribe its beans, destroy its template, and flush pending pushes before
exiting.

#### Scenario: SIGTERM during a throttle run
- **WHEN** the app receives SIGTERM while pushes are pending
- **THEN** it MUST log `flushed <n>` and exit within its shutdown timeout
- **AND THEN** those messages MUST NOT be silently lost

### Requirement: Health reports broker reachability
`GET /health` SHALL be served by the Boot web/health surface and MUST report whether the Kafka broker at
`localhost:9092` is reachable.

#### Scenario: broker down
- **WHEN** `order-kafka2` is stopped
- **THEN** `GET /health` MUST report unhealthy within its configured time
- **AND THEN** pushes MUST fail with a readable error, not hang the JVM

### Requirement: Diagnostics as a read-only broker query
`GET /lab/diagnostics` SHALL list topics, partitions, and per-partition next offsets through
`spring.kafka.admin.*`, and SHALL NOT write anything to any topic.

#### Scenario: list-only
- **WHEN** `/lab/diagnostics` is called repeatedly
- **THEN** no topic offset MAY change because of the call
- **AND THEN** the response MUST include the sibling `order-*` topics for namespace contrast

### Requirement: Kafka Streams lane (stateless + stateful)
The producer SHALL host the Streams lane: `lab-pending` in, stateless transform out to `lab-accepted`, and a
stateful per-`customerId` running total computed via Kafka Streams operators, with
`spring.kafka.streams.application-id` configured and `@EnableKafkaStreams`/`StreamsBuilder` beans.

#### Scenario: stateful accumulator visible
- **WHEN** three orders for `cust-0` are pushed through `lab-pending`
- **THEN** `GET /lab/accepted` MUST show the per-customer running totals in push order
- **AND THEN** `GET /lab/diagnostics` MUST show `lab-accepted` offsets advanced
