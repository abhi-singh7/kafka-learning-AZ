# Kafka Lab — Hands-On Learning Guide

This is the step-by-step, curl-driven runbook for the **kafka-lab-pair** project.
Everything here was built *for learning*: each API call below shows exactly what to send,
what you'll see back, and **what that single call teaches you about Kafka**.

There are two independent Spring Boot services (no shared code — they only talk through the
`lab-*` Kafka topics):

| Service | Port | Role |
|---------|------|------|
| `lab-producer` | `8091` | The **send side**. Pushes orders onto `lab-orders` and other lanes; returns the broker's real partition/offset in every ack. Also hosts the Kafka Streams lane and the AVRO lane. |
| `lab-consumer` | `8092` | The **receive side**. Pops from `lab-orders` (and other lanes), logs a per-message lifecycle, tracks committed vs end offsets, retries + quarantines failures, and exposes read-only observability endpoints. |

A Schema Registry (port `8081`) is optional and only needed for the AVRO lane (`/lab/avro`).

**The mental model:** producer writes → broker stores per partition → consumer reads from a
committed offset forward. Almost every endpoint in this guide is just a different *lens* on
those three facts: **where did it land (partition/offset)?**, **how far behind am I (lag)?**, and
**is it durable (acks / committed / txn)?**.

---

## 1. One-time setup (do these first)

Work from the repo root `/home/abhi/DEV-WORK/Kafka`. The broker is a reused Docker container
named `order-kafka2` (apache/kafka:4.0.0, KRaft, PLAINTEXT on `localhost:9092`). **Never recreate it.**

### 1.1 Confirm the broker is up and registered

```bash
# The container should be running:
docker ps --filter name=order-kafka2 --format '{{.Names}}\t{{.Status}}'

# Poll its log for the "registered broker" line (this is how you know it's ready — not a sleep):
docker logs order-kafka2 2>&1 | grep -i "Successfully registered broker" | tail -1
```
**Teaches:** a KRaft broker announces itself with an epoch. Ready = you can see that line, not a timer.

### 1.2 Create the lab topics (idempotent — safe to re-run)

The **consumer refuses to start** unless these five exist: `lab-orders`, `lab-rebalance`,
`lab-rejected`, `lab-ack-a`, `lab-ack-b`. The producer also uses `lab-throttle`, `lab-pending`,
`lab-accepted`, and `lab-avro`. Create all of them once:

```bash
B=127.0.0.1:9092
T="docker exec order-kafka2 /opt/kafka/bin/kafka-topics.sh --bootstrap-server $B"

# lab-orders gets 3 partitions so you can SEE keyed partitioning spread across them; the rest get 1.
$T --create --if-not-exists --topic lab-orders     --partitions 3 --replication-factor 1
$T --create --if-not-exists --topic lab-throttle   --partitions 1 --replication-factor 1
$T --create --if-not-exists --topic lab-ack-a      --partitions 1 --replication-factor 1
$T --create --if-not-exists --topic lab-ack-b      --partitions 1 --replication-factor 1
$T --create --if-not-exists --topic lab-pending    --partitions 1 --replication-factor 1
$T --create --if-not-exists --topic lab-accepted   --partitions 1 --replication-factor 1
$T --create --if-not-exists --topic lab-rebalance  --partitions 1 --replication-factor 1
$T --create --if-not-exists --topic lab-rejected   --partitions 1 --replication-factor 1
$T --create --if-not-exists --topic lab-avro       --partitions 1 --replication-factor 1

# Verify:
$T --list | grep '^lab-'
```
**Teaches:** topics are durable, partitioned storage. Partition count is fixed at creation and drives
how many consumers can run in parallel (one listener per partition). `--if-not-exists` makes setup idempotent.

### 1.3 (Optional) Start the Schema Registry — only for the AVRO lane

Skip this unless you want to exercise `/lab/avro`. The registry runs as a **separate process**
(it cannot be embedded in the Boot test/app JVM on this stack). Its classpath is materialized by
the producer build into `lab-producer/target/schema-registry/`.

```bash
# 1) Materialize the SR classpath (runs the helper pom that resolves Confluent deps in isolation):
cd lab-producer && mvn -B process-test-resources && cd ..

# 2) Write a minimal registry config:
cat > /tmp/sr-lab.properties <<'EOF'
kafkastore.bootstrap.servers=PLAINTEXT://localhost:9092
kafkastore.topic=lab-sr-store
host.name=localhost
port=8081
EOF

# 3) Start it (positional arg = the properties file):
java -cp "lab-producer/target/schema-registry/*" \
     io.confluent.kafka.schemaregistry.rest.Main /tmp/sr-lab.properties > /tmp/sr-lab.log 2>&1 &

# 4) Confirm it's up:
sleep 6 && curl -s localhost:8081/ && echo   # -> {} means healthy
```
**Teaches:** with a schema registry, the wire format is `<5-byte header: magic byte + 4-byte schema id> + encoded body`.
Producer and consumer never share Java classes — they agree on the *schema id* in the registry.

### 1.4 Start `lab-producer` (port 8091)

```bash
cd lab-producer && mvn -B spring-boot:run > /tmp/lab-prod.log 2>&1 &
# wait for readiness (poll, don't sleep):
for i in $(seq 1 60); do (echo > /dev/tcp/localhost/8091) 2>/dev/null && break; sleep 1; done
curl -s localhost:8091/lab/health && echo
```
**Teaches:** the producer exposes health as "can I reach the broker?", not "am my app threads alive?".

### 1.5 Start `lab-consumer` (port 8092)

```bash
cd lab-consumer && mvn -B spring-boot:run > /tmp/lab-cons.log 2>&1 &
for i in $(seq 1 60); do (echo > /dev/tcp/localhost/8092) 2>/dev/null && break; sleep 1; done
curl -s localhost:8092/lab/health && echo
```
If it fails to start, check `tail /tmp/lab-cons.log` — the fail-fast gate will name the missing topic and
the exact `kafka-topics.sh --create` command to run.
**Teaches:** a consumer is a long-lived worker that must survive an empty topic and pick up whatever arrives later.

---

## 2. `lab-producer` (port 8091) — endpoint by endpoint

Every push returns the broker's **real** partition and offset in the body *and* a
`Location: <topic>/<partition>@<offset>` header. That single fact is the heart of the producer side.

### 2.1 `POST /lab/orders` — the canonical async, keyed push

```bash
curl -s -D - -X POST localhost:8091/lab/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"cust-1","items":[{"sku":"SKU-A","qty":2,"price":9.5},{"sku":"SKU-B","qty":1,"price":4.0}]}'
```
You'll get **HTTP 202**, a `Location: lab-orders/<p>@<o>` header, and a body like:
```json
{"orderId":"…","topic":"lab-orders","partition":1,"offset":3,"messageId":"…","traceId":"…"}
```
**Teaches:**
- The send is **async** — the HTTP response is produced by the `SendResult` future, never by blocking on the broker. The request path never waits for an ack.
- **Key = `customerId`** (not the generated `orderId`). Every line of one customer's order lands on the *same* partition → per-customer ordering is preserved.
- The offset in the response is read from the **push result**, not guessed.

### 2.2 Same body, three times — watch the partition stay put

```bash
for i in 1 2 3; do curl -s -X POST localhost:8091/lab/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"cust-42","items":[{"sku":"S","qty":1,"price":1.0}]}' | grep -o '"partition":[0-9]*'; done
```
All three print the **same** partition number (different offsets).
**Teaches:** deterministic partitioning — a fixed key always maps to one partition. Swap `customerId` and you'll see a *different* partition. This is how you get "all messages for customer X in order" without any extra machinery.

### 2.3 `POST /lab/orders/unkeyed` — no key, sticky round-robin

```bash
curl -s -X POST localhost:8091/lab/orders/unkeyed \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"cust-7","items":[{"sku":"S","qty":1,"price":1.0}]}'
```
**Teaches:** with no key, Kafka uses the **sticky partitioner** — a batch of unkeyed records sticks to one partition until it's full, then moves on. So "unkeyed" does *not* mean "evenly round-robined per record". Ordering across unkeyed records is not guaranteed.

### 2.4 `POST /lab/orders/sync` — blocking ack (the contrast)

```bash
time curl -s -X POST localhost:8091/lab/orders/sync \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"cust-1","items":[{"sku":"S","qty":1,"price":1.0}]}'
```
Same 202 + offset, but this lane **blocks** until the broker acks (and uses `acks=all`).
**Teaches:** sync vs async is a latency-vs-simplicity trade-off. In a request path you almost always want async (never block a web thread on the broker); sync is for "I must know it landed before I continue". The project deliberately isolates the blocking style so you can *feel* the difference.

### 2.5 `GET /lab/pushed` — the in-memory list of your acks

```bash
curl -s localhost:8091/lab/pushed
```
A JSON array of the `{orderId, topic, partition, offset, messageId, traceId}` acks from this process's lifetime.
**Teaches:** an application-side record of *what I sent and where it landed* — useful for tracing, but **not durable**. Restart the app and it's gone; the broker is the source of truth.

### 2.6 `POST /lab/throttle` — producer back-pressure (429)

Small burst (fits in the buffer → 202):
```bash
curl -s -w '\nHTTP %{http_code}\n' -X POST localhost:8091/lab/throttle \
  -H 'Content-Type: application/json' -d '{"count":5,"recordBytes":100}'
```
Oversized burst (≫ the 64 KB buffer → **429** with `Retry-After`):
```bash
curl -s -D - -X POST localhost:8091/lab/throttle \
  -H 'Content-Type: application/json' -d '{"count":10000,"recordBytes":4000}' | grep -Ei 'HTTP|Retry-After|pushedThisCall|blockedSendsTotal'
```
**Teaches:** a producer has a finite in-memory buffer. When you hand it more work than it can accept, kafka-clients **refuses the send** (`max.block.ms` exceeded) instead of silently growing memory — and this lane surfaces that as an HTTP 429 back-pressure signal. The app survives; `blockedSendsTotal` counts how many were refused.

### 2.7 `POST /lab/txn` — one transaction across two topics (all-or-nothing)

Commit case:
```bash
curl -s -w '\nHTTP %{http_code}\n' -X POST localhost:8091/lab/txn \
  -H 'Content-Type: application/json' -d '{"orderId":"ord-txn-1","left":"A-side","right":"B-side"}'
```
Rollback case (`forceRollback` makes the lane throw *after* both records are buffered):
```bash
curl -s -w '\nHTTP %{http_code}\n' -X POST localhost:8091/lab/txn \
  -H 'Content-Type: application/json' -d '{"orderId":"ord-txn-2","left":"A-side","right":"B-side","forceRollback":true}'
```
**Teaches:** a Kafka transaction makes writes to `lab-ack-a` **and** `lab-ack-b` atomic — both commit together or neither does. On rollback, *neither* topic's committed offset advances (the records may still sit physically in the log until compaction, but they are invisible to consumers). This is "exactly-once side effects" across multiple topics.

### 2.8 `POST /lab/pending` → `GET /lab/accepted` — the Kafka Streams lane

Push a pending amount for a customer:
```bash
curl -s -X POST localhost:8091/lab/pending \
  -H 'Content-Type: application/json' -d '{"customerId":"cust-9","amount":49.5}'
curl -s -X POST localhost:8091/lab/pending \
  -H 'Content-Type: application/json' -d '{"customerId":"cust-9","amount":20.5}'
```
Then read the Streams output (per-customer running total, with real partition/offset):
```bash
curl -s localhost:8091/lab/accepted
# -> [{"customerId":"cust-9","runningTotal":70.0,"partition":0,"offset":1}, ...]
```
**Teaches:** Kafka Streams does a **stateful aggregation** (`lab-pending` → `lab-accepted`) inside the producer app. The running total is materialized in a state store and flushed on commit — so `/lab/accepted` shows *computed* results, not raw records. (This lane needed a custom stateful `process()` to avoid Streams' cache-flush collapsing values per key.)

### 2.9 `POST /lab/avro` — schema-registry-backed typed push *(needs SR from §1.3)*

```bash
curl -s -D - -X POST localhost:8091/lab/avro \
  -H 'Content-Type: application/json' \
  -d '{"orderId":"ord-avro-1","customer":"alice","amount":100.0,"discount":5.0}'
```
**Teaches:** the value is serialized to Avro against a registered schema; the wire bytes carry a **schema id**, not the schema itself. The consumer decodes by that id — so the two apps can evolve independently as long as they share the registry. (Schema *evolution* in this lab uses Avro's native `ResolvingDecoder`, since Confluent 7.9.0 has no `avro.use.latest.schema` flag.)

### 2.10 `GET /lab/diagnostics` — read-only broker view (producer side)

```bash
curl -s localhost:8091/lab/diagnostics
```
Per topic: whether it exists, its partition count, and the next offset per partition (from your push acks). Missing sibling topics are reported as `exists:false`, not an error.
**Teaches:** you can answer "what's on the broker?" by *querying* it (Admin API) instead of trusting in-memory state — and a read-only query must degrade gracefully when one topic is absent rather than failing the whole call.

### 2.11 `GET /lab/health` — producer health

```bash
curl -s localhost:8091/lab/health
# -> {"brokerReachable":true,"clusterId":"…","pushedTotal":N,"blockedSendsTotal":M}
```
**Teaches:** a producer's health is "is the broker reachable" (a live `describeCluster`), plus its own send counters.

---

## 3. `lab-consumer` (port 8092) — endpoint by endpoint

The consumer is a long-lived worker. You feed it by producing to its topics (via the producer
app, or directly with `kafka-console-producer`), then you *read* what happened through these
read-only endpoints. A handy direct-produce helper:

```bash
# Produce one raw JSON record straight onto a lab topic (bypasses the producer app):
produce() { printf '%s' "$1" | docker exec -i order-kafka2 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server 127.0.0.1:9092 --topic "$2"; }
```

### 3.1 `GET /lab/diagnostics` — committed vs end, per topic@partition

```bash
curl -s localhost:8092/lab/diagnostics
# -> {"topics":[...],"groupOffsets":{"lab-orders@0":5,"lab-orders@1":3,...},
#    "nextOffsets":{"lab-orders@0":5,...},"queueSize":0,"ledgerCount":N,
#    "hasConsumerOffsetsTopic":true,"hasSchemasTopic":false}
```
**Teaches:** the two most important consumer numbers are **end offset** (how much exists) and
**committed offset** (where the group last safely finished). They're keyed by `topic@partition` — a bare
partition number would let two single-partition topics clobber each other. When committed == end, the group is caught up.

### 3.2 `GET /lab/backlog` — lag on `lab-orders`

```bash
curl -s localhost:8092/lab/backlog
# -> {"queueSize":0,"lag":{"0":0,"1":0,"2":0}}
```
Now push a burst and watch the lag appear, then drain to zero:
```bash
for i in $(seq 1 50); do curl -s -X POST localhost:8091/lab/orders \
  -H 'Content-Type: application/json' \
  -d "{\"customerId\":\"cust-$i\",\"items\":[{\"sku\":\"S\",\"qty\":1,\"price\":1.0}]}" >/dev/null; done
curl -s localhost:8092/lab/backlog   # lag > 0 while the consumer catches up, then -> {"0":0,"1":0,"2":0}
```
**Teaches:** **lag = end − committed**, per partition. It's the single most useful "how behind am I" metric in production. On a fast local broker with a trivial handler it drains almost instantly — that's honest, not faked.

---

### 3.3 `POST /lab/inject-failures` → retry → quarantine → `GET /lab/rejected`

This is the "poison pill" story: a record that always fails gets retried, then parked on
`lab-rejected` so it can't block the rest of the partition.

```bash
# 1) Make 100% of records fail on the main lane:
curl -s -X POST localhost:8092/lab/inject-failures \
  -H 'Content-Type: application/json' -d '{"percentage":100,"lane":"orders"}'

# 2) Send one order (it will fail, retry x3 with backoff, then be quarantined):
curl -s -X POST localhost:8091/lab/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"cust-bad","items":[{"sku":"S","qty":1,"price":1.0}]}'

# 3) Turn failures back OFF so normal traffic flows again:
curl -s -X POST localhost:8092/lab/inject-failures \
  -H 'Content-Type: application/json' -d '{"percentage":0,"lane":"orders"}'

# 4) Read the quarantined record(s):
curl -s localhost:8092/lab/rejected
# -> {"records":[{"reason":"handler-throw","rawPayload":"{...}","sourceTopic":"lab-orders",
#                 "sourcePartition":0,"sourceOffset":N,"attempts":4, ...}]}
```
**Teaches:** bounded retry with exponential backoff, then **dead-letter to a quarantine topic**. The
`/lab/rejected` reader opens a throwaway consumer, seeks to the beginning, and drains *every* quarantined
record (the earlier single-offset `receive()` could only ever return offset 0 — a real bug found in smoke).
The source partition/offset tells you exactly which record to fix and replay.

---

### 3.4 `POST /lab/listeners/competitor/off` — rebalance & "who owns the partition now"

The race lane has **two listeners in one consumer group** (`lab-competitor`) on `lab-rebalance`.
With one partition, only one listener can own it at a time.

```bash
# Produce 5 records to the race topic; exactly one listener processes each (total = 5):
for i in $(seq 1 5); do produce "{\"n\":$i}" lab-rebalance; done

# Stop listener B; the survivor re-reads from the group's committed offsets:
curl -s -X POST localhost:8092/lab/listeners/competitor/off
# -> {"stopped":true,"pending":0}
```
**Teaches:** **consumer-group rebalancing**. Partitions are assigned to members; when a member leaves,
the group reassigns its partitions and survivors continue from the *committed* offset — nothing is lost or double-processed (at-least-once with committed offsets). On this single-broker box the reassignment is in-process; on a real cluster it also involves the controller.

### 3.5 `GET /lab/metrics` — Micrometer view of the messaging layer

```bash
curl -s localhost:8092/lab/metrics
# -> {"meters":[{"name":"messaging.consumer.records","tags":["lane=orders",...]}, ...]}
```
**Teaches:** spring-kafka exposes consumer/producer activity as **Micrometer meters**. Tagging them (e.g. `lab.lane=orders`) is how you'd slice "records per lane" in Grafana. This endpoint just lists the messaging/kafka meters so you can see what's observable without a full metrics backend.

---

### 3.6 `GET /lab/avro` — decode what the producer sent *(needs SR from §1.3)*

```bash
# Producer side (needs registry up):
curl -s -X POST localhost:8091/lab/avro \
  -H 'Content-Type: application/json' \
  -d '{"orderId":"ord-avro-2","customer":"bob","amount":55.0,"discount":5.0}'

# Consumer side — the decoded OrderEvents, in receive order:
curl -s localhost:8092/lab/avro
# -> {"events":[{"orderId":"ord-avro-2","customer":"bob","amount":55.0,"discount":5.0,
#                "phoneNumber":null,"partition":0,"offset":1}]}
```
**Teaches:** the consumer never imports the producer's classes — it decodes by the **schema id** in the
envelope against the registry. `discount`/`phoneNumber` are nullable (optional Avro fields), so a v1 record
reads cleanly under a later schema. This is the whole point of a shared registry: independent evolution.

### 3.7 `GET /lab/health` — consumer health

```bash
curl -s localhost:8092/lab/health
# -> {"brokerReachable":true,"queueSize":0,"ledgerCount":N}
```
**Teaches:** a consumer's health = "can I reach the broker" plus how much is currently **in flight**
(`queueSize`) and how many records this process has processed (`ledgerCount`).

---

## 4. End-to-end experiments (both services together)

These tie the two apps together through the broker — the payoff of the whole lab.

### 4.1 The full order journey: push → consume → confirm caught up

```bash
# Push an order via the producer:
curl -s -X POST localhost:8091/lab/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"cust-e2e","items":[{"sku":"SKU-X","qty":3,"price":10.0}]}'

# The consumer processes it (watch its log): 
tail -n 5 /tmp/lab-cons.log | grep -i 'lab-orders'

# Confirm the group is caught up (committed == end on lab-orders):
curl -s localhost:8092/lab/backlog     # -> lag all zeros
curl -s localhost:8092/lab/diagnostics # -> groupOffsets == nextOffsets per partition
```
**Teaches:** the producer's ack offset and the consumer's committed offset are *the same coordinate system*. When they agree, the message has durably moved from "sent" to "processed".

### 4.2 Transaction atomicity seen from both sides

```bash
# Note offsets before:
curl -s localhost:8091/lab/diagnostics | grep -o '"lab-ack-[ab]":[^}]*'

# One committed txn push (both topics advance together):
curl -s -X POST localhost:8091/lab/txn \
  -H 'Content-Type: application/json' -d '{"orderId":"ord-tx","left":"L","right":"R"}'

# Offsets after — lab-ack-a and lab-ack-b moved by the same amount:
curl -s localhost:8091/lab/diagnostics | grep -o '"lab-ack-[ab]":[^}]*'
```
**Teaches:** "all-or-nothing across topics" is only meaningful if you can *observe* both offsets move in lockstep. A rollback (`forceRollback:true`) leaves them unmoved — the consumer never sees the rolled-back pair.

### 4.3 The loss window, made concrete

```bash
# Fail one record, let it be quarantined (see §3.3), then:
curl -s localhost:8092/lab/rejected | grep -o '"sourceOffset":[0-9]*'
```
**Teaches:** the three guarantees map to mechanisms — **don't lose** = committed offsets + acks=all;
**don't do twice** = idempotent/transactional producers and per-partition ownership; **the loss window** is the gap between "broker stored it" and "consumer durably committed its offset". The quarantine topic is where you go to *inspect* records that fell through.

---

## 5. Quick reference

| What I want to learn | Call it | Watch for |
|----------------------|---------|-----------|
| Where a record lands | `POST :8091/lab/orders` | same key → same partition, offset in body + `Location` header |
| Partitioning without a key | `POST :8091/lab/orders/unkeyed` | sticky batching, not per-record round-robin |
| Sync vs async send | `POST :8091/lab/orders/sync` | blocking ack, `acks=all` |
| Producer back-pressure | `POST :8091/lab/throttle` (big burst) | HTTP 429 + `Retry-After`, `blockedSendsTotal` |
| Multi-topic atomicity | `POST :8091/lab/txn` | both `lab-ack-*` offsets move together / not at all |
| Stateful aggregation | `POST :8091/lab/pending` → `GET :8091/lab/accepted` | running total with partition/offset |
| Schema registry round-trip | `POST :8091/lab/avro` → `GET :8092/lab/avro` | decode by schema id, nullable fields |
| How far behind I am | `GET :8092/lab/backlog` | lag = end − committed, per partition |
| Committed vs end | `GET :8092/lab/diagnostics` | keyed by `topic@partition` |
| Poison pill → quarantine | `POST :8092/lab/inject-failures` then `GET :8092/lab/rejected` | retry ×3 then dead-letter with source offset |
| Rebalance / ownership | `POST :8092/lab/listeners/competitor/off` | survivor resumes from committed offset |
| Observability | `GET :8092/lab/metrics` | Micrometer messaging meters, tagged by lane |

## 6. Cleanup

```bash
# Stop the apps (find their maven/spring-boot PIDs):
pkill -f 'spring-boot:run' || true
# Stop the registry if you started it:
pkill -f 'io.confluent.kafka.schemaregistry.rest.Main' || true

# Optional: delete lab topics to reset (the broker and its sibling order-* topics are untouched):
B=127.0.0.1:9092
for t in lab-orders lab-throttle lab-ack-a lab-ack-b lab-pending lab-accepted \
         lab-rebalance lab-rejected lab-avro lab-sr-store; do
  docker exec order-kafka2 /opt/kafka/bin/kafka-topics.sh --bootstrap-server $B --delete --topic "$t" 2>/dev/null
done
```

**What this project deliberately does NOT show (and says so instead of faking it):**
multi-broker replication, SASL/TLS/ACL security, real cluster sizing, and DevTools hot-reload
(not on the classpath for JDK 25 / Boot 4.1.2). Those are documented as concept-only in `docs/enterprise/`.

---
### Where to go next
- Per-experiment detail: [`experiments.md`](./experiments.md)
- One page per enterprise concept: [`enterprise/`](./enterprise/)
- The vocabulary (don't-lose / don't-do-twice / the loss window): [`glossary.md`](./glossary.md)
