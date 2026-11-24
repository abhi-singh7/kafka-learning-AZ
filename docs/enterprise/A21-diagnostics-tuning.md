# A21 — Diagnostics & tuning (where the backlog shows in memory)

**What it is.** `/lab/backlog` reports the in-flight queue size and per-partition lag (`next offset −
committed offset`). A slow handler grows the *in-process* queue before it grows *broker* lag; a fast
producer against a stopped consumer grows broker lag.

**Why it matters in enterprise.** Backlog lives in two places — **broker-side** (durable offset gap) and
**in-process** (volatile consumer queue). Tuning means knowing which one you are looking at: a full
broker with an empty in-process queue is a different problem (consumer too slow / down) than the reverse.

**Property block that changes in a real deployment**
```properties
# dev: defaults ; prod: size the poll + handler to bound both backlogs
spring.kafka.consumer.properties[max.poll.records]=200
spring.kafka.consumer.properties[fetch.min.bytes]=1
spring.kafka.consumer.properties[fetch.max.wait.ms]=500
```
