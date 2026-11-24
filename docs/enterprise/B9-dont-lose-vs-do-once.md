# B9 — Don't-lose ≠ do-once

**What it is.** `acks=all` + an idempotent producer prevents *loss* and *wire-level duplicates*, but
the consumer can still see a record more than once (at-least-once). The dedup ledger keyed by
`orderId` makes the *effect* exactly-once. See [glossary.md](../glossary.md) for the full treatment.

**Why it matters in enterprise.** "Exactly once" is not a broker dial; it is `acks=all` (don't lose)
**plus** an idempotent handler (don't do twice). Teams that only set `acks=all` still double-apply on
replay; teams that only dedup can still lose data. You need both halves.

**Property block that changes in a real deployment**
```properties
# dev: acks=1, RF 1 ; prod: the durability half of "don't lose"
spring.kafka.producer.properties[acks]=all
spring.kafka.producer.properties[enable.idempotence]=true
# (topic-side replication.factor=3, min.insync.replicas=2)
# The "don't do twice" half is code: a durable idempotency key checked before applying.
```
