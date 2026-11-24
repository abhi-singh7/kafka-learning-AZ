# A12b — Transactional pop (KafkaTransactionManager + Chained @Primary)

**What it is.** The consumer reads `lab-orders` and produces side effects to `lab-ack-a`/`lab-ack-b` inside
one transaction. On failure the source offset on `lab-orders` is unchanged and neither ack topic advanced —
the whole unit rolled back. A `ChainedKafkaTransactionManager` is `@Primary` when multiple producers are involved.

**Why it matters in enterprise.** Consuming *and* producing side effects atomically needs a transaction
manager. Without it, a crash mid-side-effect leaves partial writes (one ack topic written, the other not).

```properties
# dev: lab-scoped prefix so Boot's auto tx manager is not activated
lab.kafka.transaction-id-prefix=lab-txn-
# prod: stable per-instance prefix; chain managers when one consumer drives several producers atomically.
```
