# A14 — Atomic multi-topic push (transaction manager + transaction-id-prefix)

**What it is.** `/lab/txn` writes to `lab-ack-a` and `lab-ack-b` inside one Kafka transaction. On
commit both advance; with `forceRollback:true` **neither** advances (the whole unit rolls back).

**Why it matters in enterprise.** Multi-topic side effects that must stand or fall together need a
transaction plus a stable `transaction-id-prefix` (so retries of the same logical op are idempotent at
the broker). Without it, a failure mid-way leaves one topic written and the other not — an inconsistent
system.

**Property block that changes in a real deployment**
```properties
# dev: lab-scoped prefix so Boot's auto transaction manager is not activated
lab.kafka.transaction-id-prefix=lab-txn-
# prod: a stable, per-producer-instance prefix (often derived from the instance id) so the broker's
# transaction coordinator can enforce exactly-once across restarts of that producer.
```
