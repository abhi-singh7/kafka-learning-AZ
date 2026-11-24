# C4 — Sync vs async ack on the pull side

**What it is.** The same commit-point trade-off as C3, mirrored on the consumer: when the offset commits
relative to processing determines your durability/latency balance (visible in the log timing).

**Why it matters in enterprise.** Just like the push side, "when do I commit?" on the pull side sets your
loss-vs-redelivery balance. The answer should be explicit per lane and paired with idempotent handlers.

**Property block that changes in a real deployment**
```properties
# Same dial as C3:
lab.ack.strategy=after-processed
# prod: align with the durability requirement of each lane (see glossary "loss window").
```
