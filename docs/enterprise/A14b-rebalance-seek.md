# A14b — Rebalance + seek (ConsumerAwareRebalanceListener), GROUP_INITIAL_REBALANCE_DELAY_MS=0

**What it is.** `POST /lab/listeners/competitor/off` stops one listener of the shared group; the survivor
re-reads from the group offsets and processes whatever the stopped listener had not acked. On this single
broker that reassignment is in-process only (a multi-broker cluster would also re-fetch the assignment from
the controller). `GROUP_INITIAL_REBALANCE_DELAY_MS=0` removes the default 3s pause so the rebalance is
observable immediately.

**Why it matters in enterprise.** Rebalance moves partitions between consumers of a group; your listeners
must handle partition assign/revoke correctly (seek to committed offsets on join, flush state on leave).
In prod you usually *keep* the initial-rebalance delay to reduce churn.

```properties
# dev: 0 so rebalance is immediately observable
lab.consumer.initial.rebalance.delay.ms=0
# prod: keep the default (~3s) to let the group settle and reduce unnecessary rebalances.
```
