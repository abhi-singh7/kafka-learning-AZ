# C2 — Backlog: cold start vs inflow-while-slow

**What it is.** `/lab/backlog` shows the in-flight queue size and per-partition lag (`next − committed`).
With a drained queue both are 0; with a slow handler + fast producer the in-process queue and/or broker
lag grow.

**Why it matters in enterprise.** You must separate *broker backlog* (durable offset gap) from *in-process
queue depth* (volatile). Capacity planning needs both: one says "how much is waiting on the broker", the
other says "how much is sitting in this JVM".

**Property block that changes in a real deployment**
```properties
# No property: backlog is observed, not configured. In prod you alert on broker lag (durable) and on
# in-process queue depth (volatile) as two distinct signals.
```
