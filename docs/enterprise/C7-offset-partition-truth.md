# C7 — Offset/partition truth

**What it is.** `/lab/diagnostics` reports, per partition, the group's committed offset **and** the next
(end) offset; their gap is the true backlog.

**Why it matters in enterprise.** "How far behind am I?" has one authoritative answer from the broker:
end-offset − committed-offset, per partition. Guessing from logs or timestamps is not a substitute for
reading the actual offsets.

**Property block that changes in a real deployment**
```properties
# No property: this reads broker state via an admin bean. In prod you build dashboards/alerts directly
# on these per-partition offsets (lag = end - committed).
```
