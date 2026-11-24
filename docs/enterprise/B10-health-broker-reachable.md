# B10 — Health = broker reachable

**What it is.** `/health` (producer) and `/lab/health` (consumer) report `brokerReachable` based on
whether the broker answers (cluster id / metadata), not just whether the JVM is up.

**Why it matters in enterprise.** A liveness check that only says "the process is running" hides a
dead or partitioned broker — the app looks healthy while silently failing to produce/consume.
Health must reflect *Kafka reachability* so orchestrators can act (restart, route away).

**Property block that changes in a real deployment**
```properties
# No property: health reads the broker via an admin bean. In prod you wire this into the
# readiness/liveness probes and alert on brokerReachable=false, treating a down broker as
# an unhealthy instance even though the JVM is fine.
spring.kafka.bootstrap-servers=localhost:9092   # dev ; prod: multiple brokers
```
