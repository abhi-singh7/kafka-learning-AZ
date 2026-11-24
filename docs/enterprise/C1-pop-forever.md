# C1 — Pop non-stop, forever

**What it is.** The consumer is a long-lived `@KafkaListener` worker: it stays up after its backlog
drains and processes whatever arrives later. A cold start processes the backlog from `earliest`.

**Why it matters in enterprise.** A consumer is not a job that finishes; it is a durable worker that must
survive an empty topic and pick up new inflow without a restart. Design for "always on, sometimes idle".

**Property block that changes in a real deployment**
```properties
# dev: process the backlog on cold start
lab.consumer.startup=process     # process (earliest) | skip (latest)
# prod: usually earliest for critical lanes so nothing is missed after a long downtime.
```
