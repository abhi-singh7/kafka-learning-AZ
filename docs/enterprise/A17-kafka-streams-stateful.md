# A17 — Kafka Streams: stateless + stateful per-customer running total

**What it is.** A hosted Streams app (app-id `lab-producer`) reads `lab-pending` (key = `customerId`),
keeps a per-customer running total in a state store, and emits each updated total to `lab-accepted`.
`GET /lab/accepted` returns the totals in push order with their real partition/offset.

**Why it matters in enterprise.** Streams gives you stateful aggregation *inside* the Kafka topology —
the running state lives in a changelog topic (`lab-producer-lab-running-totals-changelog`) and survives
restarts. It is the tool for "compute over a stream with durable state" without an external DB.

**Property block that changes in a real deployment**
```properties
# dev: short commit so state flushes are observable; app-id keeps topics/groups in lab-*
spring.kafka.streams.application-id=lab-producer
spring.kafka.streams.properties[commit.interval.ms]=100
# prod: default processing.guarantee and a sensible commit interval; the state store + changelog
# topic get real RF (>=3) so the running state is durable across broker loss.
```
