# B7 — Producer flow control

**What it is.** The producer has a bounded send buffer (`buffer.memory`), a short `max.block.ms`,
and linger/batch tuning. When a fast producer outruns the broker, `send()` is *refused* after
`max.block.ms` instead of growing memory to OOM — `/lab/throttle` turns this into HTTP 429 +
`Retry-After`.

**Why it matters in enterprise.** A producer must be able to say *no*. Bounded queue + short block
turns "silently grow until OOM" into an observable back-pressure signal the caller can act on (retry
later, shed load). This is the fast-producer/slow-consumer safety valve.

**Property block that changes in a real deployment**
```properties
# dev: deliberately tiny queue so the 429 is easy to trigger
lab.producer.throttle.buffer.bytes=65536
lab.producer.throttle.max.request.size=4096
lab.producer.throttle.max.block.ms=300
# prod: a real buffer sized to absorb broker hiccups, still bounded
# spring.kafka.producer.properties[buffer.memory]=67108864
# spring.kafka.producer.properties[max.block.ms]=5000
```
