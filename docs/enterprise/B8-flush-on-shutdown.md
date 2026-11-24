# B8 — Flush on shutdown

**What it is.** On SIGTERM, `LabShutdown` (a `DisposableBean`) calls `flush()` on the producer lanes
before destroying them, so buffered-but-unacked messages leave the buffer. Log: `…producer lane
flushed during shutdown`, then `all KafkaTemplates destroyed exactly once`.

**Why it matters in enterprise.** Without a flush on the way out, in-flight async sends are lost when
the JVM exits. Graceful shutdown must drain the send buffer — and only destroy each template once —
or a rolling deploy drops messages.

**Property block that changes in a real deployment**
```properties
# No property: this is lifecycle code (DisposableBean). In prod you pair it with the
# orchestrator's preStop/sigterm grace period so the flush has time to complete before
# the container is killed (e.g. a terminationGracePeriodSeconds long enough for flush()).
```
