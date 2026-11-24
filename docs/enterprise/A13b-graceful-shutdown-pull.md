# A13b — Graceful shutdown on the pull side

**What it is.** On SIGTERM, `lab-consumer` logs `shutdown: pending=… applied-orders=… (un-ACKed messages
stay at their offsets for the next start)`, exits 0, and a restart resumes exactly where it left off — the
acked segment is not re-processed.

**Why it matters in enterprise.** A clean stop commits only what was fully processed, so a restart resumes
without redelivering already-applied work. This is the consumer's counterpart to B8 (producer flush).

```properties
# No property: this is lifecycle code. In prod you give the orchestrator a sigterm grace period long
# enough for in-flight work to finish and offsets to commit before the container is killed.
```
