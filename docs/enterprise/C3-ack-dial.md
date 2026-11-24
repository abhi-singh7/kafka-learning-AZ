# C3 — Ack dial: immediate / after-processed / batch checkpoint

**What it is.** `lab.ack.strategy` selects the commit point: `immediate` (commit before processing —
loss window = one in-flight record), `after-processed` (default; commit after handling), or `batch`
(checkpoint every N records).

**Why it matters in enterprise.** The ack point *is* your loss window. Commit early → risk lost work on
crash; commit late → risk redelivery (handled by idempotency); batch → fewer commits, slightly larger
window. Choose deliberately and state the window — never assume it. See [glossary.md](../glossary.md).

**Property block that changes in a real deployment**
```properties
# dev: after-processed (the safe default)
lab.ack.strategy=after-processed
lab.ack.batch-size=5
# prod: pick per lane; batch for high-throughput lanes, after-processed where loss is worse than replay.
```
