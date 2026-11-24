# A16b — Idempotent processing under crash/retry

**What it is.** Push an order, force redelivery of the same `orderId`, restart: the dedup ledger keyed by
`orderId` applies each order **exactly once**; the applied count stays exact after restart despite
at-least-once delivery.

**Why it matters in enterprise.** This is the concrete "do-not-do-twice" mechanism: a durable idempotency
key checked *before* applying. It is what turns Kafka's at-least-once into an application-level exactly-once
effect — the missing half of "don't lose" (B9). See [glossary.md](../glossary.md).

```properties
# No property: idempotency is code (dedup on orderId). In prod you back it with durable storage (DB/Redis)
# so the key survives restarts, and scope it per lane where different lanes need different keys.
```
