# Glossary — the three guarantees and the one window

Kafka gives you **at-least-once** delivery by default. Everything else — "don't lose",
"don't do it twice", "how much can I actually lose?" — is a property of *your* configuration
and handler, not a free guarantee from the broker. These three terms are the vocabulary for
reasoning about that.

## Don't-lose (durability)

**The guarantee that a record you successfully produced will not be silently dropped.**

What actually provides it:
- **Producer:** `acks=all` (the leader waits for all in-sync replicas) + an idempotent producer
  so retries don't duplicate on the wire. On this box RF is 1, so "all in-sync replicas" is just
  the single leader — durability here means "the broker acknowledged it", not "a replica has it".
- **Consumer:** commit the offset **after** the work is done (ack *after-processed*), and flush
  on shutdown. Committing before processing (ack *immediate*) trades durability for latency.

The failure mode it prevents: you sent a message, got an ack, and then it vanished (broker crash
with no replica, or you committed the offset before doing the work and then crashed).

## Don't-do-twice (idempotency / exactly-once *effect*)

**The guarantee that processing a record more than once has no extra effect.**

Kafka will redeliver — on consumer rebalance, on crash-restart, on retry. "Exactly once" is not
a broker setting you can turn on for the whole pipeline; it is achieved by making the *handler*
idempotent:
- Keep a **durable idempotency key** (here `orderId`) and check it before applying (the dedup
  ledger). If already applied, skip.
- For transactional side effects, use a Kafka transaction + a stable `transaction-id-prefix` so a
  retried unit either fully applies or fully rolls back.

The failure mode it prevents: a crash between "did the work" and "committed the offset" causes a
redelivery that double-applies (double charge, double email, duplicate row).

> **Don't-lose and don't-do-twice pull in opposite directions.** Committing early reduces loss but
> increases redelivery (more double-work risk); committing late reduces redelivery but increases
> loss. You cannot get both from the broker alone — you get *at-least-once* and then make the
> handler idempotent to recover the "twice" half.

## The loss window

**The set of records that can be lost given a crash at a particular point in time.** Its size is
determined by *where you commit relative to doing the work*, plus producer acks/RF.

- **Ack `immediate` (commit before processing):** loss window = the one record currently being
  processed. If you crash mid-processing, that record's offset already advanced → it is lost.
- **Ack `after-processed` (default here):** loss window ≈ 0 for committed work; the cost is that a
  crash *after* processing but *before* commit causes **redelivery** (handled by idempotency), not loss.
- **Batch checkpoint (every N records):** you can lose up to N−1 records if you crash just after a
  checkpoint — fewer commits, slightly larger window.

Related, but distinct, is the **replay/duplicate window**: with at-least-once you may *see* a record
again even when you never *lost* it. That is not loss — it is redelivery — and it is exactly what
idempotency (don't-do-twice) is for.

## One-line summary

| Term | Question it answers | Provided by |
|------|--------------------|-------------|
| Don't-lose | "Will an acked message vanish?" | `acks=all` + RF/ISR + commit-after-work + flush on shutdown |
| Don't-do-twice | "If I see it twice, does it matter?" | Durable idempotency key / transactional side effects |
| Loss window | "How much can a crash at time T cost me?" | The ack point (immediate / after-processed / batch) + producer acks/RF |
