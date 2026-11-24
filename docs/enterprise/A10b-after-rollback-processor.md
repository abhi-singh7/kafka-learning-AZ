# A10b — AfterRollbackProcessor, both modes

**What it is.** On a transactional rollback, the processor decides what happens to the offset:
`retry-next-poll` leaves it for a later poll (reprocessed) or `discard` advances past it. Set via
`lab.rollback.mode`.

**Why it matters in enterprise.** You must *decide* what a rolled-back record does to the offset. Leaving
it reprocesses (safe, may loop if not idempotent); discarding skips it (fast, may lose work). The processor
makes that choice explicit instead of implicit.

```properties
lab.rollback.mode=retry-next-poll   # dev ; prod: choose per lane, pair with idempotency to avoid loops
```
