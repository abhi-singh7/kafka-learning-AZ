# B6 — Sync vs async ack (two beans + one toggle)

**What it is.** Two producer lanes: the async lane (`/lab/orders`) returns 202 naming topic/partition/offset once its `CompletableFuture` resolves on the broker ack (it does not return before confirmation), and the sync lane (`/lab/orders/sync`) blocks a servlet thread until the broker acks. Both wait for the real coordinates — they differ in threading, not timing. A `DO-NOT-SHIP` class documents the blocking shape on purpose.

**Why it matters in enterprise.** Async keeps request threads free (throughput) but you must handle
the completion future and its failure branch. Sync is simpler and correct for low-rate paths, wrong
for hot ones. The choice is a property, so it can be flipped per lane without code changes.

**Property block that changes in a real deployment**
```properties
# dev: one property selects the active ack mode (see LabBeans startup log)
lab.producer.lane=async          # async | sync
lab.producer.lane.async.acks=1   # dev ; prod typically all (durability)
lab.producer.lane.sync.acks=all
```
