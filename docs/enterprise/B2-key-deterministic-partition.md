# B2 — Key → deterministic partition

**What it is.** The record key selects the partition (`hash(key) % partitions`). `lab-orders` is
keyed by `customerId`, so one customer's lines always land on the same partition; The unkeyed lane (null key) uses Kafka's default sticky partitioner, so consecutive small sends typically stick to one partition rather than spreading evenly — and per-customer ordering is not guaranteed there.

**Why it matters in enterprise.** This is how you get "per-entity ordering" without a global order.
Keying by a generated id (not the business entity) silently breaks per-entity ordering — every key
hashes to a different partition. Pick the key for the *ordering domain* you need, not for evenness.

**Property block that changes in a real deployment**
```properties
# The key is code/contract, not a property — but partition count interacts with it:
# doubling partitions re-hashes every key to (possibly) different partitions, which
# reshuffles ordering. In prod you freeze the partition count once keys are live.
lab.topic.orders.partitions=3   # dev  ; prod: fixed at whatever the ordering domain requires
```
