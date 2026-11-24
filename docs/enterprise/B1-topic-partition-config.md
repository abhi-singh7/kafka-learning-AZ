# B1 — Topic + partition config

**What it is.** A topic's partition count and replication factor are set at creation (here via
Boot `NewTopic` beans) and are the backbone of throughput and ordering. `lab-orders` has 3
partitions; every other lane has 1. RF is 1 on this box.

**Why it matters in enterprise.** Partition count bounds parallelism: a consumer group cannot use
more consumers than partitions. Ordering is per-partition, so the key choice (B2) and the partition
count together define your ordering guarantee. RF defines durability (see X-replication-loss).

**Property block that changes in a real deployment**
```properties
# dev: lab-orders = 3 partitions, RF 1 (single broker)
lab.topic.orders.partitions=3
lab.topic.replication=1
# prod: more partitions for throughput, RF >= 3 across racks
# lab.topic.orders.partitions=12
# lab.topic.replication=3
# (topic-side min.insync.replicas=2 set at creation / via admin)
```
