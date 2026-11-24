# X3 — Cluster sizing  [CONCEPT-ONLY, not demonstrated]

**Why it cannot be shown here.** There is one node. You cannot observe partitions-per-broker distribution,
rack-aware replica placement, or aggregate cluster throughput, so this row is labelled concept-only.

**What it means in enterprise.** Throughput scales with total partition count and broker count; a consumer
group can use at most as many consumers as partitions per topic. Sizing decisions (partitions per broker,
racks for replica spread, target throughput) determine both performance and how well the cluster survives a
broker or rack loss. Under-partitioning caps parallelism; over-replicating wastes capacity.

**Property block that changes in a real deployment**
```properties
# dev: single node, small partition counts (cannot demonstrate sizing)
lab.topic.orders.partitions=3
# prod: size partitions to the consumer-group scale + throughput target, spread replicas across racks
# lab.topic.orders.partitions=24        # >= max consumers you will run in the group
# broker-side: rack-aware replica assignment, target msg/s per partition, RF 3 across N racks
```
