# X4 — KRaft vs ZooKeeper  [PARTIALLY VISIBLE]

**What is visible here.** The broker runs in **KRaft** mode: its log shows `PROCESS_ROLES: broker,controller`
and no ZooKeeper. So you *can* see that the controller role is a Kafka process, not an external ZK.

**What cannot be shown here.** There is only one node and no ZooKeeper to contrast it with, so you cannot
observe a multi-node **controller quorum**, controller failover, or a ZK→KRaft migration. That part is
concept-only.

**What it means in enterprise.** KRaft removes the external ZooKeeper dependency: the cluster metadata and
the active controller are managed by an internal KRaft quorum of controller nodes (typically 3 or 5). This
simplifies operations (one less system to run/back up) and makes controller failover part of Kafka itself.
The trade-off is a new quorum to size and the migration path from existing ZK-based clusters.

**Property block that changes in a real deployment**
```properties
# dev: single KRaft node, broker+controller in one process (no ZK, no multi-node controller quorum)
# prod: 3 or 5 dedicated controller nodes forming the KRaft quorum; brokers register to it.
#   controller quorum is sized for availability; broker count and RF are sized per X-cluster-sizing / X-replication-loss.
```
