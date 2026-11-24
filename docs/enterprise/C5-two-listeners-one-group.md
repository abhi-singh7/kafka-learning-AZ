# C5 — Two listeners, one topic, one group

**What it is.** Two listeners in the *same* consumer group on one topic share the partitions: total
processing across both equals the message count (offsets are shared, so no record is processed twice).

**Why it matters in enterprise.** Consumers in one group *scale out* (share work); consumers in different
groups each get a full copy. Group membership — not listener count — is the unit of partitioning. Getting
this wrong either under-uses capacity or double-processes.

**Property block that changes in a real deployment**
```properties
# dev: two lanes, groups lab-consumer + lab-competitor on the rebalance topic
lab.group.main=lab-consumer
lab.group.competitor=lab-competitor
# prod: scale a group by adding consumers (<= partition count); use separate groups for independent copies.
```
