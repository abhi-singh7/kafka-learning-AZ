# X1 — Replication loss  [CONCEPT-ONLY, not demonstrated]

**Why it cannot be shown here.** The box is a single broker with `replication.factor=1` and ISR size 1.
There is no second replica to lose, so you cannot observe data loss on broker failure or the effect of
`min.insync.replicas`. This row is labelled concept-only on purpose.

**What it means in enterprise.** With RF 1, a single broker loss can lose acked-but-unreplicated data.
Durability comes from `acks=all` + RF >= 3 + `min.insync.replicas=2`, so an acked write has reached a quorum
of replicas and survives one broker dying. The trade-off: higher RF costs capacity and throughput, and
`min.insync.replicas` too high can make the topic unavailable when a replica lags.

**Property block that changes in a real deployment**
```properties
# dev: RF 1, single broker (cannot demonstrate loss)
lab.topic.replication=1
spring.kafka.producer.properties[acks]=1
# prod: durable writes across a quorum of replicas
lab.topic.replication=3                      # topic-side, at creation / via admin
# min.insync.replicas=2                      # topic-side: ack requires 2 in-sync replicas
spring.kafka.producer.properties[acks]=all
```
