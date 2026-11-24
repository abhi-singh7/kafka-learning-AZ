# A18 — Metrics: Boot metrics + Micrometer (+ listener labels)

**What it is.** The consumer exposes `/lab/metrics`; `spring.kafka.listener.*` meters carry a
`lab.lane=orders` tag. Because spring-kafka 4.x's observation-convention hook is not reachable in this
build, the tag is applied via a `MeterFilter` (documented honestly rather than faked).

**Why it matters in enterprise.** Per-listener labels let one dashboard distinguish *which* consumer
group/lane is lagging or erroring. Untagged listener metrics blur distinct lanes together and make
incidents harder to localize.

**Property block that changes in a real deployment**
```properties
# No property: the tag is applied by a MeterFilter bean. In prod you feed these tagged meters to your
# monitoring stack and alert per-lane (e.g. consumer-records-lag per group) instead of one aggregate.
```
