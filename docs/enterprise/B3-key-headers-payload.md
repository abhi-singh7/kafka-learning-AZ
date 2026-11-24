# B3 — Key, headers, payload separation

**What it is.** A record has three independent parts: the routing **key**, opaque **headers**
(`x-lab-trace`, `x-lab-correlation`, `x-lab-schema`), and the serialized **payload**. The consumer
lifecycle log prints all three; the rule is *route before deserialize*.

**Why it matters in enterprise.** Headers carry cross-cutting metadata (trace, correlation, schema
version) that survives re-serialization and can be used for routing/filtering without parsing the
body. Keeping them out of the payload means a consumer can make decisions (skip, route, correlate)
before paying to decode.

**Property block that changes in a real deployment**
```properties
# No property: headers are set per-record in code (LabRecord). In prod you typically add
# more contract headers (tenant, region, schema-version) and may use them for header-based
# routing/filtering — the mechanism is identical, only the header names grow.
```
