# A20 — Broker-owned state read-only (`__consumer_offsets`, `_schemas`)

**What it is.** Diagnostics report `hasConsumerOffsetsTopic:true` and `hasSchemasTopic:true`; **no**
application write path touches either topic. Offset commits and registered schemas are owned by the
broker/registry, not the apps.

**Why it matters in enterprise.** Applications *read* broker-owned state for diagnostics but must never
write to it directly — offset management is the consumer-group coordinator's job and schema storage is
the registry's. Writing to `__consumer_offsets` or `_schemas` by hand corrupts group/registry state.

**Property block that changes in a real deployment**
```properties
# No property: this is a discipline, not a config. In prod you also enforce it with ACLs so the app's
# principals cannot write to __consumer_offsets / _schemas (see X-security-tls-sasl-acl).
```
