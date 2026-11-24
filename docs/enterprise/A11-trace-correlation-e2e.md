# A11 — Correlation / trace id end-to-end

**What it is.** The push response carries a `traceId`; it is set as the `x-lab-trace` header on
`lab-orders` and re-appears in the consumer lifecycle log for that order. Producer-out value equals
consumer-in value.

**Why it matters in enterprise.** Trace/correlation ids stitch one logical operation across services
and hops, so you can follow a single order through produce → consume → side-effect without grepping
by timestamp. They belong in headers (metadata), not the payload.

**Property block that changes in a real deployment**
```properties
# No property: trace/correlation are per-record headers set in code. In prod you usually adopt a
# standard propagation scheme (e.g. W3C TraceContext / B3) and the same header-carrying mechanism,
# so ids line up with your central tracing backend.
```
