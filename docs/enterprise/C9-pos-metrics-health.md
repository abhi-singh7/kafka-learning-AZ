# C9 — PoS endpoints, metrics, health (consumer)

**What it is.** The consumer's observability surface: `/lab/diagnostics` + `/lab/backlog` (offsets/lag),
`/lab/rejected` (quarantined records with raw payload + reason + `partition@offset`), `/lab/metrics`, and
`/lab/health` (broker reachability).

**Why it matters in enterprise.** You can answer "what did I reject and why?" and "how far behind am I?"
from endpoints, not logs. The rejected view is especially valuable: raw payload + reason lets you later
diagnose and re-drive a poisoned message.

**Property block that changes in a real deployment**
```properties
# No property: read-only surfaces over broker state + in-memory quarantine. In prod you point on-call
# tooling and alerting at these endpoints/metrics per consumer group.
```
