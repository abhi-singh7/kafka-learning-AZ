# A15 — PoS endpoints + metrics + audit trail

**What it is.** The producer exposes `/lab/pushed` (every acked push with topic/partition/offset),
`/lab/diagnostics` (per-partition next-offsets), and `/lab/metrics`. Together they are an audit trail
of *what was pushed where* plus live counters.

**Why it matters in enterprise.** Observability is a first-class surface: you answer "where is my data?"
and "am I keeping up?" from endpoints and metrics, not a debugger. An audit trail of acked placements
is what makes post-hoc replay and incident forensics possible.

**Property block that changes in a real deployment**
```properties
# No property: these are read-only surfaces over broker state + in-memory acks. In prod you point
# your dashboard/alerting at the metrics (Micrometer) and treat /lab/diagnostics as an on-call tool.
```
