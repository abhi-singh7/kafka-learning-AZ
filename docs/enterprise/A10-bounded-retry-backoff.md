# A10 — Bounded retry + exponential backoff, then quarantine

**What it is.** A `CommonErrorHandler` (retry budget + exponential backoff) retries a failing record a
bounded number of times, then hands it to a recoverer that writes it to `lab-rejected` and commits.
Trigger: `POST /lab/inject-failures {"percentage":30,"lane":"lab-orders"}`.

**Why it matters in enterprise.** Unbounded retry poisons the group (one bad record blocks the
partition forever); un-quarantined failures clog offsets. A *budget* plus a *quarantine sink* bounds
the blast radius and lets you diagnose/re-drive later.

**Property block that changes in a real deployment**
```properties
# dev: small budget for fast observable retries
lab.retry.max-attempts=3
lab.retry.backoff.base.ms=100
# prod: larger budget + longer backoff to ride out transient infra, still bounded
# lab.retry.max-attempts=5
# lab.retry.backoff.base.ms=500
```
