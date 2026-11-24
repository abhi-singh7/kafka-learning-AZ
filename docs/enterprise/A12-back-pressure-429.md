# A12 — Back-pressure 429 + Retry-After

**What it is.** When the bounded producer queue refuses work, `/lab/throttle` answers HTTP **429**
with `Retry-After: 1` and reports accepted vs rejected counts. (Same mechanism as B7.)

**Why it matters in enterprise.** The response must carry a *machine-readable* retry hint, not just an
error, or callers stampede on retry. A 429 + Retry-After lets the client back off cooperatively instead
of hammering an overloaded producer.

**Property block that changes in a real deployment**
```properties
# dev: fixed 1s hint
lab.producer.throttle.retry.after=1
# prod: derive Retry-After from measured load / a circuit-breaker state so the hint reflects
# how long the client should actually wait.
```
