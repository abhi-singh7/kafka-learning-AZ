# C6 — Filter bean by header ⇒ filtered-out **is** acked

**What it is.** A `RecordFilterStrategy` discards records with `x-lab-skip=true` *before* deserialization,
and the discarded record's offset **advances** (it is not re-read). The skip decision is logged.

**Why it matters in enterprise.** A deliberate "not for me" skip must commit so it isn't replayed forever —
that is fundamentally different from a *failure*, which should retry/quarantine. Conflating the two causes
either lost skips or infinite replays.

**Property block that changes in a real deployment**
```properties
# No property: the filter is a bean (skipFilter). In prod you extend it with more routing headers
# (tenant/region) and keep ackDiscarded=true so filtered records are committed, not looped.
```
