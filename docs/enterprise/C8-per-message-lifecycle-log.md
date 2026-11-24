# C8 — Per-message lifecycle log

**What it is.** One log line per message: `topic · partition p · offset o · key · headers · payload ·
decision`, where decision is accept / skip-duplicate / reject-quarantine / retry.

**Why it matters in enterprise.** A per-message audit line with an explicit *decision* makes at-least-once
debugging tractable: you can see exactly why each record was handled the way it was, which is essential
when redelivery and quarantine are in play.

**Property block that changes in a real deployment**
```properties
# No property: this is structured logging per record. In prod you ship these lines (topic/partition/
# offset/key/decision) to your log platform so a single orderId can be traced end to end.
```
