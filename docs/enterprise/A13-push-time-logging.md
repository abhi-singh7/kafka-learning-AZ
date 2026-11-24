# A13 — Push-time logging on completion (async done-branch)

**What it is.** After the broker acks, the producer logs `push acknowledged: orderId=… topic=lab-orders
partition=p offset=o` in the send's completion future — not before the ack.

**Why it matters in enterprise.** Logging partition/offset *after* the ack is what makes those values
real (the broker told you where it went). Logging them before the ack would be a guess and could lie
about placement — critical when you later need to find or replay a specific record.

**Property block that changes in a real deployment**
```properties
# No property: this is logging in the completion future. In prod you ship these structured log
# lines (orderId, topic, partition, offset, traceId) to your log platform so "where did my record
# go?" is answerable from logs, not just /lab/pushed.
```
