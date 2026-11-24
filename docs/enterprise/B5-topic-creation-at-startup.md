# B5 — Topic creation at startup; "topic exists ⇒ bean ignored"

**What it is.** Boot's `KafkaAdmin` creates the `NewTopic` beans on startup, but *skips* any topic
that already exists. First start creates the `lab-*` topics; a second start logs nothing new.

**Why it matters in enterprise.** Idempotent bootstrap makes restarts safe — but it also means a
topic that was created earlier with the *wrong* partition count or RF is silently reused, not
corrected. Verify topology (partitions/RF) before trusting an app that "auto-creates" topics.

**Property block that changes in a real deployment**
```properties
# dev: auto-create via NewTopic beans (lab.topic.* + lab.topic.replication)
# prod: topic lifecycle is usually owned by an external IaC/admin step, not the app —
# you disable app-side creation and reconcile topology out-of-band so a bad earlier
# create cannot be silently inherited.
lab.topic.replication=1   # dev ; prod topic RF is managed by the platform, not the app
```
