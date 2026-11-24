# A15b — Quarantine lane ≠ filter

**What it is.** A failed or undecodable record lands on `lab-rejected` with its **raw payload + reason**
(e.g. `json-decode`, `handler-throw`) and the source `partition@offset`; it is then acked so it isn't
replayed forever. View: `GET /lab/rejected`.

**Why it matters in enterprise.** Quarantine is a *failure* path (retry, then park + reason), distinct from
a *filter* (deliberate skip). Parking raw bytes + reason is what lets you later diagnose and re-drive the
poisoned message instead of silently dropping it or looping on it.

```properties
# No property: quarantine is a recoverer bean writing to lab-rejected. In prod you alert on rejected
# count per lane and build a re-drive tool that reads raw payload + reason from /lab/rejected.
```
