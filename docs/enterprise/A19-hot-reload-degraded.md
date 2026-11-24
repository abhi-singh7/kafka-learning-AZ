# A19 — Hot reload while messages flow  [DEGRADED]

**Status: not demonstrated.** `spring-boot-devtools` is not on the classpath (JDK 25 / Boot 4.1.2
build), so there is no live-reload of behaviour under load. The fallback is *restart without drain*.
This row is labelled degraded on purpose — never silently faked.

**Why it matters in enterprise.** The lesson is that configuration/behaviour can change while messages
are still flowing; DevTools demonstrates it on a dev box, but production changes ship as releases with
a deploy/restart. Do not treat "hot reload" as a prod capability.

**Property block that changes in a real deployment**
```properties
# None — deliberately absent. If you add spring-boot-devtools for local dev only, remember it is
# stripped from the packaged jar and must never be relied on in prod.
```
