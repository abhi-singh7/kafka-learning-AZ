# A16 — Profiles dev vs prod (TLS/SASL/multi-host block)

**What it is.** `application.properties` is the PLAINTEXT single-host dev shape;
`application-prod.properties` carries a **commented** TLS/SASL/ACL + multi-broker block naming what
this box cannot show.

**Why it matters in enterprise.** The prod delta (security, multiple brokers, real RF) must be
documented *next to the code* so the deployment shape is not tribal knowledge. The commented block is
that documentation: it is loadable guidance, not a claim the dev box has any of it.

**Property block that changes in a real deployment**
```properties
# See application-prod.properties (both apps) for the full commented block:
#   multi-broker bootstrap, SASL_SSL + SCRAM-SHA-512, truststore/keystore, acks=all, RF>=3.
# Activated with --spring.profiles.active=prod and the corresponding env vars.
```
