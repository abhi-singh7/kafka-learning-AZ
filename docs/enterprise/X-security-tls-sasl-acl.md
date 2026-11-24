# X2 — Security: TLS, SASL/SCRAM, ACLs  [CONCEPT-ONLY, not demonstrated]

**Why it cannot be shown here.** The broker is PLAINTEXT with no authentication or authorization. There is
no TLS listener, no SASL mechanism, and no ACL to exercise, so this row is labelled concept-only.

**What it means in enterprise.** Three independent layers: **TLS** encrypts the wire; **SASL/SCRAM**
authenticates each client (who you are); **ACLs** authorize what a principal may do (which topics/groups,
read/write). Together they stop unauthenticated clients from reading/writing and let you lock down
broker-owned state (e.g. forbid app principals from writing `__consumer_offsets` — see A20).

**Property block that changes in a real deployment**
```properties
# dev: PLAINTEXT, no authn/authz (cannot demonstrate)
spring.kafka.bootstrap-servers=localhost:9092
# prod: encrypt + authenticate + authorize
spring.kafka.producer.properties[security.protocol]=SASL_SSL          # (consumer: same on consumer.properties)
spring.kafka.producer.properties[sasl.mechanism]=SCRAM-SHA-512
spring.kafka.producer.properties[sasl.jaas.config]=\
  org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="${KAFKA_SASL_USER}" password="${KAFKA_SASL_PASSWORD}";
spring.kafka.producer.properties[ssl.truststore.location]=/etc/kafka/secrets/truststore.jks
# ACLs are enforced on the broker (raft/KRaft authorizer), not in client properties.
```
