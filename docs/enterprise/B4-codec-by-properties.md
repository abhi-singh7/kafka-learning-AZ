# B4 — Codec by properties (String lane vs AVRO lane)

**What it is.** The serializer/deserializer is chosen by a property (`value-serializer` /
`value-deserializer`). The String lanes write raw bytes; the AVRO lane writes a Confluent envelope
and registers schemas in `_schemas`, gaining a new version id on evolution (Task 3.2).

**Why it matters in enterprise.** Codec is config, so you can move a topic from JSON to Avro by
changing properties + providing a registry — but the *consumer* must understand the new codec too.
Avro + a schema registry is what makes safe schema evolution possible (add fields with defaults).

**Property block that changes in a real deployment**
```properties
# dev: String lanes
spring.kafka.producer.properties[value.serializer]=org.apache.kafka.common.serialization.StringSerializer
# AVRO lane (both apps):
#   value.serializer / value.deserializer = io.confluent...KafkaAvro{Ser,De}serializer
#   schema.registry.url = http://localhost:8081   (dev)  ->  a real registry cluster (prod)
lab.kafka.schema-registry-url=http://localhost:8081
```
