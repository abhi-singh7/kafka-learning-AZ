package com.example.lab.consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

/**
 * Test-only embedded Kafka broker (hermetic, no docker). One instance per test.
 * Provides raw produce/consume helpers so tests never need the full Spring context.
 */
public final class LabKafkaTestHarness implements AutoCloseable {

    private final EmbeddedKafkaBroker broker;

    /** Starts a single-broker KRaft cluster and creates the given topics (1 partition each). */
    public LabKafkaTestHarness(String... topics) {
        this(1, topics);
    }

    /** Starts a single-broker KRaft cluster with N partitions per topic. */
    public LabKafkaTestHarness(int partitionCount, String... topics) {
        this.broker = new EmbeddedKafkaKraftBroker(1, partitionCount, topics);
        this.broker.afterPropertiesSet();
        createTransactionStateTopic();
    }

    /**
     * A single-broker cluster cannot auto-create __transaction_state: the broker default is
     * 50 partitions / RF=3 and RF=3 can never be satisfied with one broker, so every
     * InitProducerId hangs forever on the failing auto-create. Pre-create it with the same
     * 50-partition layout but RF=1 so transactional producers get a live coordinator.
     */
    private void createTransactionStateTopic() {
        try (org.apache.kafka.clients.admin.AdminClient admin = org.apache.kafka.clients.admin.AdminClient.create(
                java.util.Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString()))) {
            waitUntilReady(admin);
            try {
                admin.createTopics(java.util.List.of(new org.apache.kafka.clients.admin.NewTopic(
                        "__transaction_state", 50, (short) 1)
                        .configs(java.util.Map.of("cleanup.policy", "compact")))).all().get(60, java.util.concurrent.TimeUnit.SECONDS);
            } catch (Exception e) {
                // Already exists with the same layout — fine.
            }
        }
    }

    /**
     * A freshly created KRaft cluster can open its listener port long before the controller is
     * ready; with several clusters created back-to-back in one JVM that gap can exceed a minute.
     * Block until the controller answers so tests start against a live cluster.
     */
    private static void waitUntilReady(org.apache.kafka.clients.admin.AdminClient admin) {
        long deadline = System.currentTimeMillis() + 180_000;
        while (System.currentTimeMillis() < deadline) {
            try {
                admin.describeCluster().controller().get(5, java.util.concurrent.TimeUnit.SECONDS);
                return;
            } catch (Exception e) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("interrupted waiting for embedded broker", ie);
                }
            }
        }
        throw new IllegalStateException("embedded KRaft cluster did not become ready in time");
    }
    public String bootstrapServers() {
        return broker.getBrokersAsString();
    }

    /** Sends one string message to topic with the given key. */
    public void produce(String topic, String key, String value) {
        try (KafkaProducer<String, String> p = new KafkaProducer<>(java.util.Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class))) {
            p.send(new ProducerRecord<>(topic, key, value)).get(10, java.util.concurrent.TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("produce failed", e);
        }
    }

    /** Sends one string message with the given headers. */
    public void produceWithHeaders(String topic, String key, String value, java.util.Map<String, String> headers) {
        try (KafkaProducer<String, String> p = new KafkaProducer<>(java.util.Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class))) {
            org.apache.kafka.common.header.internals.RecordHeaders rh = new org.apache.kafka.common.header.internals.RecordHeaders();
            headers.forEach((k, v) -> rh.add(k, v.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
            p.send(new ProducerRecord<>(topic, null, key, value, rh)).get(10, java.util.concurrent.TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("produce failed", e);
        }
    }

    /** Blocks up to timeoutMs for one message on topic; returns its value or null. */
    public String consumeOne(String topic, long timeoutMs) {
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(java.util.Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "harness-" + System.nanoTime(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                // D-10: read with read_committed — aborted-txn data sits above the last stable
                // offset and must stay invisible (Kafka 4.x keeps it in the log at runtime).
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
            c.subscribe(Collections.singleton(topic));
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = c.poll(Duration.ofMillis(250));
                for (ConsumerRecord<String, String> r : records) {
                    if (topic.equals(r.topic())) {
                        return r.value();
                    }
                }
            }
            return null;
        }
    }

    /** Consumes up to maxRecords messages from a topic (for asserting message counts). */
    public List<String> consumeUpTo(String topic, int maxRecords, long timeoutMs) {
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(java.util.Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "harness-" + System.nanoTime(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                // D-10: read with read_committed — aborted-txn data sits above the last stable
                // offset and must stay invisible (Kafka 4.x keeps it in the log at runtime).
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
            c.subscribe(Collections.singleton(topic));
            List<String> out = new java.util.ArrayList<>();
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (out.size() < maxRecords && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = c.poll(Duration.ofMillis(250));
                for (ConsumerRecord<String, String> r : records) {
                    if (topic.equals(r.topic())) {
                        out.add(r.value());
                    }
                }
            }
            return out;
        }
    }

    /** Consume up to maxRecords messages, exposing partition/offset/key/value so tests can assert routing. */
    public List<LinePart> consumeParts(String topic, int maxRecords, long timeoutMs) {
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(java.util.Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "harness-" + System.nanoTime(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                // D-10: read with read_committed — aborted-txn data sits above the last stable
                // offset and must stay invisible (Kafka 4.x keeps it in the log at runtime).
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
            c.subscribe(Collections.singleton(topic));
            List<LinePart> out = new java.util.ArrayList<LinePart>();
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (out.size() < maxRecords && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = c.poll(Duration.ofMillis(250));
                for (ConsumerRecord<String, String> r : records) {
                    if (topic.equals(r.topic())) {
                        out.add(new LinePart(r.partition(), r.offset(), r.key(), r.value()));
                    }
                }
            }
            return out;
        }
    }

    @Override
    public void close() {
        broker.destroy();
    }
}
