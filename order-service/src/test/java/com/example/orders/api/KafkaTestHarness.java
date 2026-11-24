package com.example.orders.api;

import java.time.Duration;
import java.util.Collections;
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
public final class KafkaTestHarness implements AutoCloseable {

    private final EmbeddedKafkaBroker broker;

    /** Starts a single-broker KRaft cluster and creates the given topics (1 partition each). */
    public KafkaTestHarness(String... topics) {
        this.broker = new EmbeddedKafkaKraftBroker(1, 1, topics);
        this.broker.afterPropertiesSet();
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

    /** Blocks up to timeoutMs for one message on topic; returns its value or null. */
    public String consumeOne(String topic, long timeoutMs) {
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(java.util.Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "harness-" + System.nanoTime(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
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

    @Override
    public void close() {
        broker.destroy();
    }
}
