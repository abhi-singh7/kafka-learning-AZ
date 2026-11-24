package com.example.orders.email;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Task 2.4 RED: a corrupt record on order-created → one poison record on
 * order-failed with {reason, rawPayload, sourceTopic, detectedBy}; the corrupt
 * record is NOT redelivered in a loop (exactly one poison per attempt).
 */
@SpringBootTest
class EmailPoisonPathTest {

    static KafkaTestHarness harness;

    @BeforeAll
    static void startBroker() {
        harness = new KafkaTestHarness("order-created", "order-email-sent", "order-failed");
    }

    @AfterAll
    static void stopBroker() {
        if (harness != null) {
            harness.close();
        }
    }

    @DynamicPropertySource
    static void kafkaProps(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", () -> harness.bootstrapServers());
    }

    @Test
    void corruptPayloadGoesToOrderFailedAndIsNotRedelivered() throws Exception {
        Thread.sleep(1500);

        String corrupt = "%%% definitely not json %%%";
        harness.produce("order-created", "poison-key", corrupt);

        // allow time for one delivery + poison publish; then confirm no loop
        List<ConsumerRecord<String, String>> poisons = consume("order-failed", 1, 15_000);
        assertEquals(1, poisons.size(), "exactly one poison record expected");

        ConsumerRecord<String, String> p = poisons.get(0);
        var mapper = tools.jackson.databind.json.JsonMapper.builder().build();
        Map<?, ?> parsed = mapper.readValue(p.value(), Map.class);
        assertEquals("email-service", parsed.get("detectedBy"));
        assertEquals("order-created", parsed.get("sourceTopic"));
        assertTrue(String.valueOf(parsed.get("reason")).startsWith("json-decode"), "reason must be json-decode");
        assertEquals(corrupt, parsed.get("rawPayload"));

        // no redelivery loop: wait a bit more, expect no second poison for this payload
        Thread.sleep(2000);
        List<ConsumerRecord<String, String>> all = consumeFromStart("order-failed", 30_000);
        long matching = all.stream().filter(r -> r.value().contains(corrupt)).count();
        assertEquals(1, matching, "corrupt record must not be redelivered in a loop");
    }

    private List<ConsumerRecord<String, String>> consume(String topic, int maxRecords, long timeoutMs) {
        return consumeInternal(topic, maxRecords, timeoutMs);
    }

    private List<ConsumerRecord<String, String>> consumeFromStart(String topic, long timeoutMs) {
        // fresh group → reads from earliest
        return consumeInternal(topic, Integer.MAX_VALUE, timeoutMs);
    }

    private List<ConsumerRecord<String, String>> consumeInternal(String topic, int maxRecords, long timeoutMs) {
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, harness.bootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "poison-test-" + System.nanoTime(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class))) {
            c.subscribe(Collections.singleton(topic));
            List<ConsumerRecord<String, String>> out = new java.util.ArrayList<>();
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (out.size() < maxRecords && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = c.poll(Duration.ofMillis(250));
                for (ConsumerRecord<String, String> r : records) {
                    if (topic.equals(r.topic())) {
                        out.add(r);
                    }
                }
            }
            return out;
        }
    }
}
