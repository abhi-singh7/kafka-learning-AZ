package com.example.orders.persistence;

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
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Task 3.4 RED: after a successful insert, order-persisted gains an ack record
 * keyed by orderId with the contract fields (result=persisted,
 * service=order-persistence-service). Embedded broker + real Postgres.
 */
@SpringBootTest
class PersistenceConsumerKafkaTest {

    static KafkaTestHarness harness;

    @BeforeAll
    static void startBroker() {
        harness = new KafkaTestHarness("order-created", "order-persisted");
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
    void handlingPublishesAckKeyedByOrderId() throws Exception {
        Thread.sleep(1500); // let the listener subscribe

        String orderId = "order-ack-p-1";
        String payload = """
                {"schemaVersion":"1","orderId":"%s","customerId":"acme","sku":"sku-a",
                 "qty":2,"price":9.99,"lineIndex":0,"messageId":"msg-p-1","createdAt":"2025-01-01T00:00:00Z"}
                """.formatted(orderId);
        harness.produce("order-created", orderId, payload);

        List<ConsumerRecord<String, String>> acks = consume("order-persisted", 1, 15_000);
        assertEquals(1, acks.size(), "expected one ack on order-persisted");

        ConsumerRecord<String, String> ack = acks.get(0);
        assertEquals(orderId, ack.key(), "ack key must equal orderId");

        var mapper = tools.jackson.databind.json.JsonMapper.builder().build();
        PersistedAck parsed = mapper.readValue(ack.value(), PersistedAck.class);
        assertEquals("1", parsed.schemaVersion());
        assertEquals(orderId, parsed.orderId());
        assertEquals("msg-p-1", parsed.messageId());
        assertEquals("sku-a", parsed.sku());
        assertEquals(2, parsed.qty());
        assertEquals("persisted", parsed.result());
        assertEquals("order-persistence-service", parsed.service());
        assertNotNull(parsed.at());
    }

    private List<ConsumerRecord<String, String>> consume(String topic, int maxRecords, long timeoutMs) {
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, harness.bootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + System.nanoTime(),
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
