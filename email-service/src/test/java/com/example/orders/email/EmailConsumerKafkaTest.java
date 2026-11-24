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
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Task 2.3 RED: after handling a line event, order-email-sent gains an ack
 * record keyed by orderId with the contract fields; the offset advances (ACK).
 * Uses an embedded KRaft broker (hermetic).
 */
@SpringBootTest
class EmailConsumerKafkaTest {

    static KafkaTestHarness harness;

    @BeforeAll
    static void startBroker() {
        harness = new KafkaTestHarness("order-created", "order-email-sent");
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
        // give the listener a moment to subscribe
        Thread.sleep(1500);

        String orderId = "order-ack-1";
        String payload = """
                {"schemaVersion":"1","orderId":"%s","customerId":"acme","sku":"sku-a",
                 "qty":2,"price":9.99,"lineIndex":0,"messageId":"msg-1","createdAt":"2025-01-01T00:00:00Z"}
                """.formatted(orderId);
        harness.produce("order-created", orderId, payload);

        List<ConsumerRecord<String, String>> acks = consume("order-email-sent", 1, 15_000);
        assertEquals(1, acks.size(), "expected one ack on order-email-sent");

        ConsumerRecord<String, String> ack = acks.get(0);
        assertEquals(orderId, ack.key(), "ack key must equal orderId");

        var mapper = tools.jackson.databind.json.JsonMapper.builder().build();
        EmailSentAck parsed = mapper.readValue(ack.value(), EmailSentAck.class);
        assertEquals("1", parsed.schemaVersion());
        assertEquals(orderId, parsed.orderId());
        assertEquals("msg-1", parsed.messageId());
        assertEquals("sku-a", parsed.sku());
        assertEquals(2, parsed.qty());
        assertEquals("email-sent", parsed.result());
        assertEquals("email-service", parsed.service());
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
