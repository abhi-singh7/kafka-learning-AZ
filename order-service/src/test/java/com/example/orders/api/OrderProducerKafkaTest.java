package com.example.orders.api;

import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Task 1.4 RED: produce to {@code order-created} with key = orderId.
 * Full Spring context against an embedded KRaft broker (hermetic).
 * After POST /orders, the topic must contain N records whose KEY equals the
 * generated orderId and whose payload round-trips through Jackson 3.
 */
@SpringBootTest
@AutoConfigureMockMvc
class OrderProducerKafkaTest {

    static KafkaTestHarness harness;

    @BeforeAll
    static void startBroker() {
        harness = new KafkaTestHarness("order-created");
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

    @Autowired
    MockMvc mockMvc;

    @Test
    void postOrderProducesOneRecordPerLineKeyedByOrderId() throws Exception {
        String body = """
                {"customerId":"acme","items":[{"sku":"a","qty":1,"price":1.0},{"sku":"b","qty":2,"price":2.0}]}
                """;

        String response = mockMvc.perform(post("/orders").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();

        String orderId = com.jayway.jsonpath.JsonPath.parse(response).read("$.orderId", String.class);

        List<ConsumerRecord<String, String>> records = consume("order-created", 2, 15_000);
        assertEquals(2, records.size(), "expected 2 line events on order-created");

        for (ConsumerRecord<String, String> r : records) {
            assertEquals(orderId, r.key(), "message key must equal the orderId");
        }

        // payload round-trips through Jackson 3 with all nine contract fields
        var mapper = tools.jackson.databind.json.JsonMapper.builder().build();
        for (ConsumerRecord<String, String> r : records) {
            OrderLineEvent event = mapper.readValue(r.value(), OrderLineEvent.class);
            assertEquals("1", event.schemaVersion());
            assertEquals(orderId, event.orderId());
            assertNotNull(event.messageId());
            assertNotNull(event.createdAt());
        }

        // lineIndex is contiguous 0..N-1
        List<Integer> indexes = records.stream().map(r -> {
            try {
                return mapper.readValue(r.value(), OrderLineEvent.class).lineIndex();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).sorted().toList();
        assertEquals(List.of(0, 1), indexes);
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
