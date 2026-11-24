package com.example.lab.producer;

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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Task 1.4 RED: produce to {@code order-created} with key = orderId.
 * Full Spring context against an embedded KRaft broker (hermetic).
 * After POST /orders, the topic must contain N records whose KEY equals the
 * generated orderId and whose payload round-trips through Jackson 3.
 */
@SpringBootTest
@AutoConfigureMockMvc
class LabPushTest {

    static LabKafkaTestHarness harness;

    @BeforeAll
    static void startBroker() {
        harness = new LabKafkaTestHarness(3, "lab-orders", "lab-throttle", "lab-ack-a", "lab-ack-b", "lab-pending", "lab-accepted");
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
        registry.add("spring.kafka.streams.state-dir", () -> harness.stateDir());
    }

    @Autowired
    MockMvc mockMvc;

    @Test
    void keyedPushUsesCustomerIdKeyAndPushResultCoordinates() throws Exception {
        String body = """
                {"customerId":"acme","items":[{"sku":"a","qty":1,"price":1.0},{"sku":"b","qty":2,"price":2.0}]}
                """;

        var async = mockMvc.perform(post("/lab/orders").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(request().asyncStarted())
                .andReturn();

        var dispatched = mockMvc.perform(asyncDispatch(async))
                .andExpect(status().isAccepted())
                .andReturn();
        String response = dispatched.getResponse().getContentAsString();
        String location = dispatched.getResponse().getHeader("Location");

        String orderId = com.jayway.jsonpath.JsonPath.parse(response).read("$.orderId", String.class);
        Integer locPartition = com.jayway.jsonpath.JsonPath.parse(response).read("$.partition", Integer.class);
        Long locOffset = com.jayway.jsonpath.JsonPath.parse(response).read("$.offset", Long.class);
        assertNotNull(locPartition, "partition must come from push result");
        assertNotNull(locOffset, "offset must come from push result");
        assertEquals("lab-orders/" + locPartition + "@" + locOffset, location,
                "Location header names the push-result coordinates");


        List<ConsumerRecord<String, String>> records = consume("lab-orders", 2, 15_000);
        assertEquals(2, records.size(), "expected 2 line events on lab-orders");

        
        for (ConsumerRecord<String, String> r : records) {
            assertEquals("acme", r.key(), "message key must equal the customerId (kafka-contract)");
            assertEquals(locPartition, r.partition(), "partition must match push result");
            assertEquals(locOffset, r.offset(), "offset must match push result (first record)");
            locOffset = locOffset + 1;
        }

        // payload round-trips through Jackson 3 with all nine contract fields
        var mapper = tools.jackson.databind.json.JsonMapper.builder().build();
        for (ConsumerRecord<String, String> r : records) {
            OrderLineEvent event = mapper.readValue(r.value(), OrderLineEvent.class);
            assertEquals("1", event.schemaVersion());
            assertEquals(orderId, event.orderId());
            assertNotNull(event.messageId());
            assertNotNull(event.deliveredAt());
        }

        // headers must be observable without deserialising the payload (routing rule)
        var headerKeys = harness.consumeHeaderKeys("lab-orders", 2, 15_000);
        assertEquals(2, headerKeys.size(), "header keys readable for both records");
        for (java.util.List<String> keys : headerKeys) {
            assertTrue(keys.contains("x-lab-trace"), "trace header must travel");
            assertTrue(keys.contains("x-lab-correlation"), "orderId correlation header must travel");
            assertTrue(keys.contains("x-lab-schema"), "schema-version header must travel");
            assertTrue(keys.contains("__TypeId__"),
                    "Boot Jackson serializers add their own type header alongside ours");
        }

    }

    @Test
    void keyedLanePinsOneCustomerToOnePartitionUnkeyedLaneCarriesNoKey() throws Exception {
        String body = """
                {"customerId":"cust-0","items":[{"sku":"a","qty":1,"price":1.0}]}
                """;
        var mapper = tools.jackson.databind.json.JsonMapper.builder().build();
        java.util.ArrayList<Integer> partitions = new java.util.ArrayList<>();
        java.util.ArrayList<Long> offsets = new java.util.ArrayList<>();
        for (int i = 0; i < 3; i++) {
            var started = mockMvc.perform(post("/lab/orders")
                    .contentType(MediaType.APPLICATION_JSON).content(body))
                    .andExpect(request().asyncStarted()).andReturn();
            String json = mockMvc.perform(asyncDispatch(started))
                    .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString();
            LabPushAckDto ack = mapper.readValue(json, LabPushAckDto.class);
            partitions.add(ack.partition());
            offsets.add(ack.offset());
        }
        for (int i = 1; i < partitions.size(); i++) {
            assertEquals(partitions.get(0), partitions.get(i),
                    "three pushes for cust-0 on the keyed lane share one partition");
        }
        for (int i = 1; i < offsets.size(); i++) {
            assertTrue(offsets.get(i) > offsets.get(i - 1),
                    "offsets increase in push order on the keyed lane");
        }

        java.util.ArrayList<String> unkeyedIds = new java.util.ArrayList<>();
        for (int i = 0; i < 2; i++) {
            var started = mockMvc.perform(post("/lab/orders/unkeyed")
                    .contentType(MediaType.APPLICATION_JSON).content(body))
                    .andExpect(request().asyncStarted()).andReturn();
            String json = mockMvc.perform(asyncDispatch(started))
                    .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString();
            LabPushAckDto ack = mapper.readValue(json, LabPushAckDto.class);
            unkeyedIds.add(ack.orderId());
        }

        java.util.List<ConsumerRecord<String, String>> all = consume("lab-orders", 10, 20_000);
        java.util.ArrayList<String> unkeyedIdsOnWire = new java.util.ArrayList<>();
        for (ConsumerRecord<String, String> r : all) {
            if (r.key() == null) {
                OrderLineEvent e = mapper.readValue(r.value(), OrderLineEvent.class);
                assertEquals("cust-0", e.customerId(), "unkeyed record is the same customer payload");
                unkeyedIdsOnWire.add(e.orderId());
            }
        }
        assertEquals(2, unkeyedIdsOnWire.size(),
                "unkeyed lane publishes with a null key on the wire");
        assertEquals(unkeyedIds, unkeyedIdsOnWire, "both unkeyed pushes reached the topic");
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
