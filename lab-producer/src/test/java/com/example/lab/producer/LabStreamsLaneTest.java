package com.example.lab.producer;

import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Task 3.1 RED: the Kafka Streams lane in lab-producer.
 *
 * <p>Three orders for one customer pushed through {@code lab-pending} must show up as a
 * per-customer RUNNING TOTAL at {@code GET /lab/accepted} in push order (10, 30, 60) —
 * the stateful part — and {@code GET /lab/diagnostics} must show {@code lab-accepted}
 * offsets advanced. The lane is built with {@code @EnableKafkaStreams} +
 * {@code StreamsBuilder} + a stateless transform feeding a stateful running total,
 * all under the {@code lab-*} namespace (application-id from properties).
 */
@SpringBootTest
@AutoConfigureMockMvc
class LabStreamsLaneTest {

    static LabKafkaTestHarness harness;

    @BeforeAll
    static void startBroker() {
        harness = new LabKafkaTestHarness(3, "lab-orders", "lab-throttle", "lab-ack-a",
                "lab-ack-b", "lab-pending", "lab-accepted", "order-created", "order-email-sent");
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

    /** Pushes one pending order (customerId, amount) and expects 202 after async dispatch. */
    private void pushPending(String customerId, double amount) throws Exception {
        String body = "{\"customerId\":\"" + customerId + "\",\"amount\":" + amount + "}";
        MvcResult started = mockMvc.perform(post("/lab/pending")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(request().asyncStarted())
                .andReturn();
        mockMvc.perform(asyncDispatch(started))
                .andExpect(status().isAccepted());
    }

    /** Polls GET /lab/accepted until it holds at least {@code expected} entries. */
    private MvcResult acceptedUntil(int expected, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            MvcResult result = mockMvc.perform(get("/lab/accepted")).andReturn();
            tools.jackson.databind.JsonNode node =
                    new tools.jackson.databind.ObjectMapper().readTree(result.getResponse().getContentAsString());
            if (node.size() >= expected) {
                return result;
            }
            Thread.sleep(250);
        }
        throw new AssertionError("GET /lab/accepted never reached " + expected + " entries within "
                + timeoutMs + " ms");
    }

    /**
     * Task 3.1: the Kafka Streams lane in lab-producer.
     *
     * <p>Three pending orders for ONE customer are pushed through {@code lab-pending}; the
     * stateful per-customer running total must appear on {@code GET /lab/accepted} in push
     * order (10, 30, 60) — proving the stateful accumulator — and {@code GET /lab/diagnostics}
     * must show a {@code lab-accepted} partition offset advanced — proving the lane actually
     * wrote to the topic. Both assertions run in one flow against a single customer because the
     * lane is a single stateful pipeline (one broker, one Streams app, one consumer group,
     * one tracker) that cannot be cleanly partitioned per test method within one context.
     */
    @Test
    void streamsLaneRunningTotalAndAcceptedOffsetAdvance() throws Exception {
        pushPending("cust-0", 10.0);
        pushPending("cust-0", 20.0);
        pushPending("cust-0", 30.0);

        // (a) Stateful running total, in push order: 10 -> 30 -> 60.
        MvcResult accepted = acceptedUntil(3, 15_000);
        tools.jackson.databind.JsonNode node =
                new tools.jackson.databind.ObjectMapper().readTree(accepted.getResponse().getContentAsString());

        List<String> customerIds = new java.util.ArrayList<>();
        List<Double> totals = new java.util.ArrayList<>();
        for (int i = 0; i < node.size(); i++) {
            customerIds.add(node.get(i).get("customerId").asString());
            totals.add(node.get(i).get("runningTotal").asDouble());
        }
        assertEquals(List.of("cust-0", "cust-0", "cust-0"), customerIds);
        assertEquals(List.of(10.0, 30.0, 60.0), totals);

        // (b) The lane wrote to lab-accepted: some partition offset advanced in diagnostics.
        long deadline = System.currentTimeMillis() + 15_000;
        boolean advanced = false;
        while (System.currentTimeMillis() < deadline && !advanced) {
            MvcResult result = mockMvc.perform(get("/lab/diagnostics")).andReturn();
            tools.jackson.databind.JsonNode diag =
                    new tools.jackson.databind.ObjectMapper().readTree(result.getResponse().getContentAsString());
            for (int i = 0; i < diag.size(); i++) {
                if (!"lab-accepted".equals(diag.get(i).get("topic").asString())) {
                    continue;
                }
                tools.jackson.databind.JsonNode parts = diag.get(i).get("offsets");
                for (int p = 0; p < parts.size(); p++) {
                    long next = parts.get(p).get("nextOffset") == null
                            ? -1 : parts.get(p).get("nextOffset").asLong();
                    if (next >= 1) {
                        advanced = true;
                        break;
                    }
                }
            }
            if (!advanced) {
                Thread.sleep(250);
            }
        }
        assertTrue(advanced, "GET /lab/diagnostics never showed lab-accepted offsets advancing");
    }
}
