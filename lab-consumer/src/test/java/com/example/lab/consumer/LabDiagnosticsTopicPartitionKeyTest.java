package com.example.lab.consumer;

import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Phase-8 smoke finding (C7): /lab/diagnostics keyed both {@code lab-orders} and
 * {@code lab-rebalance} by bare partition number, so the empty rebalance partition 0
 * clobbered lab-orders' real end-offset (committed 10 > end 0 — self-contradictory).
 * Offsets must be keyed by topic@partition so two topics can never overwrite each other,
 * and a committed offset must never exceed its own end-offset.
 */
@SpringBootTest(properties = "lab.test.id=diagkey")
@AutoConfigureMockMvc
class LabDiagnosticsTopicPartitionKeyTest {

    static LabKafkaTestHarness harness;

    @BeforeAll
    static void startBroker() {
        harness = new LabKafkaTestHarness(3, "lab-orders", "lab-rebalance", "lab-rejected", "lab-ack-a", "lab-ack-b");
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
    @SuppressWarnings("unchecked")
    void diagnosticsKeysOffsetsByTopicAndPartition() throws Exception {
        // Same key on both topics => same numeric partition, so a bare-partition key would clobber.
        harness.produce("lab-orders", "k1", orderEvent("o1"));
        harness.produce("lab-orders", "k1", orderEvent("o2"));
        harness.produce("lab-rebalance", "k1", "rebal-1");

        String body = awaitLedgerCount(2);

        assertTrue(body.contains("lab-orders@"),
                "diagnostics must key offsets by topic@partition (not bare partition number); got: " + body);

        Map<String, Object> next = com.jayway.jsonpath.JsonPath.parse(body).read("$.nextOffsets");
        Map<String, Object> group = com.jayway.jsonpath.JsonPath.parse(body).read("$.groupOffsets");
        assertTrue(group.keySet().stream()
                        .allMatch(k -> {
                            Object n = next.get(k);
                            return n == null || ((Number) n).longValue() >= ((Number) group.get(k)).longValue();
                        }),
                "committed offset must never exceed its own end-offset; next=" + next + " group=" + group);
    }

    private String awaitLedgerCount(int expected) throws Exception {
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline) {
            String b = mockMvc.perform(get("/lab/diagnostics"))
                    .andExpect(status().isOk())
                    .andReturn().getResponse().getContentAsString();
            int count = com.jayway.jsonpath.JsonPath.parse(b).read("$.ledgerCount", Integer.class);
            if (count >= expected) {
                return b;
            }
            Thread.sleep(250);
        }
        throw new AssertionError("ledger never reached " + expected + " orders within 20s");
    }

    static String orderEvent(String orderId) {
        return """
                {"schemaVersion":"1","orderId":"%s","customerId":"cust-1","sku":"a","qty":1,"price":9.5,
                 "messageId":"m-%s","traceId":"t-%s","deliveredAt":"2026-01-01T00:00:00Z"}
                """.formatted(orderId, orderId, orderId);
    }
}
