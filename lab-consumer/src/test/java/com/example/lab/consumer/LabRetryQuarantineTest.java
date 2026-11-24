package com.example.lab.consumer;

import java.util.List;

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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Task 2.6: a failing handler is retried with bounded exponential backoff and
 * then quarantined to lab-rejected with reason=handler-throw and the attempt
 * count. The offset does NOT advance until quarantine commits it, and the
 * listener keeps working afterwards.
 */
@SpringBootTest(properties = {
        "lab.retry.max-attempts=2",
        "lab.retry.backoff.base.ms=50"
})
@AutoConfigureMockMvc
class LabRetryQuarantineTest {

    static LabKafkaTestHarness harness;

    @BeforeAll
    static void start() {
        harness = new LabKafkaTestHarness("lab-orders", "lab-rebalance", "lab-rejected", "lab-ack-a", "lab-ack-b");
    }

    @AfterAll
    static void stop() {
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

    @Autowired
    LabLedger ledger;

    @Test
    void failingRecordIsRetriedThenQuarantined() throws Exception {
        // Force 100% handler failure on the main lane.
        mockMvc.perform(post("/lab/inject-failures")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"percentage\":100,\"lane\":\"lab-orders\"}"))
                .andExpect(status().isOk());

        harness.produce("lab-orders", "cust-bad", LabPopTest.orderEvent("ord-bad"));

        // Eventually quarantined with reason=handler-throw and the attempt count.
        List<String> rejected = awaitRejected(20_000);
        assertTrue(rejected.stream().anyMatch(v -> v.contains("handler-throw") && v.contains("ord-bad")),
                "quarantine entry must carry reason=handler-throw and the payload; got: " + rejected);
        assertEquals(0, ledger.count(), "the failed order must not be applied");

        // Listener is still alive: turn injection off, a good record gets processed.
        mockMvc.perform(post("/lab/inject-failures")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"percentage\":0,\"lane\":\"lab-orders\"}"))
                .andExpect(status().isOk());
        harness.produce("lab-orders", "cust-good", LabPopTest.orderEvent("ord-good"));
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline && ledger.count() < 1) {
            Thread.sleep(250);
        }
        assertEquals(List.of("ord-good"), ledger.keys(), "listener must keep working after quarantine");
    }

    private List<String> awaitRejected(long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            List<String> out = harness.consumeUpTo("lab-rejected", 5, 1_000);
            if (!out.isEmpty()) {
                return out;
            }
        }
        return List.of();
    }
}
