package com.example.lab.consumer;

import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Task 2.1 RED: continuous pop. Cold start with an existing backlog processes the
 * backlog; the JVM stays alive after drain; a new push while alive is processed
 * without restarting the app. Group {@code lab-consumer} on {@code lab-orders}.
 */
@SpringBootTest(properties = "lab.test.id=pop")
@AutoConfigureMockMvc
class LabPopTest {

    static LabKafkaTestHarness harness;

    @BeforeAll
    static void startBrokerWithBacklog() {
        // Backlog exists BEFORE the Spring context (and its listeners) start:
        // a cold start must process these.
        harness = new LabKafkaTestHarness(3, "lab-orders", "lab-rebalance", "lab-rejected", "lab-ack-a", "lab-ack-b");
        harness.produce("lab-orders", "cust-1", orderEvent("ord-backlog-1"));
        harness.produce("lab-orders", "cust-1", orderEvent("ord-backlog-2"));
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
    void coldStartProcessesBacklogAndStaysAlive() throws Exception {
        // Cold start: the two backlog records must be processed by now.
        String diagnostics = awaitLedgerCount(2);
        assertTrue(diagnostics.contains("lab-orders"), "diagnostics must name the topic it reads");

        // Alive after drain: the app is still serving and polling.
        mockMvc.perform(get("/lab/health")).andExpect(status().isOk());

        // New push while alive is processed without a restart.
        harness.produce("lab-orders", "cust-2", orderEvent("ord-live"));
        awaitLedgerCount(3);
    }

    /** Polls /lab/diagnostics until the ledger holds at least {@code expected} orders. */
    private String awaitLedgerCount(int expected) throws Exception {
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline) {
            String body = mockMvc.perform(get("/lab/diagnostics"))
                    .andExpect(status().isOk())
                    .andReturn().getResponse().getContentAsString();
            int count = com.jayway.jsonpath.JsonPath.parse(body).read("$.ledgerCount", Integer.class);
            if (count >= expected) {
                return body;
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
