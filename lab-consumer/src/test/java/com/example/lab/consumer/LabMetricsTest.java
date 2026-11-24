package com.example.lab.consumer;

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
 * Task 2.12: the micrometer observation bean tags listener metrics with the lab
 * lane, and /lab/diagnostics answers the two where-is-my-data questions
 * (broker-side lag vs in-process queue).
 */
@SpringBootTest(properties = "lab.test.id=metrics")
@AutoConfigureMockMvc
class LabMetricsTest {

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
    void listenerMetricsCarryLaneTagAndDiagnosticsAnswerBothQuestions() throws Exception {
        harness.produce("lab-orders", "cust-m-1", LabPopTest.orderEvent("ord-m-1"));
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline && ledger.count() < 1) {
            Thread.sleep(250);
        }

        String metrics = mockMvc.perform(get("/lab/metrics"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        assertTrue(metrics.contains("spring.kafka.listener"),
                "listener observation metric must be present; got: " + metrics);
        assertTrue(metrics.contains("lab.lane=orders"),
                "the observation convention must tag the lab lane; got: " + metrics);

        String diagnostics = mockMvc.perform(get("/lab/diagnostics"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        assertTrue(diagnostics.contains("queueSize") && diagnostics.contains("ledgerCount"),
                "diagnostics must expose the in-process queue gauge; got: " + diagnostics);
        assertTrue(diagnostics.contains("groupOffsets") && diagnostics.contains("nextOffsets"),
                "diagnostics must expose the broker-side offsets; got: " + diagnostics);
    }
}
