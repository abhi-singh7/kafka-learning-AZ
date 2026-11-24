package com.example.lab.producer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Phase 8 smoke finding: /lab/diagnostics must tolerate a "contrast" topic that does not exist on
 * the broker. The live broker only has order-created (not order-email-sent) until the sibling apps
 * create it, so a missing extra topic must be skipped gracefully, not turn the whole query into a 500.
 * This context points lab.diagnostics.extra.topics at a topic the harness does NOT create.
 */
@SpringBootTest
@AutoConfigureMockMvc
class LabDiagnosticsMissingTopicTest {

    static LabKafkaTestHarness harness;

    @BeforeAll
    static void startBroker() {
        // Only the six lab topics exist here - order-email-sent is deliberately absent.
        harness = new LabKafkaTestHarness(3, "lab-orders", "lab-throttle", "lab-ack-a",
                "lab-ack-b", "lab-pending", "lab-accepted");
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
        // A contrast topic that is NOT present on this broker.
        registry.add("lab.diagnostics.extra.topics", () -> "order-email-sent");
    }

    @Autowired
    MockMvc mockMvc;

    @Test
    void diagnosticsToleratesAMissingContrastTopic() throws Exception {
        String body = mockMvc.perform(get("/lab/diagnostics"))
                .andExpect(status().isOk())   // must be 200, not 500
                .andReturn().getResponse().getContentAsString();
        assertTrue(body.contains("lab-orders"), "existing lab topics are still reported");
    }
}
