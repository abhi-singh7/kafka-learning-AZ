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
 * Phase-8 smoke finding (C9): /lab/rejected used {@code template.receive(topic, 0, 0, 1500ms)} —
 * a single fixed-offset read on a fresh transient consumer that could not bootstrap within the
 * timeout, so it returned empty even though quarantined records were sitting on lab-rejected.
 * The endpoint must return every record currently on lab-rejected (from the beginning).
 */
@SpringBootTest(properties = "lab.test.id=rejread")
@AutoConfigureMockMvc
class LabRejectedReadTest {

    static LabKafkaTestHarness harness;

    @BeforeAll
    static void startBroker() {
        harness = new LabKafkaTestHarness(1, "lab-orders", "lab-rebalance", "lab-rejected", "lab-ack-a", "lab-ack-b");
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
    void rejectedEndpointReturnsEveryRecordOnTheTopic() throws Exception {
        // Two quarantined-shaped records: a correct reader returns BOTH. The buggy
        // template.receive(topic, 0, 0, ...) can only ever read the single record at offset 0.
        harness.produce("lab-rejected", "k1",
                "{\"reason\":\"handler-throw\",\"rawPayload\":\"x\",\"sourceTopic\":\"lab-orders\"," +
                        "\"sourcePartition\":0,\"sourceOffset\":7,\"marker\":\"first-marker\"}");
        harness.produce("lab-rejected", "k2",
                "{\"reason\":\"handler-throw\",\"rawPayload\":\"y\",\"sourceTopic\":\"lab-orders\"," +
                        "\"sourcePartition\":0,\"sourceOffset\":8,\"marker\":\"second-marker\"}");

        String body = awaitRejectedContainsBoth("first-marker", "second-marker");
        assertTrue(body.contains("first-marker") && body.contains("second-marker"),
                "rejected endpoint must expose every record on lab-rejected; got: " + body);
    }

    private String awaitRejectedContainsBoth(String a, String b) throws Exception {
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline) {
            String resp = mockMvc.perform(get("/lab/rejected"))
                    .andExpect(status().isOk())
                    .andReturn().getResponse().getContentAsString();
            if (resp.contains(a) && resp.contains(b)) {
                return resp;
            }
            Thread.sleep(250);
        }
        throw new AssertionError("/lab/rejected never returned both records within 20s");
    }
}
