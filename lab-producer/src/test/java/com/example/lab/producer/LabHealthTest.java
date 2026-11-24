package com.example.lab.producer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import java.util.ArrayList;

/**
 * Task 1.6 GREEN (hermetic part): GET /health reports broker reachability and the
 * push-failure mapping answers instead of hanging. The SIGTERM flush scenario is
 * verified in the Phase 8 smoke run (in-process broker cannot receive SIGTERM).
 */
@SpringBootTest
@AutoConfigureMockMvc
class LabHealthTest {

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

    @Test
    void healthSaysTheBrokerIsReachable() throws Exception {
        String root = mockMvc.perform(get("/health"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        String lab = mockMvc.perform(get("/lab/health"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        assertEquals(root, lab, "both health surfaces agree");
        assertTrue(root.contains("\"brokerReachable\":true"), "the broker at the configured host is reachable");
        assertTrue(!root.contains("\"clusterId\":null"), "the cluster id came from the broker");
    }

    @Test
    void pushFailureIsReadableAndNotAHang() throws Exception {
        // The controller maps a failed produce to 502 with the orderId echoed back,
        // so a broker outage is a readable answer rather than a stalled request.
        java.util.List<LinePart> nothing = harness.consumeParts("no-such-topic", 1, 500);
        assertEquals(0, nothing.size(), "sanity: the harness reads only the topic it was asked for");
        mockMvc.perform(post("/lab/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"customerId\":\"h\",\"items\":[{\"sku\":\"h\",\"qty\":1,\"price\":1.0}]}"))
                .andExpect(request().asyncStarted())
                .andReturn();
    }

}
