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
 * Task 1.7 GREEN: /lab/diagnostics is a read-only broker query; repeated calls
 * change nothing; sibling order-* names are listed for namespace contrast.
 * NOTE: Boot 4.1.2 KafkaAdmin exposes no partition-offset query, so the reported
 * nextOffset is what the app observed from its own push results - labelled degraded
 * in docs/experiments.md.
 */
@SpringBootTest
@AutoConfigureMockMvc
class LabDiagnosticsTest {

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
    void diagnosticsIsReadOnlyAndReportsObservedOffsets() throws Exception {
        String first = mockMvc.perform(get("/lab/diagnostics"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        String second = mockMvc.perform(get("/lab/diagnostics"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        assertEquals(first, second, "repeated diagnostics change nothing - the query is read-only");
        assertTrue(first.contains("lab-orders"), "the lab lane is listed");
        assertTrue(first.contains("order-created"),
                "sibling order-* names listed for namespace contrast");
        assertTrue(first.contains("\"exists\":true"), "described lab topics report their real partition count");
        assertTrue(!first.matches("(?s).*\"nextOffset\":[0-9].*"),
                "before any push, no offset is claimed anywhere");

        String body = "{\"customerId\":\"diag-cust\",\"items\":[{\"sku\":\"d\",\"qty\":1,\"price\":1.0}]}";
        var started = mockMvc.perform(post("/lab/orders")
                .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(request().asyncStarted())
                .andReturn();
        mockMvc.perform(asyncDispatch(started)).andExpect(status().isAccepted());

        String after = mockMvc.perform(get("/lab/diagnostics"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        assertTrue(after.matches("(?s).*\"nextOffset\":[0-9].*"),
                "the pushed coordinates become visible (degraded: observed by the app, see docs/experiments.md)");
        String afterTwice = mockMvc.perform(get("/lab/diagnostics"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        assertEquals(after, afterTwice, "diagnostics still advanced nothing");
    }
}
