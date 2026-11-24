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
 * Task 1.4 GREEN: bounded queue + short max.blocking (properties only) make
 * /lab/throttle answer 429 + Retry-After and count the rejected pushes.
 */
@SpringBootTest
@AutoConfigureMockMvc
class LabThrottleTest {

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
        registry.add("lab.producer.throttle.buffer.bytes", () -> "32768");
        registry.add("lab.producer.throttle.max.request.size", () -> "1024");
        registry.add("lab.producer.throttle.linger.ms", () -> "60000");
        registry.add("lab.producer.throttle.max.block.ms", () -> "200");
        registry.add("lab.producer.throttle.probe.timeout", () -> "400");
    }

    @Autowired
    MockMvc mockMvc;

    @Test
    void throttleLaneAnswers429WhenTheQueueFills() throws Exception {
        var response = mockMvc.perform(post("/lab/throttle")
                .contentType(MediaType.APPLICATION_JSON).content("{\"count\":40,\"recordBytes\":900}"))
                .andExpect(status().is(429))
                .andReturn().getResponse();

        assertEquals("1", response.getHeader("Retry-After"), "Retry-After names the retry window");
        var mapper = tools.jackson.databind.json.JsonMapper.builder().build();
        LabThrottleAckDto ack = mapper.readValue(response.getContentAsString(), LabThrottleAckDto.class);
        assertTrue(ack.blockedSendsTotal() > 0, "rejected pushes are counted, not lost silently");
        assertTrue(ack.pushedThisCall() < 40, "the loop stops as soon as send() refuses");
    }

    @Test
    void oversizedBurstKeepsTheAppServing() throws Exception {
        // Task 1.4 gate: JVM must not OOM. A second oversized call proves the process
        // is still alive and still answering 429 after the queue filled up.
        var response = mockMvc.perform(post("/lab/throttle")
                .contentType(MediaType.APPLICATION_JSON).content("{\"count\":40,\"recordBytes\":900}"))
                .andExpect(status().is(429))
                .andReturn().getResponse();
        assertTrue(response.getContentAsString().contains("blockedSendsTotal"),
                "the throttle counter is still reported - the app throttled instead of growing memory");
    }

}
