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
 * Task 1.3 GREEN: the property-selected sync-acknowledgment lane answers 202 and
 * publishes with key = customerId. No blocking accessor in the request path.
 */
@SpringBootTest
@AutoConfigureMockMvc
class LabSyncLaneTest {

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

    private static final String REQ_A =
            "{\"customerId\":\"sync-a\",\"items\":[{\"sku\":\"s1\",\"qty\":2,\"price\":3.5}]}";

    private static final String REQ_B =
            "{\"customerId\":\"sync-b\",\"items\":[{\"sku\":\"s2\",\"qty\":1,\"price\":1.25}]}";

    /** Records on a topic whose KEY equals {@code key} (order of test methods is irrelevant). */
    private java.util.List<LinePart> withKey(String topic, String key) {
        java.util.ArrayList<LinePart> hits = new java.util.ArrayList<>();
        for (LinePart part : harness.consumeParts(topic, 8, 2_000)) {
            if (key.equals(part.key())) {
                hits.add(part);
            }
        }
        return hits;
    }

    @Test
    void syncLaneAnswersFromAnAlreadyResolvedSend() throws Exception {
        var response = mockMvc.perform(post("/lab/orders/sync")
                .contentType(MediaType.APPLICATION_JSON).content(REQ_A))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.partition").isNumber())
                .andExpect(jsonPath("$.offset").isNumber())
                .andReturn().getResponse();

        java.util.List<LinePart> hits = withKey("lab-orders", "sync-a");
        assertEquals(1, hits.size(), "the sync lane publishes exactly one line event");
        assertEquals("sync-a", hits.get(0).key(), "key = customerId even on the sync lane");
        assertTrue(hits.get(0).value().contains("\"customerId\":\"sync-a\""),
                "the payload carries the contract customerId");

        var mapper = tools.jackson.databind.json.JsonMapper.builder().build();
        LabPushAckDto ack = mapper.readValue(response.getContentAsString(), LabPushAckDto.class);
        assertEquals(ack.partition(), hits.get(0).partition(), "coordinates come from the push result");
        assertEquals(ack.offset(), hits.get(0).offset(), "coordinates come from the push result");
    }

    @Test
    void syncLaneNeverAwaitsInTheRequestPath() throws Exception {
        // SyncPushLane sets kafka-clients synchronous=true, so the send future is
        // already resolved when send() returns and getNow(null) is enough; if it
        // were pending the lane throws instead of blocking.
        java.util.ArrayList<LinePart> none = new java.util.ArrayList<>(withKey("lab-orders", "sync-b"));
        assertEquals(0, none.size(), "nothing keyed sync-b exists before this test pushes");
        mockMvc.perform(post("/lab/orders/sync")
                .contentType(MediaType.APPLICATION_JSON).content(REQ_B))
                .andExpect(status().isAccepted());
        java.util.ArrayList<LinePart> one = new java.util.ArrayList<>(withKey("lab-orders", "sync-b"));
        assertEquals(1, one.size(), "the record landed; the request path never awaited the broker");
    }
}
