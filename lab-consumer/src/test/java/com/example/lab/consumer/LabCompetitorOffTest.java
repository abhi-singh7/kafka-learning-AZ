package com.example.lab.consumer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Task 2.10: the competitor race. Two listeners share ONE group on lab-rebalance,
 * so total processings == message count. Stopping one listener leaves the
 * survivor to process the rest (on a multi-broker cluster it would also re-fetch
 * its assignment from the controller after the rebalance — concept-only here).
 */
@SpringBootTest(properties = "lab.test.id=competitor")
@AutoConfigureMockMvc
class LabCompetitorOffTest {

    static LabKafkaTestHarness harness;

    @BeforeAll
    static void start() {
        harness = new LabKafkaTestHarness("lab-orders", "lab-rebalance", "lab-rejected", "lab-ack-a", "lab-ack-b");
        LabListeners.RaceCounter.reset();
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

    @org.springframework.beans.factory.annotation.Autowired
    org.springframework.kafka.config.KafkaListenerEndpointRegistry listenerRegistry;

    @org.springframework.beans.factory.annotation.Autowired
    MockMvc mockMvc;

    @Test
    void oneGroupTwoListenersThenCompetitorOff() throws Exception {
        // Both listeners alive: total processings == message count.
        for (int i = 1; i <= 6; i++) {
            harness.produce("lab-rebalance", "k" + i, "race-" + i);
        }
        awaitRaceTotal(6);

        // Stop the competitor listener.
        var containers = listenerRegistry.getListenerContainersMatching(id -> id.equals("lab-race-b"));
        assertEquals(1, containers.size(), "the competitor listener container must exist");
        for (var c : containers) {
            c.stop();
        }

        // The survivor processes the rest: total still == message count.
        for (int i = 7; i <= 10; i++) {
            harness.produce("lab-rebalance", "k" + i, "race-" + i);
        }
        awaitRaceTotal(10);
    }

    private void awaitRaceTotal(int expected) {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline && LabListeners.RaceCounter.total() < expected) {
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }
        assertEquals(expected, LabListeners.RaceCounter.total(),
                "one group ⇒ total processings must equal the message count");
    }
}
