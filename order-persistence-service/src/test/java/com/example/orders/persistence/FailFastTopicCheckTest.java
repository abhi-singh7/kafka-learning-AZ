package com.example.orders.persistence;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Task 3.6 RED: fail-fast topic check. If order-persisted (or order-created) is
 * missing at startup, the service must refuse to start with a readable message
 * naming the missing topic and the create command.
 */
class FailFastTopicCheckTest {

    static KafkaTestHarness harness;

    @BeforeAll
    static void startBroker() {
        // deliberately create ONLY order-created — order-persisted is missing
        harness = new KafkaTestHarness("order-created");
    }

    @AfterAll
    static void stopBroker() {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void startupFailsFastWhenAckTopicMissing() {
        TopicStartupProbe probe = new TopicStartupProbe(harness.bootstrapServers());
        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> probe.verifyRequiredTopics(List.of("order-created", "order-persisted")));
        assertTrue(ex.getMessage().contains("order-persisted"),
                "failure must name the missing topic: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("kafka-topics"),
                "failure must include the create command: " + ex.getMessage());
    }
}
