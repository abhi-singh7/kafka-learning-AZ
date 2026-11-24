package com.example.lab.producer;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.Test;

/**
 * Task 0.2 RED→GREEN: the hermetic test seam — an in-process KRaft broker, never the docker
 * broker {@code order-kafka2} (that container is only for the smoke run in Task 8.0).
 *
 * RED failed with "cannot find symbol class LabKafkaTestHarness": the seam was absent, which is
 * the right reason. GREEN = the seam exists and a keyed message round-trips through a topic.
 */
class EmbeddedBrokerRoundTripTest {

    @Test
    void roundTripThroughInProcessBroker() {
        try (LabKafkaTestHarness harness = new LabKafkaTestHarness("lab-orders")) {
            assertFalse(harness.bootstrapServers().isBlank(), "the seam must report a broker to talk to");
            harness.produce("lab-orders", "cust-0", "hello-lab");
            assertEquals("hello-lab", harness.consumeOne("lab-orders", 10_000),
                    "a produced message must be readable back from the topic");
        }
    }

    @Test
    void seamCreatesOnlyTheTopicsATestAskedFor() {
        try (LabKafkaTestHarness harness = new LabKafkaTestHarness("lab-orders", "lab-rejected")) {
            harness.produce("lab-orders", "cust-0", "one");
            harness.produce("lab-orders", "cust-0", "two");
            List<String> orders = harness.consumeUpTo("lab-orders", 2, 10_000);
            assertEquals(2, orders.size(), "both pushes must arrive");
            assertEquals(java.util.List.of("one", "two"), orders, "per-partition order is push order");
            harness.produce("lab-rejected", "corrupt", "raw-poison");
            assertEquals("raw-poison", harness.consumeOne("lab-rejected", 10_000),
                    "the quarantine lane is just another topic");
        }
    }
}
