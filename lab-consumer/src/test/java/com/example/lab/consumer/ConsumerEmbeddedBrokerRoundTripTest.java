package com.example.lab.consumer;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.Test;

/**
 * Task 0.2 GREEN (consumer side): same seam, this project's own topics. The harness is a
 * deliberate copy of the producer project's — the two apps are coupled only through topics.
 */
class ConsumerEmbeddedBrokerRoundTripTest {

    @Test
    void roundTripThroughInProcessBroker() {
        try (LabKafkaTestHarness harness = new LabKafkaTestHarness("lab-pending", "lab-rejected")) {
            assertFalse(harness.bootstrapServers().isBlank(), "the seam must report a broker to talk to");
            harness.produce("lab-pending", "cust-9", "pending-order");
            assertEquals("pending-order", harness.consumeOne("lab-pending", 10_000),
                    "a produced message must be readable back from the topic");
        }
    }

    @Test
    void perKeyPushOrderSurvivesTheSeam() {
        try (LabKafkaTestHarness harness = new LabKafkaTestHarness("lab-pending")) {
            harness.produce("lab-pending", "cust-9", "first");
            harness.produce("lab-pending", "cust-9", "second");
            List<String> records = harness.consumeUpTo("lab-pending", 2, 10_000);
            assertEquals(2, records.size(), "both pushes must arrive");
            assertEquals(java.util.List.of("first", "second"), records, "per-partition order is push order");
        }
    }
}
