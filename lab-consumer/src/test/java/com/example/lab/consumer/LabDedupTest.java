package com.example.lab.consumer;

import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Task 2.11: redelivery of an already-applied order counts once. The ledger is
 * keyed by the business key (orderId), so a replay at a different offset is
 * still one event.
 */
@SpringBootTest(properties = "lab.test.id=dedup")
class LabDedupTest {

    static LabKafkaTestHarness harness;

    @BeforeAll
    static void start() {
        harness = new LabKafkaTestHarness("lab-orders", "lab-rebalance", "lab-rejected", "lab-ack-a", "lab-ack-b");
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

    @Autowired
    LabLedger ledger;

    @Test
    void redeliveredOrderCountsOnce() throws Exception {
        String event = LabPopTest.orderEvent("ord-dup");
        harness.produce("lab-orders", "cust-dup", event);
        // Redelivery of the SAME business event (a new offset).
        harness.produce("lab-orders", "cust-dup", event);

        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline && ledger.count() < 1) {
            Thread.sleep(250);
        }
        // Give a redelivery attempt time to be seen and skipped.
        Thread.sleep(2_000);

        assertEquals(List.of("ord-dup"), ledger.keys(), "the order must be applied exactly once");
        assertEquals(1, ledger.stamps().size(), "one offset stamp for one business event");
    }
}
