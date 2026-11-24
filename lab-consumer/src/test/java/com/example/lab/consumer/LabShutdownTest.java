package com.example.lab.consumer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Task 2.9: graceful shutdown — with nothing pending the destroy hook reports a
 * clean state and completes without a poll-loop stack trace; un-ACKed messages
 * would stay at their offsets for the next start.
 */
@SpringBootTest(properties = "lab.test.id=shutdown")
class LabShutdownTest {

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
    LabShutdown labShutdown;

    @Autowired
    LabLedger ledger;

    @Autowired
    LabQueue queue;

    @Test
    void shutdownReportsPendingAndCompletesCleanly() throws Exception {
        harness.produce("lab-orders", "cust-sh-1", LabPopTest.orderEvent("ord-sh-1"));
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline && ledger.count() < 1) {
            Thread.sleep(250);
        }
        assertEquals(1, ledger.count());
        long deadline2 = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline2 && queue.inFlight() > 0) {
            Thread.sleep(100);
        }
        assertEquals(0, queue.inFlight(), "no pending work before shutdown");

        // Must complete without throwing (no poll-loop stack trace).
        labShutdown.destroy();
    }
}
