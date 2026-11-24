package com.example.lab.consumer;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Task 2.8 (discard mode): with lab.rollback.mode=discard a rolled-back txn-lane
 * record is SKIPPED with a log line naming its offset — nothing lands on ack-a/
 * ack-b and the group offset moves past it.
 */
@SpringBootTest(properties = {
        "lab.kafka.transaction-id-prefix=lab-txn-test-c-",
        "lab.rollback.mode=discard"
})
class LabTxnDiscardTest {

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
    FailureInjector failureInjector;

    @Autowired
    LabKafkaAdmin admin;

    @Test
    void discardModeSkipsRolledBackRecordWithOffsetLogLine() throws Exception {
        LogLines captor = new LogLines(TxnSideEffects.class);
        failureInjector.setFailTxnSideEffect(true);
        harness.produce("lab-orders", "cust-disc", LabPopTest.orderEvent("ord-disc"));

        // The discard log line names the offset.
        assertTrue(captor.awaitContains(30_000, line -> line.contains("discarding") && line.contains("offset")),
                "expected a discard log line naming the offset; got:\n" + captor.lines());
        captor.close();

        // Nothing landed on either side-effect topic.
        assertNull(harness.consumeOne("lab-ack-a", 2_000), "discarded record must not land ack-a");
        assertNull(harness.consumeOne("lab-ack-b", 2_000), "discarded record must not land ack-b");

        // The group offset moved past the discarded record.
        long deadline = System.currentTimeMillis() + 20_000;
        boolean caughtUp = false;
        while (System.currentTimeMillis() < deadline) {
            Map<org.apache.kafka.common.TopicPartition, Long> group = admin.groupOffsets("lab-txn");
            Map<org.apache.kafka.common.TopicPartition, Long> next = admin.nextOffsets(List.of("lab-orders"));
            caughtUp = !next.isEmpty() && next.equals(group);
            if (caughtUp) {
                break;
            }
            Thread.sleep(250);
        }
        assertEquals(true, caughtUp, "group offset must pass the discarded record");
    }
}
