package com.example.lab.consumer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Task 2.8 (atomicity): the txn lane pushes ack-a and ack-b inside ONE Kafka
 * transaction. A forced side-effect failure rolls the whole transaction back:
 * NEITHER topic receives a record. With no failure, both arrive.
 */
@SpringBootTest(properties = {
        "lab.kafka.transaction-id-prefix=lab-txn-test-c-"
})
class LabTxnAtomicityTest {

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

    @Test
    void forcedFailureRollsBackBothSideEffects() throws Exception {
        // Force the side effect to fail inside the transaction.
        failureInjector.setFailTxnSideEffect(true);
        harness.produce("lab-orders", "cust-txn-1", LabPopTest.orderEvent("ord-txn-1"));

        // Give the listener time to attempt + roll back, then verify NOTHING landed.
        Thread.sleep(5_000);
        assertNull(harness.consumeOne("lab-ack-a", 2_000), "rolled-back transaction must not land ack-a");
        assertNull(harness.consumeOne("lab-ack-b", 2_000), "rolled-back transaction must not land ack-b");

        // No failure: both side effects arrive atomically (payload carries the orderId).
        failureInjector.setFailTxnSideEffect(false);
        harness.produce("lab-orders", "cust-txn-2", LabPopTest.orderEvent("ord-txn-2"));
        String a = harness.consumeOne("lab-ack-a", 90_000);
        String b = harness.consumeOne("lab-ack-b", 90_000);
        org.junit.jupiter.api.Assertions.assertNotNull(a, "ack-a must receive the order");
        org.junit.jupiter.api.Assertions.assertNotNull(b, "ack-b must receive the order");
        assertTrue(a.contains("ord-txn-2"), "ack-a payload must carry the orderId; got: " + a);
        assertTrue(b.contains("ord-txn-2"), "ack-b payload must carry the orderId; got: " + b);
    }
}
