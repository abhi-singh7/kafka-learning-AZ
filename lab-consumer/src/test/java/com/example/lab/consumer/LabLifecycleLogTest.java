package com.example.lab.consumer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Task 2.4: one line per message — topic, partition, offset, key, headers, payload,
 * and the decision (accept / skip-duplicate / reject-quarantine / retry).
 */
@SpringBootTest(properties = "lab.test.id=lifecycle")
class LabLifecycleLogTest {

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

    @Test
    void oneLogLinePerMessageWithFullContext() {
        try (LogLines captor = new LogLines(LabListeners.class)) {
            harness.produce("lab-orders", "cust-9", LabPopTest.orderEvent("ord-log-1"));
            boolean found = captor.awaitContains(20_000,
                    line -> line.contains("lifecycle") && line.contains("decision=accept")
                            && line.contains("topic=lab-orders") && line.contains("partition=")
                            && line.contains("offset=") && line.contains("key=cust-9")
                            && line.contains("ord-log-1"));
            assertTrue(found, "expected one lifecycle line with topic/partition/offset/key/payload/decision; got:\n"
                    + captor.lines());
        }
    }
}
