package com.example.lab.consumer;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Task 2.5: the x-lab-skip header is honored BEFORE deserialization (a garbage
 * payload with the header must NOT produce a json-decode quarantine entry), and
 * the skipped record IS acked — the group offset passes it.
 */
@SpringBootTest(properties = "lab.test.id=filter")
class LabFilterTest {

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

    @Autowired
    LabKafkaAdmin admin;

    @Test
    void skipHeaderFiltersBeforeDecodeAndIsAcked() throws Exception {
        // Garbage payload + skip header: filtered pre-deserialization, so NO json-decode quarantine.
        harness.produceWithHeaders("lab-orders", "cust-skip", "not-json-at-all", Map.of("x-lab-skip", "true"));
        // A normal record that must be accepted.
        harness.produce("lab-orders", "cust-keep", LabPopTest.orderEvent("ord-keep"));

        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline && ledger.count() < 1) {
            Thread.sleep(250);
        }
        assertEquals(List.of("ord-keep"), ledger.keys(), "only the non-skipped order is applied");

        // The skipped record was ACKed: group offset == end offset (no lag, no quarantine).
        long ackDeadline = System.currentTimeMillis() + 20_000;
        boolean caughtUp = false;
        while (System.currentTimeMillis() < ackDeadline) {
            Map<org.apache.kafka.common.TopicPartition, Long> group = admin.groupOffsets("lab-consumer");
            Map<org.apache.kafka.common.TopicPartition, Long> next = admin.nextOffsets(List.of("lab-orders"));
            caughtUp = !next.isEmpty() && next.equals(group);
            if (caughtUp) {
                break;
            }
            Thread.sleep(250);
        }
        assertEquals(true, caughtUp, "group offset must pass the skipped record (it was acked on purpose)");
    }
}
