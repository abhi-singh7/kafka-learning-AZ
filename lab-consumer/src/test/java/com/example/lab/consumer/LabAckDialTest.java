package com.example.lab.consumer;

import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListenerContainer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Task 2.3 RED: the acknowledgment dial. {@code lab.ack.strategy} selects
 * immediate / after-processed (default) / batch; the active mode is logged at
 * startup and the container's ack mode maps to it:
 *   immediate        -> MANUAL, commit before processing
 *   after-processed  -> MANUAL, commit after processing
 *   batch            -> COUNT with ackCount = lab.ack.batch-size (checkpoint)
 */
@SpringBootTest(properties = "lab.ack.strategy=batch")
class LabAckDialTest {

    static LabKafkaTestHarness harness;

    @BeforeAll
    static void startBroker() {
        harness = new LabKafkaTestHarness(1, "lab-orders", "lab-rebalance", "lab-rejected", "lab-ack-a", "lab-ack-b");
    }

    @AfterAll
    static void stopBroker() {
        if (harness != null) {
            harness.close();
        }
    }

    @org.springframework.test.context.DynamicPropertySource
    static void kafkaProps(org.springframework.test.context.DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", () -> harness.bootstrapServers());
    }

    @Autowired
    KafkaListenerEndpointRegistry registry;

    @Test
    void batchModeMapsToCountAckWithCheckpointSize() {
        MessageListenerContainer main = registry.getListenerContainersMatching(id -> id.equals("lab-consumer")).iterator().next();
        ContainerProperties props = ((org.springframework.kafka.listener.ConcurrentMessageListenerContainer) main).getContainerProperties();
        assertEquals(ContainerProperties.AckMode.COUNT, props.getAckMode(),
                "batch strategy must commit at checkpoint boundaries (COUNT)");
        assertEquals(5, props.getAckCount(), "checkpoint size must come from lab.ack.batch-size");
    }
}
