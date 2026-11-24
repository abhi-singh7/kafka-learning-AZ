package com.example.lab.consumer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Task 2.3 RED: default {@code lab.ack.strategy=after-processed} maps to MANUAL
 * ack (the listener commits after processing each record).
 */
@SpringBootTest(properties = "lab.test.id=ack-after")
class LabAckAfterProcessedTest {

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
    void afterProcessedIsManualAck() {
        var main = registry.getListenerContainersMatching(id -> id.equals("lab-consumer")).iterator().next();
        ContainerProperties props = ((ConcurrentMessageListenerContainer) main).getContainerProperties();
        assertEquals(ContainerProperties.AckMode.MANUAL, props.getAckMode(),
                "after-processed must commit manually after each record is processed");
    }
}
