package com.example.orders.email;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Task 0.2 RED: proves the embedded broker harness works — produce a message,
 * read it back from the topic. Fails while {@link KafkaTestHarness} does not exist.
 */
class EmbeddedBrokerRoundTripTest {

    @Test
    void roundTripThroughEmbeddedBroker() {
        try (KafkaTestHarness h = new KafkaTestHarness("round-trip-topic")) {
            h.produce("round-trip-topic", "k1", "hello-kafka");
            String body = h.consumeOne("round-trip-topic", 10_000);
            assertEquals("hello-kafka", body);
        }
    }
}
