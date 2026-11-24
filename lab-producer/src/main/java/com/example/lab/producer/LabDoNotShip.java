package com.example.lab.producer;

import org.springframework.kafka.core.KafkaTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DO-NOT-SHIP. This class demonstrates the FORBIDDEN request-path pattern:
 * awaiting the produce result in the handler thread. It exists so the
 * experiment can contrast it, and MUST NEVER be called from any controller or
 * production lane (nothing references it; the SHIP shape returns the
 * CompletableFuture chain instead).
 *
 * Keep as the last shape a reviewer reads when comparing lanes.
 */
public final class LabDoNotShip {

    private static final Logger log = LoggerFactory.getLogger(LabDoNotShip.class);

    private LabDoNotShip() {
    }

    /**
     * DO-NOT-SHIP: blocks the calling request thread until the broker
     * acknowledges (send().syncAwait/join). Slow brokers stall the HTTP thread;
     * this is exactly the anti-pattern the spec bans. NEVER call from a route.
     */
    public static void blockingProduceDemo(KafkaTemplate<String, OrderLineEvent> kafkaTemplate,
            String topic, String key, OrderLineEvent event) {
        log.info("DO-NOT-SHIP: awaiting produce result in the request path (topic={})", topic);
        kafkaTemplate.send(topic, key, event).join();
    }
}
