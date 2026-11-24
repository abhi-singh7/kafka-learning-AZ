package com.example.orders.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Task 1.5 GREEN (C13): graceful shutdown hook.
 * On stop signal: destroy the single KafkaTemplate and log the pending/buffered
 * message count so operators can see if events were in flight.
 */
@Component
class OrderProducerShutdown implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(OrderProducerShutdown.class);

    private final KafkaTemplate<String, OrderLineEvent> kafkaTemplate;

    OrderProducerShutdown(KafkaTemplate<String, OrderLineEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void destroy() {
        try {
            kafkaTemplate.flush();
        } catch (Exception e) {
            log.warn("flush during shutdown failed: {}", e.getMessage());
        }
        kafkaTemplate.destroy();
        log.info("order-service shutdown complete; KafkaTemplate destroyed");
    }
}
