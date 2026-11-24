package com.example.lab.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Graceful shutdown hook for the push side (mirrors the sibling
 * OrderProducerShutdown): flushes the async lane so buffered pushes leave the
 * process toward the broker, then destroys every KafkaTemplate exactly once.
 * No Kafka client is created during or after shutdown.
 */
@Component
class LabShutdown implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(LabShutdown.class);

    private final KafkaTemplate<String, OrderLineEvent> orderKafkaTemplate;
    private final KafkaTemplate<String, String> rawKafkaTemplate;
    private final SyncPushLane orderSyncPush;

    LabShutdown(KafkaTemplate<String, OrderLineEvent> orderKafkaTemplate,
                KafkaTemplate<String, String> rawKafkaTemplate,
                SyncPushLane orderSyncPush) {
        this.orderKafkaTemplate = orderKafkaTemplate;
        this.rawKafkaTemplate = rawKafkaTemplate;
        this.orderSyncPush = orderSyncPush;
    }

    @Override
    public void destroy() {
        try {
            orderKafkaTemplate.flush();
            log.info("lab-orders producer lane flushed during shutdown");
        } catch (Exception e) {
            log.warn("flush of async lane during shutdown failed: {}", e.getMessage());
        }
        try {
            rawKafkaTemplate.flush();
            log.info("raw producer lane flushed during shutdown");
        } catch (Exception e) {
            log.warn("flush of raw lane during shutdown failed: {}", e.getMessage());
        }
        orderSyncPush.destroy();
        rawKafkaTemplate.destroy();
        orderKafkaTemplate.destroy();
        log.info("lab-producer shutdown complete; all KafkaTemplates destroyed exactly once");
    }
}
