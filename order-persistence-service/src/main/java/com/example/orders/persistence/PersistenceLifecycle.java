package com.example.orders.persistence;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Task 3.6 GREEN: fail-fast startup topic check + graceful shutdown.
 * - On ApplicationReady: verify order-created and order-persisted exist, else the
 *   app refuses to serve (readable message with the create command).
 * - On destroy: flush + destroy the KafkaTemplate, log persisted row count.
 */
@Component
class PersistenceLifecycle implements ApplicationListener<ApplicationReadyEvent>, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(PersistenceLifecycle.class);
    private static final List<String> REQUIRED_TOPICS = List.of("order-created", "order-persisted");

    private final String bootstrapServers;
    private final KafkaTemplate<?, ?> kafkaTemplate;
    private final OrderLineEventRepository repository;

    PersistenceLifecycle(@org.springframework.beans.factory.annotation.Value("${spring.kafka.bootstrap-servers}")
                         String bootstrapServers,
                         KafkaTemplate<String, PersistedAck> kafkaTemplate,
                         OrderLineEventRepository repository) {
        this.bootstrapServers = bootstrapServers;
        this.kafkaTemplate = kafkaTemplate;
        this.repository = repository;
    }

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        try {
            new TopicStartupProbe(bootstrapServers).verifyRequiredTopics(REQUIRED_TOPICS);
            log.info("order-persistence-service startup topic check passed: {}", REQUIRED_TOPICS);
        } catch (RuntimeException e) {
            // Fail fast: the context is up but the service refuses to serve.
            log.error("order-persistence-service refusing to start: {}", e.getMessage());
            throw e;
        }
    }

    @Override
    public void destroy() {
        try {
            kafkaTemplate.flush();
        } catch (Exception e) {
            log.warn("flush during shutdown failed: {}", e.getMessage());
        }
        kafkaTemplate.destroy();
        log.info("order-persistence-service shutdown complete; persisted rows={}", repository.count());
    }
}
