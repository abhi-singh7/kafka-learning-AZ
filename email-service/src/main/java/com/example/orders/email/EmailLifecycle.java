package com.example.orders.email;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Task 2.5 GREEN: fail-fast startup topic check + graceful shutdown.
 * - On ApplicationReady: verify order-created and order-email-sent exist, else
 *   the app refuses to serve (readable message with the create command).
 * - On destroy: flush + destroy the KafkaTemplate, log pending/buffered count.
 */
@Component
class EmailLifecycle implements ApplicationListener<ApplicationReadyEvent>, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(EmailLifecycle.class);
    private static final List<String> REQUIRED_TOPICS = List.of("order-created", "order-email-sent");

    private final String bootstrapServers;
    private final KafkaTemplate<?, ?> kafkaTemplate;
    private final MockMailSink sink;

    EmailLifecycle(@org.springframework.beans.factory.annotation.Value("${spring.kafka.bootstrap-servers}")
                   String bootstrapServers,
                   KafkaTemplate<String, EmailSentAck> kafkaTemplate,
                   MockMailSink sink) {
        this.bootstrapServers = bootstrapServers;
        this.kafkaTemplate = kafkaTemplate;
        this.sink = sink;
    }

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        try {
            new TopicStartupProbe(bootstrapServers).verifyRequiredTopics(REQUIRED_TOPICS);
            log.info("email-service startup topic check passed: {}", REQUIRED_TOPICS);
        } catch (RuntimeException e) {
            // Fail fast: the context is up but the service refuses to serve.
            log.error("email-service refusing to start: {}", e.getMessage());
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
        log.info("email-service shutdown complete; buffered mails pending={}, evicted={}",
                sink.allMails().size(), sink.evictedCount());
    }
}
