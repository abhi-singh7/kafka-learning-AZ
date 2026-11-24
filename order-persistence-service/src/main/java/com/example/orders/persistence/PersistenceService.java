package com.example.orders.persistence;

import java.math.BigDecimal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Task 3.3 GREEN: persist exactly once per messageId (at-least-once → idempotent).
 * insert-or-skip on the uniqueness guards (messageId UNIQUE, (orderId,sku) PK);
 * one transaction per Kafka message. A duplicate-key collision is an idempotent
 * skip (NOT a failure); any other JDBC error propagates so the listener NACKs.
 */
@Service
public class PersistenceService {

    private static final Logger log = LoggerFactory.getLogger(PersistenceService.class);

    public enum PersistResult { PERSISTED, ALREADY_PERSISTED }

    private final OrderLineEventRepository repository;

    public PersistenceService(OrderLineEventRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public PersistResult persist(OrderLineEventRecord event) {
        if (repository.existsByMessageId(event.messageId())) {
            log.info("messageId {} already persisted — idempotent skip", event.messageId());
            return PersistResult.ALREADY_PERSISTED;
        }
        OrderLineEvent entity = new OrderLineEvent(
                event.orderId(),
                event.customerId(),
                event.sku(),
                event.qty(),
                BigDecimal.valueOf(event.price()),
                event.lineIndex(),
                event.messageId(),
                event.createdAt());
        try {
            repository.saveAndFlush(entity);
        } catch (DataIntegrityViolationException e) {
            // Concurrent redelivery inserted the same row first → idempotent skip.
            log.info("duplicate key for {} — idempotent skip", event.messageId());
            return PersistResult.ALREADY_PERSISTED;
        }
        return PersistResult.PERSISTED;
    }
}
