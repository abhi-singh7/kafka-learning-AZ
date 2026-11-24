package com.example.orders.persistence;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Seam the Kafka consumer depends on, so a failing DB implementation can be
 * substituted in tests to prove the NACK contract (rethrow, no ack, no row).
 */
public interface LineEventPersistence {

    PersistResult persist(OrderLineEventRecord event);

    public PersistResult(String outcome, long rows) {
    }
}
