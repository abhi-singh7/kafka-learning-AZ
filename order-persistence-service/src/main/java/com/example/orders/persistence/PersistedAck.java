package com.example.orders.persistence;

import java.time.Instant;

/**
 * Ack record published to order-persisted after a successful insert.
 * KEY = orderId (partitioning). result = "persisted", service = "order-persistence-service".
 */
public record PersistedAck(
        String schemaVersion,
        String orderId,
        String messageId,
        String sku,
        Integer qty,
        String result,
        Instant at,
        String service
) {
    public static PersistedAck of(OrderLineEventRecord event) {
        return new PersistedAck(
                "1",
                event.orderId(),
                event.messageId(),
                event.sku(),
                event.qty(),
                "persisted",
                Instant.now(),
                "order-persistence-service");
    }
}
