package com.example.orders.api;

import java.time.Instant;

/**
 * One message on {@code order-created}. Exactly the nine fields of the
 * schemaVersion-1 contract (specs/kafka-contract.md). Jackson 3 encodes this.
 */
public record OrderLineEvent(
        String schemaVersion,
        String orderId,
        String customerId,
        String sku,
        int qty,
        double price,
        int lineIndex,
        String messageId,
        Instant createdAt
) {
    public static final String SCHEMA_VERSION = "1";
}
