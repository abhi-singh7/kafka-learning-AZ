package com.example.lab.producer;

import java.time.Instant;

/**
 * One message on {@code lab-orders}: exactly the nine fields of the schemaVersion-1
 * contract in specs/kafka-contract.md (Jackson 3 encodes this record).
 */
public record OrderLineEvent(
        String schemaVersion,
        String orderId,
        String customerId,
        String sku,
        Integer qty,
        Double price,
        String messageId,
        String traceId,
        Instant deliveredAt
) {
    public static final String SCHEMA_VERSION = "1";
}
