package com.example.orders.email;

import java.time.Instant;

/**
 * email-service's OWN copy of the line-event record (deliberate duplication —
 * see design "Deliberate duplication"). Decoded from JSON on order-created.
 * Exactly the nine schemaVersion-1 contract fields.
 */
public record OrderLineEventRecord(
        String schemaVersion,
        String orderId,
        String customerId,
        String sku,
        Integer qty,
        Double price,
        Integer lineIndex,
        String messageId,
        Instant createdAt
) {
}
