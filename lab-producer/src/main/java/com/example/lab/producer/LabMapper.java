package com.example.lab.producer;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Pure mapper from one accepted order to N line-event messages (one per item).
 * No I/O — the caller decides where the events go.
 *
 * {@code messageId} and {@code traceId} are set here, by the producer, per the
 * contract in specs/kafka-contract.md; consumers only echo them back.
 */
public final class LabMapper {

    private LabMapper() {
    }

    public static List<OrderLineEvent> toLineEvents(OrderRequestDto request, String orderId, String traceId) {
        List<OrderLineEvent> events = new ArrayList<>();
        if (request == null || request.items() == null) {
            return events;
        }
        Instant createdAt = java.time.Instant.now();
        for (LineDto item : request.items()) {
            events.add(new OrderLineEvent(
                    OrderLineEvent.SCHEMA_VERSION,
                    orderId,
                    request.customerId(),
                    item.sku(),
                    item.qty(),
                    item.price(),
                    UUID.randomUUID().toString(),
                    traceId,
                    createdAt));
        }
        return events;
    }
}
