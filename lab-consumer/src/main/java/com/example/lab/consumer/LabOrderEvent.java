package com.example.lab.consumer;

/**
 * Consumer-side copy of the {@code lab-orders} line/order event. Deliberately DUPLICATED from
 * lab-producer's OrderLineEvent instead of sharing a class: the two apps are coupled only
 * through the topic contract (specs/kafka-contract.md), which is how independent services
 * actually stay independent.
 */
public record LabOrderEvent(
        String schemaVersion,
        String orderId,
        String customerId,
        String sku,
        int qty,
        double price,
        String messageId,
        String traceId,
        String deliveredAt) {
}
