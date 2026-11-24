package com.example.lab.producer;

/**
 * Response of a successful produce (202 body). Partition/offset come from the
 * push result — never a local counter (kafka-contract A5). They are null on the
 * fire-and-forget error path only.
 */
public record LabPushAckDto(String orderId, String topic, Integer partition, Long offset,
        String messageId, String traceId) {
}
