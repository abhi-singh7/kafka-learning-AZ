package com.example.lab.producer;

/**
 * Request of the transactional push lane (Task 1.5). {@code forceRollback}
 * makes the controller throw AFTER both records are buffered inside the Kafka
 * transaction, so the transaction must roll back and NEITHER topic advances
 * (all-or-nothing across lab-ack-a + lab-ack-b).
 */
public record LabTxnPushRequestDto(String orderId, String left, String right,
        Boolean forceRollback) {
}
