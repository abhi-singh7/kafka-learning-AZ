package com.example.lab.producer;

/**
 * 202 body of the transactional push lane: which topics received each half.
 */
public record LabTxnAckDto(String orderId, String topicA, String topicB) {
}
