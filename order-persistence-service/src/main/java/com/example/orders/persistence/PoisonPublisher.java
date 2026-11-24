package com.example.orders.persistence;

/** Publishes a poison record to order-failed for undecodable/foreign payloads. */
interface PoisonPublisher {
    void publishPoison(String sourceTopic, String rawPayload, String reason);
}
