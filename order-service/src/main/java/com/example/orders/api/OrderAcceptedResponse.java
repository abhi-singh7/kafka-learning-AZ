package com.example.orders.api;

/**
 * 201 response body: {@code {"orderId","lineCount","kafkaKey","topic"}}.
 * {@code kafkaKey} equals the orderId (partitioning key per the Kafka contract).
 */
public record OrderAcceptedResponse(String orderId, int lineCount, String kafkaKey, String topic) {

    public static OrderAcceptedResponse of(String orderId, int lineCount, String topic) {
        return new OrderAcceptedResponse(orderId, lineCount, orderId, topic);
    }
}
