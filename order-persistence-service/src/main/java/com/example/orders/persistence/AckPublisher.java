package com.example.orders.persistence;

/** Publishes the order-persisted ack record (key = orderId). */
interface AckPublisher {
    void publishAck(OrderLineEventRecord event);
}
