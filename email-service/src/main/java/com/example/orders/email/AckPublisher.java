package com.example.orders.email;

/** Publishes the order-email-sent ack record (key = orderId). */
interface AckPublisher {
    void publishAck(OrderLineEventRecord event);
}
