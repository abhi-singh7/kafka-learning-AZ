package com.example.orders.email;

import java.time.Instant;

/** Ack record published to order-email-sent after a line event is handled. */
public record EmailSentAck(
        String schemaVersion,
        String orderId,
        String messageId,
        String sku,
        int qty,
        String result,
        Instant at,
        String service
) {
    public static final String SERVICE = "email-service";

    public static EmailSentAck of(OrderLineEventRecord event) {
        return new EmailSentAck("1", event.orderId(), event.messageId(), event.sku(),
                event.qty(), "email-sent", Instant.now(), SERVICE);
    }
}
