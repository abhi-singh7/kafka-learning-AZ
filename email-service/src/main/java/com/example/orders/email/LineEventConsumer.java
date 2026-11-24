package com.example.orders.email;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Task 2.3 GREEN: consume order-created, render a mock mail, publish the ack
 * to order-email-sent (key = orderId), then ACK. On rendering failure → NACK
 * (rethrow) so Kafka redelivers. On undecodable/foreign payload → poison path
 * (Task 2.4) — ACK after publishing to order-failed.
 */
@Component
class LineEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(LineEventConsumer.class);
    public static final String ORDER_CREATED = "order-created";

    private final MockMailSink sink;
    private final AckPublisher ackPublisher;
    private final PoisonPublisher poisonPublisher;

    LineEventConsumer(MockMailSink sink, AckPublisher ackPublisher, PoisonPublisher poisonPublisher) {
        this.sink = sink;
        this.ackPublisher = ackPublisher;
        this.poisonPublisher = poisonPublisher;
    }

    @KafkaListener(topics = ORDER_CREATED, groupId = "email-service")
    void onLineEvent(String payload, Acknowledgment acknowledgment) {
        LineEventDecodeResult decode = LineEventDecoder.decode(payload);

        if (!decode.isAccepted()) {
            // Poison path (A1): publish to order-failed, then ACK so it is not redelivered.
            poisonPublisher.publishPoison(ORDER_CREATED, payload, decode.rejectionReason());
            acknowledgment.acknowledge();
            return;
        }

        OrderLineEventRecord event = decode.record();
        try {
            sink.record(event);
            ackPublisher.publishAck(event);
        } catch (RuntimeException e) {
            // NACK: rethrow so the listener container redelivers the same record.
            log.warn("mail handling failed for {} — will redeliver", event.messageId());
            throw e;
        }
        acknowledgment.acknowledge();
    }
}
