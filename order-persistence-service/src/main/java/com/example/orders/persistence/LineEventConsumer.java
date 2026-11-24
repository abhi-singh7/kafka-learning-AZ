package com.example.orders.persistence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Task 3.3 GREEN: consume order-created, persist exactly once per messageId,
 * publish the ack to order-persisted (key = orderId), then ACK. On undecodable/
 * foreign payload → poison path (order-failed) + ACK. On JDBC infrastructure
 * failure → NACK (rethrow) so Kafka redelivers; no partial row is written.
 */
@Component
class LineEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(LineEventConsumer.class);
    public static final String ORDER_CREATED = "order-created";

    private final PersistenceService persistenceService;
    private final AckPublisher ackPublisher;
    private final PoisonPublisher poisonPublisher;

    LineEventConsumer(PersistenceService persistenceService,
                      AckPublisher ackPublisher,
                      PoisonPublisher poisonPublisher) {
        this.persistenceService = persistenceService;
        this.ackPublisher = ackPublisher;
        this.poisonPublisher = poisonPublisher;
    }

    @KafkaListener(topics = ORDER_CREATED, groupId = "order-persistence-service")
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
            persistenceService.persist(event);
            ackPublisher.publishAck(event);
        } catch (RuntimeException e) {
            // NACK: rethrow so the listener container redelivers the same record.
            log.warn("persistence failed for {} — will redeliver", event.messageId());
            throw e;
        }
        acknowledgment.acknowledge();
    }
}
