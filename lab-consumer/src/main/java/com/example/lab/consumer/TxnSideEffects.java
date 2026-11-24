package com.example.lab.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * The consumer-side transactional side effects: for each accepted order, push to ack-a AND
 * ack-b inside ONE Kafka transaction — either both topics advance or neither does (atomic).
 * A forced side-effect failure (FailureInjector) rolls the whole transaction back.
 *
 * <p>Rollback modes (lab.rollback.mode): retry-next-poll rethrows so the record is
 * reprocessed on a later poll; discard skips it with a log line naming its offset.
 */
@Component
class TxnSideEffects {

    private static final Logger log = LoggerFactory.getLogger(TxnSideEffects.class);

    private final KafkaTemplate<String, String> template;
    private final FailureInjector injector;
    private final String ackA;
    private final String ackB;
    private final String rollbackMode;
    /**
     * A FRESH transactional.id per transaction. Reusing one id across transactions
     * (each executeInTransaction creates+closes a producer) makes the broker bump
     * the producer epoch on re-init; Kafka 4.x then exposes records of earlier
     * ABORTED transactions to consumers — breaking the atomicity guarantee.
     */
    private final AtomicLong txnSeq = new AtomicLong();

    TxnSideEffects(KafkaTemplate<String, String> sideEffectKafkaTemplate,
                   FailureInjector injector,
                   @Value("${lab.topic.ack.a:lab-ack-a}") String ackA,
                   @Value("${lab.topic.ack.b:lab-ack-b}") String ackB,
                   @Value("${lab.rollback.mode:retry-next-poll}") String rollbackMode) {
        this.template = sideEffectKafkaTemplate;
        this.injector = injector;
        this.ackA = ackA;
        this.ackB = ackB;
        this.rollbackMode = rollbackMode;
    }

    /**
     * Push one payload to ack-a and ack-b atomically. On rollback the mode decides whether
     * we skip (discard) or rethrow for a later poll (retry-next-poll).
     */
    void pushPair(ConsumerRecord<String, String> record, String orderId, String payload) {
        // Unique id per tx: "lab-txn-c-<n>-" + 0 (template appends the producer index).
        template.setTransactionIdPrefix("lab-txn-c-" + txnSeq.incrementAndGet() + "-");
        try {
            template.executeInTransaction(ops -> {
                ops.send(new ProducerRecord<>(ackA, orderId, payload));
                if (injector.failTxnSideEffect()) {
                    throw new IllegalStateException("forced side-effect failure");
                }
                ops.send(new ProducerRecord<>(ackB, orderId, payload));
                return null;
            });
        } catch (RuntimeException ex) {
            if ("discard".equals(rollbackMode)) {
                log.info("discarding topic={} partition={} offset={} after transaction rollback (mode=discard)",
                        record.topic(), record.partition(), record.offset());
                return; // skipped with a log line; the listener acks it
            }
            throw ex; // retry-next-poll: reprocess on a later poll
        }
    }
}
