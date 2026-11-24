package com.example.lab.consumer;

import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;


/**
 * All @KafkaListener methods of the consumer side. Each lane is a separate consumer group,
 * so each reads the topics independently with its own committed offsets:
 *
 * <ul>
 *   <li><b>main lane</b> — group lab-consumer on the orders topic. Does the real work:
 *       ack strategy (before/after processing), JSON decode, dedupe via the ledger,
 *       schema check, injected failures → bounded retry → quarantine.</li>
 *   <li><b>race lane</b> — TWO listeners sharing ONE group (lab-competitor) on the rebalance
 *       topic. The group shares offsets, so total processings across both = message count;
 *       stopping one triggers a rebalance and the other picks up its partitions.</li>
 *   <li><b>txn lane</b> — group lab-txn on the orders topic again; pushes ack-a + ack-b
 *       atomically inside ONE Kafka transaction per record.</li>
 *   <li><b>avro lane</b> — group from properties on the avro topic; values decoded by the
 *       Confluent Avro deserializer (generic records, schema id from the envelope).</li>
 * </ul>
 *
 * Group names and topic names come from properties (never hardcoded in Java).
 */
@Component
class LabListeners {

    private static final Logger log = LoggerFactory.getLogger(LabListeners.class);

    private final LabLedger ledger;
    private final LabQueue queue;
    private final FailureInjector failureInjector;
    private final QuarantineWriter quarantineWriter;
    private final tools.jackson.databind.ObjectMapper mapper;
    private final com.example.lab.consumer.TxnSideEffects txnSideEffects;
    private final LabAvroTracker avroTracker;
    private final String ackStrategy;

    LabListeners(LabLedger ledger, LabQueue queue, FailureInjector failureInjector,
                 QuarantineWriter quarantineWriter, TxnSideEffects txnSideEffects,
                 LabAvroTracker avroTracker,
                 @Value("${lab.ack.strategy:after-processed}") String ackStrategy) {
        this.ledger = ledger;
        this.queue = queue;
        this.failureInjector = failureInjector;
        this.quarantineWriter = quarantineWriter;
        this.txnSideEffects = txnSideEffects;
        this.avroTracker = avroTracker;
        this.mapper = new tools.jackson.databind.ObjectMapper();
        this.ackStrategy = ackStrategy;
    }

    /**
     * Main lane handler. The Acknowledgment parameter is what makes MANUAL ack mode
     * possible: the offset only advances when we call ack.acknowledge() — and WHERE in this
     * method that call happens IS the experiment:
     * <ul>
     *   <li>immediate — commit BEFORE processing (a crash now loses the record: at-most-once);
     *   <li>after-processed (default) — commit only after handling completes (at-least-once,
     *       which is why the ledger dedupes below).</li>
     * </ul>
     * A thrown exception deliberately does NOT ack — the error handler takes over
     * (retry with backoff, then quarantine).
     */
    @KafkaListener(id = "${lab.group.main}", topics = "${lab.topic.orders}")
    void mainLane(ConsumerRecord<String, String> record, Acknowledgment ack) {
        queue.received();
        boolean commitBefore = "immediate".equals(ackStrategy);
        if (commitBefore) {
            ack.acknowledge(); // loss window: this record is committed before it is processed
            log.info("commit-point=before-processed topic={} partition={} offset={}",
                    record.topic(), record.partition(), record.offset());
        }
        try {
            process(record);
        } catch (RuntimeException ex) {
            // NO ack here: the offset must not advance past an unacked message.
            // The CommonErrorHandler retries with backoff, then quarantines and commits.
            queue.done();
            throw ex;
        }
        if (!commitBefore) {
            ack.acknowledge(); // after-processed: commit only once handling is done
            log.info("commit-point=after-processed topic={} partition={} offset={}",
                    record.topic(), record.partition(), record.offset());
        }
        queue.done();
    }

    /**
     * The actual per-record decision chain for the main lane, in order:
     * <ol>
     *   <li>JSON decode — undecodable payload → quarantine (reason json-decode), never a throw;</li>
     *   <li>dedupe by orderId via the ledger — a redelivery of an already-applied order is
     *       skipped, so at-least-once delivery still applies each order exactly once;</li>
     *   <li>schema version check — unknown version → quarantine (reason schema-mismatch);
     *   <li>injected-failure hook — a throw here exercises the retry-then-quarantine path;
     *   <li>otherwise: record the application in the ledger and log "accept".</li>
     * </ol>
     */
    private void process(ConsumerRecord<String, String> record) {
        LabOrderEvent event;
        try {
            event = mapper.readValue(record.value(), LabOrderEvent.class);
        } catch (tools.jackson.core.JacksonException decodeFailure) {
            quarantineWriter.quarantine(record, "json-decode", null);
            logLifecycle(record, "reject-quarantine");
            return;
        }
        if (ledger.seen(event.orderId())) {
            // Redelivery of an already-applied order: count it once, skip the rest.
            logLifecycle(record, "skip-duplicate");
            return;
        }
        if (!"1".equals(event.schemaVersion())) {
            quarantineWriter.quarantine(record, "schema-mismatch", event.schemaVersion());
            logLifecycle(record, "reject-quarantine");
            return;
        }
        try {
            failureInjector.maybeThrow("lab-orders");
        } catch (RuntimeException ex) {
            quarantineWriter.noteAttempt(record);
            logLifecycle(record, "retry");
            throw ex; // bounded retry with backoff, then quarantine (handler-throw)
        }
        ledger.apply(event.orderId(), record.topic(), record.partition(), record.offset());
        logLifecycle(record, "accept");
    }

    /** Race lane, listener A. Same group as raceB: the broker assigns each partition to
     *  exactly one of them, so every message is processed by exactly one in total. */
    @KafkaListener(id = "lab-race-a", groupId = "${lab.group.competitor}", topics = "${lab.topic.rebalance}")
    void raceA(ConsumerRecord<String, String> record, Acknowledgment ack) {
        log.info("race=a topic={} partition={} offset={} key={}",
                record.topic(), record.partition(), record.offset(), record.key());
        RaceCounter.record("a");
        ack.acknowledge(); // commit so a survivor does not re-process after the rebalance
    }

    /** Race lane, listener B — the competitor that POST /lab/listeners/competitor/off stops. */
    @KafkaListener(id = "lab-race-b", groupId = "${lab.group.competitor}", topics = "${lab.topic.rebalance}")
    void raceB(ConsumerRecord<String, String> record, Acknowledgment ack) {
        log.info("race=b topic={} partition={} offset={} key={}",
                record.topic(), record.partition(), record.offset(), record.key());
        RaceCounter.record("b");
        ack.acknowledge(); // commit so a survivor does not re-process after the rebalance
    }

    /**
     * Txn lane handler: decodes the order (quarantining bad payloads like the main lane),
     * then pushes ack-a + ack-b inside ONE Kafka transaction. No Acknowledgment parameter —
     * the container commits per record automatically, including records that rolled back
     * (that is what the AfterRollbackProcessor's discard mode needs).
     */
    @KafkaListener(id = "${lab.group.txn}", topics = "${lab.topic.orders}", containerFactory = "txnListenerContainerFactory")
    void txnLane(ConsumerRecord<String, String> record) {
        queue.received();
        try {
            LabOrderEvent event;
            try {
                event = mapper.readValue(record.value(), LabOrderEvent.class);
            } catch (tools.jackson.core.JacksonException decodeFailure) {
                quarantineWriter.quarantine(record, "json-decode", null);
                return;
            }
            if (!"1".equals(event.schemaVersion())) {
                quarantineWriter.quarantine(record, "schema-mismatch", event.schemaVersion());
                return;
            }
            txnSideEffects.pushPair(record, event.orderId(), record.value());
            log.info("txn-lane accepted orderId={} topic={} partition={} offset={}",
                    event.orderId(), record.topic(), record.partition(), record.offset());
        } finally {
            queue.done();
        }
    }

    /**
     * AVRO lane: decode OrderEvent from lab-avro via the Confluent Avro deserializer and
     * record it in the tracker. Generic decoding reads by the envelope's schema id, so old
     * (v1) records are read with whatever schema the registry holds for them — optional
     * fields come back null when the writer's schema lacked them.
     */
    @KafkaListener(id = "${lab.group.avro}", topics = "${lab.topic.avro}",
            containerFactory = "avroListenerContainerFactory")
    void avroLane(ConsumerRecord<String, GenericData.Record> record) {
        GenericData.Record r = record.value();
        String orderId = str(r.get("orderId"));
        String customer = str(r.get("customer"));
        double amount = ((Number) r.get("amount")).doubleValue();
        Double discount = r.hasField("discount") ? ((Number) r.get("discount")).doubleValue() : null;
        String phoneNumber = r.hasField("phoneNumber") ? str(r.get("phoneNumber")) : null;
        avroTracker.record(new LabAvroTracker.AvroEntry(orderId, customer, amount,
                discount, phoneNumber, record.partition(), record.offset()));
        log.info("avro-lane orderId={} customer={} amount={} discount={} phone={} partition={} offset={}",
                orderId, customer, amount, discount, phoneNumber, record.partition(), record.offset());
    }

    @KafkaListener(id = "person", topics = "${lab.topic.person}")
    void personLane(ConsumerRecord<String, Object> record) {
        String key = record.key();
        Object value = record.value();
        log.info("Key is " + key + " and value is " + value);
    }

    private static String str(Object value) {
        return value == null ? null : value.toString();
    }

    /** One structured line per main-lane message: coordinates, headers, payload, decision. */
    private void logLifecycle(ConsumerRecord<String, String> record, String decision) {
        StringBuilder headers = new StringBuilder("[");
        for (org.apache.kafka.common.header.Header h : record.headers()) {
            if (headers.length() > 1) {
                headers.append(", ");
            }
            headers.append(h.key()).append("=").append(new String(h.value()));
        }
        headers.append("]");
        log.info("lifecycle topic={} partition={} offset={} key={} headers={} payload={} decision={}",
                record.topic(), record.partition(), record.offset(), record.key(),
                headers, record.value(), decision);
    }

    /** Shared processing counter for the two race listeners (one group ⇒ total = message count). */
    static final class RaceCounter {
        private static final java.util.concurrent.atomic.AtomicInteger TOTAL = new java.util.concurrent.atomic.AtomicInteger(0);

        static void record(String lane) {
            TOTAL.incrementAndGet();
        }

        static int total() {
            return TOTAL.get();
        }

        static void reset() {
            TOTAL.set(0);
        }
    }
}
