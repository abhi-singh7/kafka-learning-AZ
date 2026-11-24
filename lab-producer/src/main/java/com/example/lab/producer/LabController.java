package com.example.lab.producer;

import java.util.List;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartitionInfo;
import org.springframework.kafka.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * REST surface of the push side (lab-producer, port 8091). Every endpoint demonstrates one
 * Kafka producer pattern, so read this file top-to-bottom as a tour:
 *
 * <ul>
 *   <li>{@code POST /lab/orders} — the SHIP async lane: validate the order, produce one
 *       record per line item, return 202 with partition/offset from the PUSH RESULT and a
 *       {@code Location: lab-orders/<partition>@<offset>} header.</li>
 *   <li>{@code POST /lab/orders/unkeyed} — same lane but WITHOUT a key, so the broker
 *       round-robins records across partitions instead of grouping by customer.</li>
 *   <li>{@code POST /lab/orders/sync} — the sync-ack demo lane ({@link SyncPushLane});
 *       the handler waits (bounded) for the broker ack before answering.</li>
 *   <li>{@code POST /lab/throttle} — bounded-queue back-pressure demo: a producer with a
 *       tiny buffer refuses work it cannot absorb (429 + Retry-After).</li>
 *   <li>{@code POST /lab/txn} — transactional push to two topics; all-or-nothing.</li>
 *   <li>{@code POST /lab/pending} / {@code GET /lab/accepted} — Kafka Streams lane: push a
 *       pending order, then read back the per-customer running totals it produced.</li>
 *   <li>{@code POST /lab/avro} — Avro lane: values serialized against the Confluent schema
 *       registry instead of plain JSON.</li>
 *   <li>{@code GET /health} / {@code GET /lab/health} — broker reachability + push counters.</li>
 *   <li>{@code GET /lab/pushed} / {@code GET /lab/diagnostics} — introspection: what this
 *       process pushed, and what the broker says about each topic/partition.</li>
 * </ul>
 *
 * All async endpoints return a reactive {@link CompletableFuture}; Boot WebMVC resolves it
 * without blocking the request thread. The one deliberately-blocking demo is isolated in
 * {@link LabDoNotShip} and is never called from any route.
 */
@RestController
class LabController {

    private static final Logger log = LoggerFactory.getLogger(LabController.class);

    private final KafkaTemplate<String, OrderLineEvent> orderKafkaTemplate;
    private final KafkaTemplate<String, String> rawKafkaTemplate;
    private final KafkaTemplate<String, Object> pendingKafkaTemplate;
    private final SyncPushLane orderSyncPush;
    private final KafkaTemplate<String, GenericData.Record> avroKafkaTemplate;
    private final Schema orderEventSchema;
    private final KafkaAdmin kafkaAdmin;
    private final LabTopicsDto topics;
    private final LabExtraTopicsDto extraTopics;
    private final LabAcceptedTracker acceptedTracker;

    @Value("${lab.topic.avro}")
    private String avroTopic;
    @Value("${lab.topic.person}")
    private String personTopic;

    private final List<LabPushAckDto> pushed = new CopyOnWriteArrayList<>();
    private final AtomicLong blockedSends = new AtomicLong();

    // Throttle lane tuning (properties only - kafka-contract: flow control via props).
    @Value("${lab.producer.throttle.retry.after:1}")
    private String throttleRetryAfterSeconds;

    @Value("${lab.producer.throttle.probe.timeout:250}")
    private Integer throttleProbeTimeoutMillis;

    LabController(KafkaTemplate<String, OrderLineEvent> orderKafkaTemplate,
                  KafkaTemplate<String, String> rawKafkaTemplate,
                  KafkaTemplate<String, Object> pendingKafkaTemplate,
                  KafkaTemplate<String, GenericData.Record> avroKafkaTemplate,
                  Schema orderEventSchema,
                  SyncPushLane orderSyncPush,
                  KafkaAdmin kafkaAdmin,
                  LabTopicsDto topics,
                  LabExtraTopicsDto extraTopics,
                  LabAcceptedTracker acceptedTracker) {
        this.orderKafkaTemplate = orderKafkaTemplate;
        this.rawKafkaTemplate = rawKafkaTemplate;
        this.pendingKafkaTemplate = pendingKafkaTemplate;
        this.avroKafkaTemplate = avroKafkaTemplate;
        this.orderEventSchema = orderEventSchema;
        this.orderSyncPush = orderSyncPush;
        this.kafkaAdmin = kafkaAdmin;
        this.topics = topics;
        this.extraTopics = extraTopics;
        this.acceptedTracker = acceptedTracker;
    }

    /**
     * Keyed lane: every record carries key = customerId, so all lines of one customer's
     * order land on the SAME partition (in-order per customer, no cross-customer mixing).
     */
    @PostMapping("/lab/orders")
    public CompletableFuture<ResponseEntity<LabPushAckDto>> push(@RequestBody OrderRequestDto request) {
        return pushAsync(request, true);
    }

    /** Unkeyed lane: key = null, so the broker assigns partitions round-robin. */
    @PostMapping("/lab/orders/unkeyed")
    public CompletableFuture<ResponseEntity<LabPushAckDto>> pushUnkeyed(@RequestBody OrderRequestDto request) {
        return pushAsync(request, false);
    }

    /**
     * Shared implementation of both order endpoints. Steps:
     * <ol>
     *   <li>validate the payload (422 with field-level errors when invalid);</li>
     *   <li>generate orderId + traceId here, on the producer side — consumers only echo them;</li>
     *   <li>fan the order out into one record per line item ({@link LabMapper});</li>
     *   <li>send each record async; the FIRST send's future is kept as the representative
     *       ack (all records share the same key, so they land on the same partition);</li>
     *   <li>on success → 202 + Location header read from the PUSH RESULT; on failure → 502.</li>
     * </ol>
     */
    private CompletableFuture<ResponseEntity<LabPushAckDto>> pushAsync(OrderRequestDto request,
            boolean keyed) {
        rejectInvalid(request);
        String orderId = UUID.randomUUID().toString();
        String traceId = UUID.randomUUID().toString();
        List<OrderLineEvent> events = LabMapper.toLineEvents(request, orderId, traceId);
        // kafka-contract spec: key = customerId (NOT the generated orderId) so that
        // every line of one customer's order lands on the same partition.
        String key = keyed ? request.customerId() : null;

        CompletableFuture<SendResult<String, OrderLineEvent>> acked = null;
        for (OrderLineEvent event : events) {
            ProducerRecord<String, OrderLineEvent> record =
                    LabRecord.of(topics.orders(), key, traceId, orderId, event);
            CompletableFuture<SendResult<String, OrderLineEvent>> result =
                    orderKafkaTemplate.send(record);
            if (acked == null) {
                acked = result;
            }
        }
        return acked.thenApply(result -> accepted(orderId, traceId, result))
                .exceptionally(error -> pushFailed(orderId, traceId, error));
    }

    /**
     * Contrast to the async lane: uses {@link SyncPushLane}, whose producer waits for the
     * broker ack (bounded by max.blocking) before send() returns, so this handler can read
     * partition/offset directly from a resolved result — no CompletableFuture chain.
     */
    @PostMapping("/lab/orders/sync")
    public ResponseEntity<LabPushAckDto> pushSync(@RequestBody OrderRequestDto request) {
        rejectInvalid(request);
        String orderId = UUID.randomUUID().toString();
        String traceId = UUID.randomUUID().toString();
        List<OrderLineEvent> events = LabMapper.toLineEvents(request, orderId, traceId);

        SendResult<String, OrderLineEvent> resolved = null;
        for (OrderLineEvent event : events) {
            ProducerRecord<String, OrderLineEvent> record =
                    LabRecord.of(topics.orders(), request.customerId(), traceId, orderId, event);
            SendResult<String, OrderLineEvent> result = orderSyncPush.pushSync(record);
            if (resolved == null) {
                resolved = result;
            }
        }
        return accepted(orderId, traceId, resolved);
    }

    /**
     * Back-pressure experiment: fires {@code count} sends through the raw lane, whose producer
     * has a TINY buffer (buffer.memory / max.block.ms from properties). If the queue cannot
     * absorb them, send() is refused and we answer 429 + Retry-After with how many got in;
     * otherwise 202. This is the observable form of "the producer stops you" — not an OOM.
     */
    @PostMapping("/lab/throttle")
    public ResponseEntity<LabThrottleAckDto> throttle(@RequestBody LabThrottleRequestDto request) {
        // lab-throttle lane: a producer configured with a TINY send queue and a SHORT
        // max.blocking (both from properties - see LabBeans + application.properties)
        // refuses to buffer more work than it can hand to the broker.
        java.util.ArrayList<java.util.concurrent.CompletableFuture<org.springframework.kafka.support.SendResult<String, String>>> fired =
                new java.util.ArrayList<>();
        String payload = padding(request.recordBytes());
        try {
            for (int i = 0; i < request.count(); i++) {
                fired.add(rawKafkaTemplate.send(topics.throttle(), String.valueOf(i),
                        "probe-" + i + "-" + payload));
            }
        } catch (Throwable refused) {
            // SYNCHRONOUS back-pressure: kafka-clients 7.x refuses the send because it
            // cannot allocate a message buffer within max.block.ms (the producer's own
            // memory budget). Everything already in `fired` was accepted by the lane.
            long overflow = request.count() - fired.size();
            blockedSends.addAndGet(overflow);
            log.warn("lab-throttle lane refused the send after {} accepted (queue/buffer full)",
                    fired.size());
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                    .header("Retry-After", throttleRetryAfterSeconds)
                    .body(new LabThrottleAckDto(fired.size(), blockedSends.get()));
        }

        // Observation only, with a BOUNDED wait per firehose slot: the SHIP lane above
        // never awaits; here the experiment must SEE the back-pressure to answer 429.
        long rejected = 0;
        for (java.util.concurrent.CompletableFuture<org.springframework.kafka.support.SendResult<String, String>> attempt : fired) {
            try {
                attempt.get(throttleProbeTimeoutMillis, java.util.concurrent.TimeUnit.MILLISECONDS);
            } catch (Throwable probeOutcome) {
                // TimeoutException => still queued (not a rejection).
                // KafkaException => the bounded queue refused this send.
                if (attempt.isCompletedExceptionally()) {
                    rejected++;
                    break;
                }
            }
            if (attempt.isCompletedExceptionally()) {
                rejected++;
                break;
            }
        }

        int pushedThisCall = fired.size() - (int) rejected;
        blockedSends.addAndGet(rejected);
        LabThrottleAckDto ack = new LabThrottleAckDto(pushedThisCall, blockedSends.get());
        if (rejected > 0) {
            log.warn("lab-throttle lane refused {} of {} sends (queue full)", rejected, fired.size());
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                    .header("Retry-After", throttleRetryAfterSeconds)
                    .body(ack);
        }
        return ResponseEntity.accepted().body(ack);
    }

    /**
     * Transactional push: both records (one per topic, ack-a + ack-b) are buffered inside ONE
     * Kafka transaction and commit atomically. If the handler throws — or forceRollback=true —
     * the broker rolls back and NEITHER topic advanced; we surface that as 409.
     */
    @PostMapping("/lab/txn")
    public ResponseEntity<LabTxnAckDto> txn(@RequestBody LabTxnPushRequestDto request) {
        LabTxnAckDto ack = new LabTxnAckDto(request.orderId(), topics.ackA(), topics.ackB());
        try {
            rawKafkaTemplate.executeInTransaction(
                    (org.springframework.kafka.core.KafkaOperations<String, String> ops) -> {
                        ops.send(topics.ackA(), request.orderId(), request.left());
                        ops.send(topics.ackB(), request.orderId(), request.right());
                        if (Boolean.TRUE.equals(request.forceRollback())) {
                            throw new RuntimeException("forced side-effect failure inside the transaction");
                        }
                        return ack;
                    });
            log.info("transaction committed across {} {}", topics.ackA(), topics.ackB());
            return ResponseEntity.accepted().body(ack);
        } catch (Throwable rollbackFailure) {
            // Spring Kafka rolled the buffered sends back: NEITHER topic advanced.
            log.warn("transaction rolled back for orderId={}: {}",
                    request.orderId(), rollbackFailure.getMessage());
            return ResponseEntity.status(HttpStatus.CONFLICT).body(ack);
        }
    }

    /** Pads the probe payload so the producer has to absorb a realistic body size. */
    private String padding(Integer recordBytes) {
        int size = recordBytes == null || recordBytes < 0 ? 0 : recordBytes;
        StringBuilder text = new StringBuilder();
        while (text.length() < size) {
            text.append('x');
        }
        return text.toString();
    }

    /**
     * Streams lane push side (Task 3.1): one pending order amount for a customer onto
     * {@code lab-pending} (JSON value, key = customerId). Non-blocking: the send is
     * handed to the producer and the ack comes back via the reactive return.
     */
    @PostMapping("/lab/pending")
    public CompletableFuture<ResponseEntity<LabPushAckDto>> pushPending(
            @RequestBody LabPendingRequestDto request) {
        if (request.customerId() == null || request.customerId().isBlank()) {
            throw new UnprocessableEntityException(
                    java.util.List.of(new ValidationFailure("customerId", "must not be blank")));
        }
        if (request.amount() < 0) {
            throw new UnprocessableEntityException(
                    java.util.List.of(new ValidationFailure("amount", "must be >= 0")));
        }
        PendingOrder order = new PendingOrder(request.customerId(), request.amount());
        CompletableFuture<SendResult<String, Object>> acked = pendingKafkaTemplate
                .send(topics.pending(), request.customerId(), order);
        return acked.thenApply(result -> {
            RecordMetadata metadata = result.getRecordMetadata();
            LabPushAckDto ack = new LabPushAckDto(request.customerId(), topics.pending(),
                    metadata.partition(), metadata.offset(), null, null);
            log.info("pending acknowledged: customerId={} topic={} partition={} offset={}",
                    request.customerId(), topics.pending(), metadata.partition(), metadata.offset());
            return ResponseEntity.accepted()
                    .header("Location", topics.pending() + "/" + metadata.partition() + "@" + metadata.offset())
                    .body(ack);
        }).exceptionally(error -> {
            log.error("pending push failed for customerId={}: {}",
                    request.customerId(), error.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_GATEWAY)
                    .body(new LabPushAckDto(request.customerId(), topics.pending(), null, null, null, null));
        });
    }

    /**
     * AVRO lane (Task 3.2): one OrderEvent onto {@code lab-avro} serialized by the Confluent
     * KafkaAvroSerializer against the schema registry. Non-blocking: the send is handed to
     * the producer and the ack returns via the reactive CompletableFuture (kafka-contract).
     */
    @PostMapping("/lab/avro")
    public CompletableFuture<ResponseEntity<LabPushAckDto>> pushAvro(@RequestBody LabAvroRequestDto request) {
        if (request.customer() == null || request.customer().isBlank()) {
            throw new UnprocessableEntityException(
                    java.util.List.of(new ValidationFailure("customer", "must not be blank")));
        }
        if (request.amount() == null || request.amount() < 0) {
            throw new UnprocessableEntityException(
                    java.util.List.of(new ValidationFailure("amount", "must be >= 0")));
        }
        String orderId = (request.orderId() == null || request.orderId().isBlank())
                ? UUID.randomUUID().toString() : request.orderId();
        GenericData.Record record = new GenericData.Record(orderEventSchema);
        record.put("orderId", orderId);
        record.put("customer", request.customer());
        record.put("amount", request.amount());
        if (request.discount() != null) {
            record.put("discount", request.discount());
        }
        if (request.phoneNumber() != null) {
            record.put("phoneNumber", request.phoneNumber());
        }
        CompletableFuture<SendResult<String, GenericData.Record>> acked =
                avroKafkaTemplate.send(avroTopic, orderId, record);
        return acked.thenApply(result -> {
            RecordMetadata metadata = result.getRecordMetadata();
            LabPushAckDto ack = new LabPushAckDto(orderId, avroTopic,
                    metadata.partition(), metadata.offset(), null, null);
            log.info("avro acknowledged: orderId={} topic={} partition={} offset={}",
                    orderId, avroTopic, metadata.partition(), metadata.offset());
            return ResponseEntity.accepted()
                    .header("Location", avroTopic + "/" + metadata.partition() + "@" + metadata.offset())
                    .body(ack);
        }).exceptionally(error -> {
            log.error("avro push failed for orderId={}: {}", orderId, error.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_GATEWAY)
                    .body(new LabPushAckDto(orderId, avroTopic, null, null, null, null));
        });
    }

    /**
     * Streams lane read side (Task 3.1): the per-customer running totals as observed on
     * {@code lab-accepted}, in push order, each with its real partition/offset.
     */
    @GetMapping("/lab/accepted")
    public List<LabAcceptedDto> accepted() {
        return acceptedTracker.snapshot();
    }

    /** Every push ack this process has produced, in push order (for smoke tests/demos). */
    @GetMapping("/lab/pushed")
    public List<LabPushAckDto> listPushed() {
        java.util.ArrayList<LabPushAckDto> snapshot = new java.util.ArrayList<>();
        for (LabPushAckDto ack : pushed) {
            snapshot.add(ack);
        }
        return snapshot;
    }

    /** Health at the root path (smoke scripts may probe either path). */
    @GetMapping("/health")
    public LabHealthDto healthAtRoot() {
        return healthSnapshot();
    }

    /** Same snapshot under /lab/health. */
    @GetMapping("/lab/health")
    public LabHealthDto healthLab() {
        return healthSnapshot();
    }

    @PostMapping("/test")
    public String personDetails(@RequestBody Person person){
        pendingKafkaTemplate.send(personTopic,person);
        return person.toString();

    }

    /** Broker reachable = KafkaAdmin can fetch a cluster id within the configured timeout. */
    private LabHealthDto healthSnapshot() {
        String clusterId = kafkaAdmin.clusterId();
        boolean reachable = clusterId != null;
        return new LabHealthDto(reachable, clusterId, pushed.size(), blockedSends.get());
    }

    /**
     * Broker-state introspection: for every lab topic (plus any extra topics configured via
     * lab.diagnostics.extra.topics) report whether it exists, its partition count, and the next
     * offset per partition as far as this process knows. A missing topic is reported as
     * exists=false instead of failing the whole call.
     */
    @GetMapping("/lab/diagnostics")
    public List<LabTopicDiagnosticDto> diagnostics() {
        java.util.ArrayList<String> names = new java.util.ArrayList<>();
        names.add(topics.orders());
        names.add(topics.throttle());
        names.add(topics.ackA());
        names.add(topics.ackB());
        names.add(topics.pending());
        names.add(topics.accepted());
        names.addAll(extraTopics.topics());

        // Describe each topic individually so a missing "contrast" topic (e.g. a sibling
        // order-* topic not yet created on this broker) is skipped gracefully instead of
        // failing the whole query with UnknownTopicOrPartition.
        java.util.ArrayList<LabTopicDiagnosticDto> result = new java.util.ArrayList<>();
        for (String name : names) {
            TopicDescription description;
            try {
                java.util.Map<String, TopicDescription> described = kafkaAdmin.describeTopics(name);
                description = described.get(name);
            } catch (KafkaException failure) {
                if (causedByUnknownTopic(failure)) {
                    result.add(new LabTopicDiagnosticDto(name, false, null,
                            new java.util.ArrayList<>()));
                    continue;
                }
                log.error("diagnostics: describeTopics failed for {}: {}", name, failure.getMessage());
                return new java.util.ArrayList<>();
            }
            if (description == null) {
                result.add(new LabTopicDiagnosticDto(name, false, null,
                        new java.util.ArrayList<>()));
                continue;
            }
            java.util.ArrayList<LabPartitionDiagnosticDto> offsets = new java.util.ArrayList<>();
            for (TopicPartitionInfo part : description.partitions()) {
                long last = -1;
                for (LabPushAckDto ack : pushed) {
                    if (name.equals(ack.topic()) && part.partition() == ack.partition()
                            && ack.offset() != null && ack.offset() > last) {
                        last = ack.offset();
                    }
                }
                // lab-accepted is written by the Streams lane, not by a push ack: its
                // offset advance comes from the accepted tracker (real offsets).
                if (name.equals(topics.accepted())) {
                    for (LabAcceptedDto entry : acceptedTracker.snapshot()) {
                        if (part.partition() == entry.partition() && entry.offset() > last) {
                            last = entry.offset();
                        }
                    }
                }
                long next = last < 0 ? -1 : last + 1;
                offsets.add(new LabPartitionDiagnosticDto(part.partition(),
                        last < 0 ? null : next));
            }
            result.add(new LabTopicDiagnosticDto(name, true,
                    description.partitions().size(), offsets));
        }
        return result;
    }

    /** True when the failure's cause chain contains an UnknownTopicOrPartitionException. */
    private boolean causedByUnknownTopic(Throwable error) {
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (t instanceof org.apache.kafka.common.errors.UnknownTopicOrPartitionException) {
                return true;
            }
        }
        return false;
    }

    private void rejectInvalid(OrderRequestDto request) {
        List<ValidationFailure> errors =
                LabValidation.validate(request.customerId(), request.items());
        if (!errors.isEmpty()) {
            throw new UnprocessableEntityException(errors);
        }
    }

    /** Build the 202 ack: coordinates come from the PUSH RESULT (never a local counter). */
    private ResponseEntity<LabPushAckDto> accepted(String orderId, String traceId,
            SendResult<String, OrderLineEvent> result) {
        RecordMetadata metadata = result.getRecordMetadata();
        Integer partition = metadata.partition();
        Long offset = metadata.offset();
        LabPushAckDto ack = new LabPushAckDto(orderId, topics.orders(), partition, offset,
                result.getProducerRecord().value().messageId(), traceId);
        pushed.add(ack);
        log.info("push acknowledged: orderId={} topic={} partition={} offset={}",
                orderId, topics.orders(), partition, offset);
        return ResponseEntity.accepted()
                .header("Location", topics.orders() + "/" + partition + "@" + offset)
                .body(ack);
    }

    /** 502 body when the produce future completes exceptionally (null coordinates). */
    private ResponseEntity<LabPushAckDto> pushFailed(String orderId, String traceId,
            Throwable error) {
        log.error("push failed for orderId={}: {}", orderId, error.getMessage());
        return ResponseEntity.status(HttpStatus.BAD_GATEWAY)
                .body(new LabPushAckDto(orderId, topics.orders(), null, null, null, traceId));
    }

    public record UnprocessableEntityBody(java.util.List<ValidationFailure> errors) {
    }

    static final class UnprocessableEntityException extends ResponseStatusException {
        private final java.util.List<ValidationFailure> errors;

        UnprocessableEntityException(java.util.List<ValidationFailure> errors) {
            super(HttpStatus.UNPROCESSABLE_ENTITY, "order validation failed");
            this.errors = errors;
        }

        public java.util.List<ValidationFailure> getErrors() {
            return errors;
        }
    }
}
