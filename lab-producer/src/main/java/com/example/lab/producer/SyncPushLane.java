package com.example.lab.producer;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

/**
 * Demo lane for the SYNC-acknowledgment mode: producer properties set
 * {@code synchronous=true} + full acks, so {@code send()} returns an ALREADY
 * resolved future and coordinates are readable WITHOUT waiting — in contrast to
 * the SHIP async lane, which hands back a CompletableFuture. (The truly blocking
 * anti-pattern is separate, see LabDoNotShip.java.) One bean exists; the
 * property-selected active lane is logged at startup by LabBeans (the app's
 * SHIP lane stays async).
 */
public final class SyncPushLane implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(SyncPushLane.class);

    private final DefaultKafkaProducerFactory<String, OrderLineEvent> producerFactory;
    private final KafkaTemplate<String, OrderLineEvent> kafkaTemplate;
    private final Long maxBlockingMillis;

    SyncPushLane(String acks, String queueSize, String maxAttempts, String lingerMs,
                 String batchSize, String maxBlockingMs, String bootstrapServers) {
        this.maxBlockingMillis = Long.parseLong(maxBlockingMs);
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", JacksonJsonSerializer.class);
        props.put("acks", acks);
        props.put("queue.size", queueSize);
        props.put("max.attempts", maxAttempts);
        props.put("linger.ms", lingerMs);
        props.put("batch.size", batchSize);
        props.put("max.blocking", maxBlockingMs);
        // kafka-clients producer property: resolve the send future synchronously
        // before send() returns (demo of sync producer semantics).
        props.put("synchronous", "true");
        this.producerFactory = new DefaultKafkaProducerFactory<>(props);
        this.kafkaTemplate = new KafkaTemplate<>(producerFactory);
        log.info("sync push lane constructed (acks={})", acks);
    }

    /**
     * Pushes one record and returns the send result. With {@code synchronous=true}
     * the future is resolved when send() returns, so getNow() never blocks;
     * if it is somehow still pending we fail loudly rather than wait.
     *
     * DO-NOT-SHIP usage pattern would be send().join()/get() — see LabDoNotShip.
     *
     * @return the resolved send result with partition/offset coordinates
     */
    public SendResult<String, OrderLineEvent> pushSync(ProducerRecord<String, OrderLineEvent> record) {
        java.util.concurrent.CompletableFuture<SendResult<String, OrderLineEvent>> attempt =
                kafkaTemplate.send(record);
        // SYNC-acknowledgment demo: this lane deliberately waits (BOUNDED by
        // max.blocking from properties) for the broker ack so the handler can read
        // coordinates straight away. That is the sync MODE being contrasted: the
        // SHIP lane above never waits, and LabDoNotShip waits WITHOUT a bound.
        try {
            attempt.get(maxBlockingMillis, java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.TimeoutException ackTimedOut) {
            throw new RuntimeException("sync lane timed out after " + maxBlockingMillis + " ms waiting for the ack");
        } catch (java.util.concurrent.ExecutionException ackRejected) {
            // The bounded wait finished and the broker rejected the send.
            throw new RuntimeException("sync lane send was rejected: " + ackRejected.getMessage());
        } catch (java.lang.InterruptedException ackWaitInterrupted) {
            throw new RuntimeException(ackWaitInterrupted);
        }
        SendResult<String, OrderLineEvent> resolved = attempt.getNow(null);
        if (resolved == null) {
            throw new RuntimeException("sync lane send was rejected after the bounded wait");
        }
        RecordMetadata metadata = resolved.getRecordMetadata();
        log.info("sync lane produced record: partition={} offset={}",
                metadata.partition(), metadata.offset());
        return resolved;
    }

    public void destroy() {
        kafkaTemplate.destroy();
        producerFactory.destroy();
    }
}
