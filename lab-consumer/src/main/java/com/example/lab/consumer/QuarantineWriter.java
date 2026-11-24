package com.example.lab.consumer;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tools.jackson.databind.ObjectMapper;

/**
 * The quarantine sink: when a message exhausts its retry budget (or cannot be decoded /
 * fails the schema check), it is written to {@code lab-rejected} as a JSON envelope with
 * raw payload, failure reason, source coordinates and attempt count — then the error
 * handler ACKs the source record, so a bad message can never loop forever.
 *
 * <p>Records dropped by the x-lab-skip header filter NEVER land here (they are skipped
 * on purpose before any handler runs, which is the contrast the lab wants visible).
 */
public class QuarantineWriter {

    private static final Logger log = LoggerFactory.getLogger(QuarantineWriter.class);
    private static final int RAW_PAYLOAD_LIMIT = 1000;

    private final KafkaTemplate<String, String> template;
    private final ObjectMapper mapper;
    private final String topic;
    private final ConcurrentHashMap<String, Integer> attemptsByRecord = new ConcurrentHashMap<>();

    public QuarantineWriter(KafkaTemplate<String, String> template, ObjectMapper mapper, String topic) {
        this.template = template;
        this.mapper = mapper;
        this.topic = topic;
    }

    /** The listener calls this on every failed delivery so the quarantine envelope can report the budget. */
    public void noteAttempt(ConsumerRecord<?, ?> record) {
        attemptsByRecord.merge(attemptKey(record), 1, Integer::sum);
    }

    /** How many times THIS record (topic+partition+offset) has been attempted. */
    public int attemptsFor(ConsumerRecord<?, ?> record) {
        return attemptsByRecord.getOrDefault(attemptKey(record), 0);
    }

    /**
     * Write the failed record to the quarantine topic as a JSON envelope. The raw payload is
     * truncated at 1000 chars so one huge bad message cannot bloat the topic.
     */
    public void quarantine(ConsumerRecord<?, ?> source, String reason, String schemaVersion) {
        String raw = source.value() == null ? "" : source.value().toString();
        boolean truncated = raw.length() > RAW_PAYLOAD_LIMIT;
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("schemaVersion", schemaVersion);
        body.put("reason", reason);
        body.put("rawPayload", truncated ? raw.substring(0, RAW_PAYLOAD_LIMIT) : raw);
        body.put("truncated", truncated);
        body.put("sourceTopic", source.topic());
        body.put("sourcePartition", source.partition());
        body.put("sourceOffset", source.offset());
        body.put("detectedBy", "lab-consumer");
        body.put("attempts", Math.max(1, attemptsFor(source)));
        body.put("firstFailureAt", Instant.now().toString());
        body.put("lastFailureAt", Instant.now().toString());
        String key = source.key() == null ? "corrupt" : source.key().toString();
        try {
            template.send(new ProducerRecord<>(topic, key, mapper.writeValueAsString(body)));
            log.info("quarantined {}@{}:{} reason={} attempts={}", source.topic(), source.partition(), source.offset(),
                    reason, body.get("attempts"));
        } catch (Exception e) {
            throw new IllegalStateException("quarantine write failed for " + source, e);
        }
    }

    /** Read a header value off a record (null-safe); used by the skip filter. */
    static String headerValue(ConsumerRecord<?, ?> record, String name) {
        org.apache.kafka.common.header.Header h = record.headers().lastHeader(name);
        return h == null ? null : new String(h.value());
    }

    /** Identity of a delivery: topic-partition-offset (stable across retries of the same record). */
    private static String attemptKey(ConsumerRecord<?, ?> record) {
        return record.topic() + "-" + record.partition() + "-" + record.offset();
    }
}
