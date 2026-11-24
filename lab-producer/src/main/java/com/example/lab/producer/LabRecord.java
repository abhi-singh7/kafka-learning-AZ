package com.example.lab.producer;

import java.util.ArrayList;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One place decides what every produced record looks like — topic, key (null for unkeyed
 * lanes), contract headers, payload. Keeping it here means the controller never assembles
 * records inline, and the log line shows exactly what is being sent.
 */
public final class LabRecord {

    private static final Logger log = LoggerFactory.getLogger(LabRecord.class);

    private LabRecord() {
    }

    /**
     * Build a record for lab-orders with the contract headers
     * x-lab-trace / x-lab-correlation / x-lab-schema and the given key.
     *
     * @return producer record ready for KafkaTemplate.send
     */
    public static ProducerRecord<String, OrderLineEvent> of(
            String topic, String key, String traceId, String correlationId, OrderLineEvent event) {
        java.util.ArrayList<org.apache.kafka.common.header.Header> headers = new java.util.ArrayList<>();
        headers.add(new RecordHeader("x-lab-trace", traceId.getBytes()));
        headers.add(new RecordHeader("x-lab-correlation", correlationId.getBytes()));
        headers.add(new RecordHeader("x-lab-schema", event.schemaVersion().getBytes()));
        log.info("produce record shape: key={} headers={} payload-present={}",
                key, "x-lab-trace,x-lab-correlation,x-lab-schema", event != null);
        return new ProducerRecord<String, OrderLineEvent>(topic, null, key, event, headers);
    }
}
