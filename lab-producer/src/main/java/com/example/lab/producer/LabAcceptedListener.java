package com.example.lab.producer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Reads {@code lab-accepted} (the Streams lane output) with a dedicated lab group and
 * records every running total — value plus real partition/offset — in the tracker.
 * This is what makes GET /lab/answered show the stateful accumulator and lets
 * diagnostics prove the offsets advanced.
 */
@Component
class LabAcceptedListener {

    private final LabAcceptedTracker tracker;

    @Value("${lab.listener.accepted.group:lab-accepted-read}")
    private String groupId;

    LabAcceptedListener(LabAcceptedTracker tracker) {
        this.tracker = tracker;
    }

    @KafkaListener(topics = "${lab.topic.accepted}", groupId = "${lab.listener.accepted.group:lab-accepted-read}")
    void onAccepted(AcceptedTotal total, ConsumerRecord<String, AcceptedTotal> record) {
        tracker.add(new LabAcceptedDto(total.customerId(), total.runningTotal(),
                record.partition(), record.offset()));
    }
}
