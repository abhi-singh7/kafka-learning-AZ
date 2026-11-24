package com.example.lab.producer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.springframework.stereotype.Component;

/**
 * In-memory view of what the Kafka Streams lane has written to {@code lab-accepted}:
 * each running total with its real partition/offset. Feeds GET /lab/accepted and the
 * diagnostics offset advance for {@code lab-accepted}.
 */
@Component
class LabAcceptedTracker {

    private final List<LabAcceptedDto> accepted = new CopyOnWriteArrayList<>();

    void add(LabAcceptedDto entry) {
        accepted.add(entry);
    }

    List<LabAcceptedDto> snapshot() {
        return new ArrayList<>(accepted);
    }
}
