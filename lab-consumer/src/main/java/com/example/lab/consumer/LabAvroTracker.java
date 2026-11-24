package com.example.lab.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Records OrderEvent values decoded by the AVRO lane (group lab-avro-read). The optional
 * {@code discount}/{@code phoneNumber} fields are null when the writer's schema lacked them
 * and no default applied - which is how an old (v1) record reads on the latest (v2) schema.
 */
class LabAvroTracker {

    public record AvroEntry(String orderId, String customer, double amount,
            Double discount, String phoneNumber, int partition, long offset) {
    }

    private final List<AvroEntry> entries = new CopyOnWriteArrayList<>();

    void record(AvroEntry entry) {
        entries.add(entry);
    }

    List<AvroEntry> snapshot() {
        return new ArrayList<>(entries);
    }
}
