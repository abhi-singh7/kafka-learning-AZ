package com.example.orders.email;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

/** Bounded in-memory store of poison records observed by email-service. */
public class PoisonStore {

    public record PoisonRecord(String detectedBy, String sourceTopic, String reason,
                               String rawPayload, Instant receivedAt) {
    }

    private static final int RAW_PAYLOAD_MAX = 1000;

    private final ConcurrentLinkedDeque<PoisonRecord> records = new ConcurrentLinkedDeque<>();
    private final int capacity;

    public PoisonStore() {
        this(1000);
    }

    public PoisonStore(int capacity) {
        this.capacity = capacity;
    }

    public PoisonRecord record(String detectedBy, String sourceTopic, String reason, String rawPayload) {
        String truncated = rawPayload == null ? "" :
                (rawPayload.length() > RAW_PAYLOAD_MAX
                        ? rawPayload.substring(0, RAW_PAYLOAD_MAX) : rawPayload);
        PoisonRecord rec = new PoisonRecord(detectedBy, sourceTopic, reason, truncated, Instant.now());
        records.addLast(rec);
        while (records.size() > capacity) {
            records.pollFirst();
        }
        return rec;
    }

    public List<PoisonRecord> all() {
        return new ArrayList<>(records);
    }
}
