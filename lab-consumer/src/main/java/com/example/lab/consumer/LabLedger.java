package com.example.lab.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * The seen-ledger: the answer to "have we already handled this?". Because the main lane
 * commits AFTER processing (at-least-once), a record can legitimately arrive twice — the
 * ledger is what makes each order apply exactly once.
 *
 * <p>Keyed by the BUSINESS key (orderId), not topic partition+offset: the same business
 * event replayed at a different partition/offset is still one event and must be applied
 * once. (The wrong choice — keying by partition+": "+offset — is documented as DO-NOT-SHIP
 * in docs/experiments.md and deliberately NOT wired here.)
 */
public class LabLedger {

    private final ConcurrentHashMap<String, Long> firstSeenNanos = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<OffsetStamp> stamps = new ConcurrentLinkedQueue<>();

    /** Where an application happened (topic/partition/offset) — kept for inspection. */
    record OffsetStamp(String topic, int partition, long offset) {
    }

    /** True when the key was already applied: the caller ACKs and skips. */
    public boolean seen(String key) {
        return firstSeenNanos.containsKey(key);
    }

    /** Records the application; a second call for the same key does not add a second entry. */
    public void apply(String key, String topic, int partition, long offset) {
        firstSeenNanos.putIfAbsent(key, System.nanoTime());
        stamps.add(new OffsetStamp(topic, partition, offset));
    }

    public List<String> keys() {
        return new ArrayList<>(firstSeenNanos.keySet());
    }

    public int count() {
        return firstSeenNanos.size();
    }

    public Long firstSeenNanos(String key) {
        return firstSeenNanos.get(key);
    }

    /** All application stamps in apply order (diagnostics/tests). */
    public List<OffsetStamp> stamps() {
        return new ArrayList<>(stamps);
    }
}
