package com.example.lab.consumer;

import java.util.concurrent.atomic.AtomicInteger;

/** In-process queue gauge — one of the TWO "where is my data" answers (the other, broker-side lag,
 * comes from /lab/diagnostics):
 * the number of records received but not yet finished processing — distinct from the
 * broker-side offset gap reported by /lab/diagnostics. */
public class LabQueue {

    private final AtomicInteger inFlight = new AtomicInteger(0);
    private final AtomicInteger totalReceived = new AtomicInteger(0);

    public void received() {
        totalReceived.incrementAndGet();
        inFlight.incrementAndGet();
    }

    public void done() {
        inFlight.decrementAndGet();
    }

    public int inFlight() {
        return Math.max(0, inFlight.get());
    }

    public int totalReceived() {
        return totalReceived.get();
    }
}
