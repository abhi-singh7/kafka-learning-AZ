package com.example.lab.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Component;

/**
 * Graceful shutdown. The Boot-managed listener containers stop and commit what was
 * legitimately ACKed; this bean logs the pending (in-flight) count so the backlog is
 * observable, and exits cleanly (no poll-loop stack trace). Un-ACKed messages stay at
 * their offsets and are picked up again on the next start.
 */
@Component
public class LabShutdown implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(LabShutdown.class);

    private final LabQueue queue;
    private final LabLedger ledger;

    public LabShutdown(LabQueue queue, LabLedger ledger) {
        this.queue = queue;
        this.ledger = ledger;
    }

    @Override
    public void destroy() {
        log.info("shutdown: pending={} applied-orders={} (un-ACKed messages stay at their offsets for the next start)",
                queue.inFlight(), ledger.count());
    }
}
