package com.example.orders.email;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Task 2.2 GREEN: bounded in-memory mock-mail store (default capacity 1000).
 * Keeps the most recent {@code capacity} mails; evicts oldest first and counts
 * evictions. No SMTP, no Postgres — one log line per rendered mail.
 */
public class MockMailSink {

    private static final Logger log = LoggerFactory.getLogger(MockMailSink.class);
    public static final int DEFAULT_CAPACITY = 1000;

    private final int capacity;
    private final ConcurrentLinkedDeque<MockMail> mails = new ConcurrentLinkedDeque<>();
    private final AtomicLong evicted = new AtomicLong();

    public MockMailSink() {
        this(DEFAULT_CAPACITY);
    }

    public MockMailSink(int capacity) {
        this.capacity = capacity;
    }

    /** Renders and stores one mock mail for the given line event. */
    public MockMail record(OrderLineEventRecord event) {
        String subject = "Order %s — item %s (qty %d)".formatted(event.orderId(), event.sku(), event.qty());
        String body = """
                Order confirmation (mock mail)

                Order ID:   %s
                Customer:   %s
                SKU:        %s
                Quantity:   %d
                Unit price: %.2f
                Line index: %d
                Message:    %s
                """.formatted(event.orderId(), event.customerId(), event.sku(),
                event.qty(), event.price(), event.lineIndex(), event.messageId());

        MockMail mail = new MockMail(event.orderId(), subject, body, Instant.now());
        mails.addLast(mail);
        while (mails.size() > capacity) {
            if (mails.pollFirst() != null) {
                evicted.incrementAndGet();
            }
        }
        log.info("mock mail rendered for order {} sku {} (store size {}, evicted {})",
                event.orderId(), event.sku(), mails.size(), evicted.get());
        return mail;
    }

    /** All stored mails, oldest first. */
    public List<MockMail> allMails() {
        return new ArrayList<>(mails);
    }

    /** Mails for one order, oldest first; empty if none. */
    public List<MockMail> mailsFor(String orderId) {
        List<MockMail> out = new ArrayList<>();
        for (MockMail m : mails) {
            if (m.orderId().equals(orderId)) {
                out.add(m);
            }
        }
        return out;
    }

    public long evictedCount() {
        return evicted.get();
    }
}
