package com.example.orders.email;

import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Task 2.2 RED: one mock mail per line event, bounded in-memory store.
 * - render names customerId, sku, qty, price, orderId
 * - store is bounded (default 1000), keeps most recent, reports evictions
 * - lookup by orderId; unknown orderId → empty
 */
class MockMailSinkTest {

    @Test
    void rendersMailNamingAllRequiredFields() {
        OrderLineEventRecord record = sampleRecord("o-1", "acme", "sku-a", 2, 9.99);
        MockMailSink sink = new MockMailSink(1000);

        MockMail mail = sink.record(record);

        assertNotNull(mail.subject());
        String body = mail.body();
        assertTrue(body.contains("acme"), "body must name customerId");
        assertTrue(body.contains("sku-a"), "body must name sku");
        assertTrue(body.contains("2"), "body must name qty");
        assertTrue(body.contains("9.99"), "body must name price");
        assertTrue(body.contains("o-1"), "body must name orderId");
    }

    @Test
    void storeKeepsMailsGroupedByOrderId() {
        MockMailSink sink = new MockMailSink(1000);
        sink.record(sampleRecord("o-1", "acme", "a", 1, 1.0));
        sink.record(sampleRecord("o-1", "acme", "b", 2, 2.0));
        sink.record(sampleRecord("o-2", "beta", "c", 3, 3.0));

        List<MockMail> forOrder1 = sink.mailsFor("o-1");
        assertEquals(2, forOrder1.size());

        List<MockMail> all = sink.allMails();
        assertEquals(3, all.size());
    }

    @Test
    void unknownOrderIdReturnsEmptyList() {
        MockMailSink sink = new MockMailSink(1000);
        assertTrue(sink.mailsFor("no-such-order").isEmpty());
    }

    @Test
    void boundedStoreEvictsOldestAndReportsCount() {
        MockMailSink sink = new MockMailSink(3);
        for (int i = 0; i < 5; i++) {
            sink.record(sampleRecord("o-" + i, "cust", "sku-" + i, 1, 1.0));
        }

        assertEquals(3, sink.allMails().size(), "store must keep at most capacity mails");
        // most recent three survive: o-2, o-3, o-4
        assertTrue(sink.mailsFor("o-4").size() == 1);
        assertTrue(sink.mailsFor("o-2").size() == 1);
        assertTrue(sink.mailsFor("o-0").isEmpty(), "oldest must be evicted");
        assertEquals(2, sink.evictedCount());
    }

    private OrderLineEventRecord sampleRecord(String orderId, String customerId, String sku, int qty, double price) {
        return new OrderLineEventRecord("1", orderId, customerId, sku, qty, price, 0,
                "msg-" + orderId + "-" + sku, java.time.Instant.now());
    }
}
