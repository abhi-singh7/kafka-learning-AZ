package com.example.orders.email;

import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * Task 2.2 GREEN: demo navigation surface for rendered mock mails.
 * - GET /emails → all stored mails (oldest first), [] when none
 * - GET /orders/{orderId}/emails → mails for that order, 404 when unknown
 */
@RestController
class MailController {

    private final MockMailSink sink;
    private final PoisonStore poisonStore;

    MailController(MockMailSink sink, PoisonStore poisonStore) {
        this.sink = sink;
        this.poisonStore = poisonStore;
    }

    @GetMapping("/orders/failed")
    public List<PoisonStore.PoisonRecord> failedOrders() {
        return poisonStore.all();
    }

    @GetMapping("/emails")
    public List<MockMail> allMails() {
        return sink.allMails();
    }

    @GetMapping("/orders/{orderId}/emails")
    public List<MockMail> mailsForOrder(@PathVariable String orderId) {
        List<MockMail> mails = sink.mailsFor(orderId);
        if (mails.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "no mails for order " + orderId);
        }
        return mails;
    }

    @GetMapping("/health")
    public HealthDto health() {
        return new HealthDto("UP", sink.allMails().size(), sink.evictedCount());
    }

    public record HealthDto(String status, int mailsStored, long evicted) {
    }
}
