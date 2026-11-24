package com.example.orders.email;

import java.time.Instant;

/** One rendered mock mail. No SMTP — observable via GET /emails. */
public record MockMail(String orderId, String subject, String body, Instant sentAt) {
}
