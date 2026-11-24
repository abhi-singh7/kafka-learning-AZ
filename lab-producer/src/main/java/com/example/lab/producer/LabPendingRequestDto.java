package com.example.lab.producer;

/** Request body of POST /lab/pending: one pending order amount for a customer. */
public record LabPendingRequestDto(String customerId, double amount) {
}
