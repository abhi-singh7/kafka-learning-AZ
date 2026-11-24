package com.example.lab.producer;

/**
 * Value record on {@code lab-pending}: one pending order amount for a customer.
 * Serialized to JSON by Boot's Jackson serializer (type header carries this class).
 */
public record PendingOrder(String customerId, double amount) {
}
