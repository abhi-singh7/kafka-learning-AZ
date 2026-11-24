package com.example.orders.api;

/**
 * A single validation problem: the offending field (e.g. {@code items[0].qty})
 * and a human-readable reason. Serialized into the 422 response body.
 */
public record OrderValidationError(String field, String reason) {
}
