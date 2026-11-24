package com.example.lab.producer;

/**
 * A single validation problem: the offending field (e.g. {@code items[0].qty})
 * and a human-readable reason. Serialized into the 422 response body.
 */
public record ValidationFailure(String field, String reason) {
}
