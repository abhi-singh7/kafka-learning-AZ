package com.example.lab.consumer;

/** One consumed message with its routing coordinates: partition, offset, key, payload. */
public record LinePart(int partition, long offset, String key, String value) {
}
