package com.example.lab.producer;

/**
 * One entry of GET /lab/accepted: a running total as observed on {@code lab-accepted},
 * with the real partition/offset so the offset advance is visible.
 */
public record LabAcceptedDto(String customerId, double runningTotal, int partition, long offset) {
}
