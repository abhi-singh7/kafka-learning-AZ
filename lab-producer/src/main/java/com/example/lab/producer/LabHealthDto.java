package com.example.lab.producer;

/**
 * GET /health and GET /lab/health response: whether the broker is reachable
 * within the configured time (KafkaAdmin cluster id) + counts for the smoke test.
 */
public record LabHealthDto(Boolean brokerReachable, String clusterId, Integer pushedTotal,
        Long blockedSendsTotal) {
}
