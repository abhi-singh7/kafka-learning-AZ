package com.example.lab.producer;

/**
 * Value record on {@code lab-accepted}: the stateful per-customer RUNNING TOTAL
 * produced by the Kafka Streams lane after each pending order is folded in.
 */
public record AcceptedTotal(String customerId, double runningTotal) {
}
