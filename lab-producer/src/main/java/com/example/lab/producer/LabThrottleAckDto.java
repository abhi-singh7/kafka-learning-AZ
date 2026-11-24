package com.example.lab.producer;

/**
 * 429/202 body of the throttle lane: how many messages this call pushed
 * before back-pressure kicked in, and the running total of sends the producer
 * had to block (then reject) since startup.
 */
public record LabThrottleAckDto(Integer pushedThisCall, Long blockedSendsTotal) {
}
