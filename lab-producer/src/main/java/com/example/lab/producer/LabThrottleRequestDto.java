package com.example.lab.producer;

/**
 * POST /lab/throttle request body: how many messages to try to push (count),
 * each padded to recordBytes so the producer's memory budget is exercised in
 * proportion to the traffic it is asked to absorb through the bounded-queue lane.
 */
public record LabThrottleRequestDto(Integer count, Integer recordBytes) {
}
