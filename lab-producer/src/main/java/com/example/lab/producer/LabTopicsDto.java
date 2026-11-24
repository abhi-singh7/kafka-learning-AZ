package com.example.lab.producer;

/**
 * Topic NAMES the lab uses, injected from properties so controllers never
 * hardcode them (kafka-contract B1).
 */
public record LabTopicsDto(String orders, String throttle, String ackA, String ackB,
        String pending, String accepted) {
}
