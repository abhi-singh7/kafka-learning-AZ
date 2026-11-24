package com.example.lab.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point for the consumer side of the Kafka learning pair (port 8092).
 * Boots the four listener lanes (main / race / txn / avro — see LabListeners),
 * the read-only REST surface (LabController) and the startup topology check.
 */
@SpringBootApplication
public class Main {
    public static void main(String[] args) {
        SpringApplication.run(Main.class, args);
    }
}
