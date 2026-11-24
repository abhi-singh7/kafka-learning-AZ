package com.example.lab.producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point for the Kafka learning pair - push side (port 8091).
 *
 * The whole point of this app is that every Kafka client comes from Spring Boot Kafka beans and
 * {@code spring.kafka.*} properties - no hand-built org.apache.kafka producer anywhere in app code.
 */
@SpringBootApplication
public class Main {
    public static void main(String[] args) {
        SpringApplication.run(Main.class, args);
    }
}
