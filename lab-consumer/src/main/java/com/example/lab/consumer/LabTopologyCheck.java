package com.example.lab.consumer;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;

/**
 * Startup gate: fail fast if any required topic is missing (the error names it plus the
 * create command), then log which topics this app reads vs writes so a reader of the logs
 * can see the topology before the first message arrives.
 */
@Component
class LabTopologyCheck {

    private static final Logger log = LoggerFactory.getLogger(LabTopologyCheck.class);

    private final LabKafkaAdmin admin;
    private final List<String> requiredTopics;

    LabTopologyCheck(LabKafkaAdmin admin,
                     @Value("${lab.topic.orders:lab-orders}") String orders,
                     @Value("${lab.topic.rebalance:lab-rebalance}") String rebalance,
                     @Value("${lab.topic.rejected:lab-rejected}") String rejected,
                     @Value("${lab.topic.ack.a:lab-ack-a}") String ackA,
                     @Value("${lab.topic.ack.b:lab-ack-b}") String ackB) {
        this.admin = admin;
        this.requiredTopics = List.of(orders, rebalance, rejected, ackA, ackB);
    }

    @PostConstruct
    void verify() {
        admin.verifyRequiredTopics(requiredTopics);
        log.info("lab-consumer reads {} and {} (groups lab-consumer/lab-competitor/lab-txn); writes {}",
                requiredTopics.get(0), requiredTopics.get(1), List.of(requiredTopics.get(2), requiredTopics.get(3), requiredTopics.get(4)));
    }
}
