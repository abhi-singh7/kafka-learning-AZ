package com.example.lab.producer;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Lab topic names + partition counts from properties (kafka-contract B1: never
 * hardcoded in controllers). {@code NewTopic} beans create the lab-* topics on
 * Boot startup when they do not exist yet (Boot Kafka autoconfig creates them
 * via KafkaAdmin; it skips existing topics).
 */
@Configuration
class LabTopics {

    private static final Logger log = LoggerFactory.getLogger(LabTopics.class);

    @Value("${lab.topic.orders}")
    private String ordersTopic;

    @Value("${lab.topic.orders.partitions:3}")
    private Integer ordersPartitions;

    @Value("${lab.topic.throttle}")
    private String throttleTopic;

    @Value("${lab.topic.ack.a}")
    private String ackATopic;

    @Value("${lab.topic.ack.b}")
    private String ackBTopic;

    @Value("${lab.topic.pending}")
    private String pendingTopic;

    @Value("${lab.topic.accepted}")
    private String acceptedTopic;

    @Value("${lab.topic.avro}")
    private String avroTopic;

    @Value("${lab.topic.replication:1}")
    private Integer replication;

    @Value("${lab.diagnostics.extra.topics:}")
    private String extraTopicsCsv;

    @Bean
    public LabTopicsDto labTopicsConfig() {
        log.info("lab topic beans registered: {} {} {} {} {} {}",
                ordersTopic, throttleTopic, ackATopic, ackBTopic, pendingTopic, acceptedTopic);
        return new LabTopicsDto(ordersTopic, throttleTopic, ackATopic, ackBTopic, pendingTopic, acceptedTopic);
    }

    @Bean
    public NewTopic labOrdersTopic() {
        return new NewTopic(ordersTopic, ordersPartitions, replication.shortValue());
    }

    @Bean
    public NewTopic labThrottleTopic() {
        return new NewTopic(throttleTopic, 1, replication.shortValue());
    }

    @Bean
    public NewTopic labAckATopic() {
        return new NewTopic(ackATopic, 1, replication.shortValue());
    }

    @Bean
    public NewTopic labAckBTopic() {
        return new NewTopic(ackBTopic, 1, replication.shortValue());
    }

    @Bean
    public NewTopic labPendingTopic() {
        return new NewTopic(pendingTopic, 1, replication.shortValue());
    }

    @Bean
    public NewTopic labAcceptedTopic() {
        return new NewTopic(acceptedTopic, 1, replication.shortValue());
    }

    @Bean
    public NewTopic labAvroTopic() {
        return new NewTopic(avroTopic, 1, replication.shortValue());
    }

    @Bean
    public LabExtraTopicsDto labDiagnosticsExtraTopics() {
        return new LabExtraTopicsDto(splitCsv(extraTopicsCsv));
    }

    private List<String> splitCsv(String csv) {
        List<String> names = new java.util.ArrayList<>();
        for (String name : csv.trim().split(",")) {
            String trimmed = name.trim();
            if (!trimmed.isEmpty()) {
                names.add(trimmed);
            }
        }
        return names;
    }
}
