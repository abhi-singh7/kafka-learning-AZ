package com.example.orders.email;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

/**
 * Single KafkaTemplate per concern. Producer: ack to order-email-sent (typed JSON)
 * and poison to order-failed (string JSON). Consumer: order-created, manual ACK so
 * NACK = rethrow → redelivery.
 */
@Configuration
@EnableKafka
class EmailKafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ProducerFactory<String, EmailSentAck> ackProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", JacksonJsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, EmailSentAck> ackKafkaTemplate(
            ProducerFactory<String, EmailSentAck> ackProducerFactory) {
        return new KafkaTemplate<>(ackProducerFactory);
    }

    @Bean
    public ProducerFactory<String, String> poisonProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> poisonKafkaTemplate(
            ProducerFactory<String, String> poisonProducerFactory) {
        return new KafkaTemplate<>(poisonProducerFactory);
    }

    @Bean
    public DefaultKafkaConsumerFactory<String, String> lineEventConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.deserializer", StringDeserializer.class);
        props.put("value.deserializer", StringDeserializer.class);
        props.put("group.id", "email-service");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "false");
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            DefaultKafkaConsumerFactory<String, String> lineEventConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(lineEventConsumerFactory);
        factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);
        return factory;
    }

    @Bean
    public AckPublisher ackPublisher(KafkaTemplate<String, EmailSentAck> ackKafkaTemplate) {
        return event -> ackKafkaTemplate.send("order-email-sent", event.orderId(), EmailSentAck.of(event));
    }

    @Bean
    public PoisonStore poisonStore() {
        return new PoisonStore();
    }

    @Bean
    public PoisonPublisher poisonPublisher(KafkaTemplate<String, String> poisonKafkaTemplate,
            tools.jackson.databind.json.JsonMapper mapper, PoisonStore poisonStore) {
        return (sourceTopic, rawPayload, reason) -> {
            Map<String, Object> poison = new HashMap<>();
            poison.put("detectedBy", "email-service");
            poison.put("sourceTopic", sourceTopic);
            poison.put("reason", reason);
            poison.put("rawPayload", rawPayload);
            poison.put("receivedAt", java.time.Instant.now().toString());
            String json = mapper.writeValueAsString(poison);
            poisonKafkaTemplate.send("order-failed", null, json);
            poisonStore.record("email-service", sourceTopic, reason, rawPayload);
        };
    }
}
