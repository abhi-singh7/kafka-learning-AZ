package com.example.lab.producer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

/**
 * All Kafka client beans for the push side. Rule of the whole app: every producer comes from
 * a Spring bean configured via properties — no hand-built org.apache.kafka clients in app code.
 *
 * <p>There are four lanes, each with its own ProducerFactory + KafkaTemplate pair:
 *
 * <ul>
 *   <li><b>order lane</b> ({@link #orderKafkaTemplate}) — the SHIP shape: async, keyed,
 *       JSON values onto lab-orders. This is the pattern you would copy for real code.</li>
 *   <li><b>raw lane</b> ({@link #rawKafkaTemplate}) — String→String; shared by two demos:
 *       the throttle back-pressure lane (tiny buffer.memory + short max.block.ms) and the
 *       transactional push (transaction-id prefix). Both are deliberately constrained on
 *       purpose so the experiment is observable.</li>
 *   <li><b>pending lane</b> ({@link #pendingKafkaTemplate}) — JSON values onto lab-pending,
 *       the input side of the Kafka Streams running-total pipeline.</li>
 *   <li><b>avro lane</b> ({@link #avroKafkaTemplate}) — Avro values serialized against the
 *       Confluent schema registry instead of plain JSON.</li>
 * </ul>
 *
 * <p>Plus {@link SyncPushLane}, a fifth, self-contained sync-ack demo producer.
 *
 * <p>Why one bean per exact type parameterization: Boot resolves beans BY TYPE, so there can
 * be at most one bean of each (K,V) pairing — that is why the lanes differ in value type
 * instead of overloading a single template.
 */
@Configuration
class LabBeans {

    private static final Logger log = LoggerFactory.getLogger(LabBeans.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.producer.properties[acks]:1}")
    private String asyncAcks;

    @Value("${spring.kafka.producer.properties[queue.size]:1000}")
    private String queueSize;

    @Value("${spring.kafka.producer.properties[max.attempts]:3}")
    private String maxAttempts;

    @Value("${spring.kafka.producer.properties[linger.ms]:50}")
    private String lingerMs;

    @Value("${spring.kafka.producer.properties[batch.size]:8192}")
    private String batchSize;

    @Value("${spring.kafka.producer.properties[max.blocking]:5000}")
    private String maxBlockingMs;

    @Value("${lab.kafka.schema-registry-url:http://localhost:8081}")
    private String schemaRegistryUrl;

    @Value("${lab.producer.lane:async}")
    private String lane;

    @Value("${lab.producer.lane.sync.acks:all}")
    private String syncAcks;

    @Value("${lab.producer.throttle.max.block.ms:200}")
    private String throttleMaxBlockMs;

    @Value("${lab.producer.throttle.buffer.bytes:65536}")
    private String throttleBufferBytes;

    @Value("${lab.producer.throttle.max.request.size:4096}")
    private String throttleMaxRequestSize;

    @Value("${lab.producer.throttle.linger.ms:50}")
    private String throttleLingerMs;

    // Lab-scoped key on purpose: setting spring.kafka.producer.transaction-id-prefix
    // would activate Boot's own auto kafkaTransactionManager bean (ProducerFactory<?,?>)
    // which cannot resolve against our typed factory beans.
    @Value("${lab.kafka.transaction-id-prefix:}")
    private String transactionIdPrefix;

    /**
     * SHIP-lane factory. The startup log names the active acknowledgment mode and its acks
     * value so a reader of the logs can tell which lane is live without opening properties.
     */
    @Bean
    public ProducerFactory<String, OrderLineEvent> orderProducerFactory() {
        String activeAcks = "sync".equals(lane) ? syncAcks : asyncAcks;
        log.info("producer acknowledgment lane active: {} (acks={})", lane, activeAcks);
        return new DefaultKafkaProducerFactory<>(typedProps(activeAcks));
    }

    /** SHIP-lane template: the async keyed lane used by POST /lab/orders. */
    @Bean
    public KafkaTemplate<String, OrderLineEvent> orderKafkaTemplate(
            ProducerFactory<String, OrderLineEvent> orderProducerFactory) {
        return new KafkaTemplate<>(orderProducerFactory);
    }

    /**
     * Raw String→String lane factory. Two demos share it, and both constraints are deliberate:
     * <ul>
     *   <li><b>throttle demo</b> — buffer.memory + max.block.ms are property-bound to tiny
     *       values so a burst of sends hits the producer's memory budget and send() is refused
     *       (KafkaException in the future) instead of growing memory without bound;
     *   <li><b>txn demo</b> — transactions require the idempotent producer, which kafka-clients
     *       only allows with acks=all; the transaction-id prefix makes executeInTransaction legal.</li>
     * </ul>
     */
    @Bean
    public ProducerFactory<String, String> rawProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);
        // Required for the transactional demo (idempotent producer ⇒ acks=all).
        props.put("acks", "all");
        // Flow control on kafka-clients 7.x: the producer buffers at most
        // buffer.memory bytes and blocks at most max.block.ms when it cannot
        // accept more work. Beyond that, send() FAILS (KafkaException goes into
        // the returned future) instead of growing memory without bound.
        props.put("buffer.memory", throttleBufferBytes);
        props.put("max.block.ms", throttleMaxBlockMs);
        // kafka-clients requires buffer.memory >= max.request.size, so both are
        // property-bound; a tiny pair of them creates the observable refusal.
        props.put("max.request.size", throttleMaxRequestSize);
        props.put("linger.ms", throttleLingerMs);
        DefaultKafkaProducerFactory<String, String> factory = new DefaultKafkaProducerFactory<>(props);
        if (!transactionIdPrefix.isEmpty()) {
            factory.setTransactionIdPrefix(transactionIdPrefix);
        }
        return factory;
    }

    /**
     * Raw-lane template. setAllowNonTransactional(true) is what lets the throttle lane use
     * this SAME bean for plain sends while the txn lane uses it transactionally.
     */
    @Bean
    public KafkaTemplate<String, String> rawKafkaTemplate(
            ProducerFactory<String, String> rawProducerFactory) {
        // The FACTORY (above) already carries the transaction-id prefix, which is what makes
        // executeInTransaction legal on this template.
        KafkaTemplate<String, String> template = new KafkaTemplate<>(rawProducerFactory);
        if (!transactionIdPrefix.isEmpty()) {
            template.setTransactionIdPrefix(transactionIdPrefix);
            // Allow plain (non-transactional) sends too so the throttle lane can share this bean.
            template.setAllowNonTransactional(true);
        }
        return template;
    }

    /**
     * Streams-lane push side: JSON values onto lab-pending, key = customerId. Distinct type
     * parameterization from the lanes above because Boot resolves beans by exact type.
     */
    @Bean
    public ProducerFactory<String, Object> pendingProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", JacksonJsonSerializer.class);
        props.put("acks", asyncAcks);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, Object> pendingKafkaTemplate(
            ProducerFactory<String, Object> pendingProducerFactory) {
        return new KafkaTemplate<>(pendingProducerFactory);
    }

    /**
     * AVRO lane: load the current OrderEvent schema once from src/main/avro (on the classpath
     * via the pom &lt;resources&gt; entry) so templates can build GenericData.Record values.
     */
    @Bean
    public Schema orderEventSchema() {
        try (InputStream in = new org.springframework.core.io.ClassPathResource(
                "com/example/lab/producer/OrderEvent.avsc").getInputStream()) {
            return new Schema.Parser().parse(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        } catch (IOException failure) {
            throw new IllegalStateException("cannot load OrderEvent.avsc", failure);
        }
    }

    // AVRO lane producer: value serializer = Confluent KafkaAvroSerializer, registry URL from
    // properties. The serializer is LAZY - it only talks to the registry on first send, so this
    // bean (and therefore the app context) starts fine even when no registry is reachable.
    @Bean
    public ProducerFactory<String, GenericData.Record> avroProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", KafkaAvroSerializer.class);
        props.put("schema.registry.url", schemaRegistryUrl);
        props.put("acks", asyncAcks);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, GenericData.Record> avroKafkaTemplate(
            ProducerFactory<String, GenericData.Record> avroProducerFactory) {
        return new KafkaTemplate<>(avroProducerFactory);
    }

    /**
     * The sync-ack demo lane (POST /lab/orders/sync). Always constructed alongside the async
     * lane; lab.producer.lane only changes which acks value is logged as "active".
     */
    @Bean
    public SyncPushLane orderSyncPush() {
        return new SyncPushLane(syncAcks, queueSize, maxAttempts, lingerMs, batchSize, maxBlockingMs, bootstrapServers);
    }

    /** Shared property map for the order lane: serializers + tuning knobs, all from properties. */
    private Map<String, Object> typedProps(String acks) {
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", JacksonJsonSerializer.class);
        props.put("acks", acks);
        props.put("queue.size", queueSize);
        props.put("max.attempts", maxAttempts);
        props.put("linger.ms", lingerMs);
        props.put("batch.size", batchSize);
        props.put("max.blocking", maxBlockingMs);
        return props;
    }
}
