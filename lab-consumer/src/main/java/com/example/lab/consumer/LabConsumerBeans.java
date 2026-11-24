package com.example.lab.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.listener.AfterRollbackProcessor;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;

import org.springframework.kafka.transaction.ChainedKafkaTransactionManager;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.util.backoff.ExponentialBackOff;



/**
 * All consumer-side Kafka beans. Rule of the app: every client comes from a Spring bean
 * configured via properties — no raw org.apache.kafka construction in app code.
 *
 * <p>There are four lanes, each with its own consumer group (and therefore its own committed
 * offsets — groups read independently):
 *
 * <ul>
 *   <li><b>main lane</b> — group {@code lab.group.main} on the orders topic. The one that
 *       does real work: decode → dedupe via the ledger → retry with backoff → quarantine.</li>
 *   <li><b>race lane</b> — TWO listeners sharing ONE group ({@code lab.group.competitor}) on
 *       the rebalance topic; together they process each message exactly once in total.
 *       Stopping one (POST /lab/listeners/competitor/off) triggers a rebalance.</li>
 *   <li><b>txn lane</b> — group {@code lab.group.txn} on the orders topic again, but with
 *       transactional side effects (ack-a + ack-b pushed atomically).</li>
 *   <li><b>avro lane</b> — group {@code lab.group.avro} on the avro topic, values decoded by
 *       the Confluent Avro deserializer against the schema registry.</li>
 * </ul>
 *
 * <p>Bean map: consumerFactory (String→String, shared by main/race/txn) →
 * kafkaListenerContainerFactory (main+race, named so @KafkaListener picks it by default);
 * txnListenerContainerFactory adds the rollback processor; avroConsumerFactory +
 * avroListenerContainerFactory carry the Avro deserializer. The side-effect producer
 * (quarantine writes + transactional pushes) is a separate ProducerFactory/KafkaTemplate pair.
 */
@Configuration
class LabConsumerBeans {

    private static final Logger log = LoggerFactory.getLogger(LabConsumerBeans.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${lab.consumer.startup:process}")
    private String startupMode;

    @Value("${lab.ack.strategy:after-processed}")
    private String ackStrategy;

    @Value("${lab.ack.batch-size:5}")
    private int batchSize;

    @Value("${lab.retry.max-attempts:3}")
    private int maxAttempts;

    @Value("${lab.retry.backoff.base.ms:100}")
    private long backoffBaseMs;

    @Value("${lab.rollback.mode:retry-next-poll}")
    private String rollbackMode;

    // Lab-scoped key on purpose: spring.kafka.producer.transaction-id-prefix would
    // activate Boot's own auto kafkaTransactionManager, which cannot resolve against
    // our typed factory beans (same decision as the producer side).
    @Value("${lab.kafka.transaction-id-prefix:}")
    private String transactionIdPrefix;

    @Value("${lab.topic.rejected:lab-rejected}")
    private String rejectedTopic;

    @Value("${lab.kafka.schema-registry-url:http://localhost:8081}")
    private String schemaRegistryUrl;

    /**
     * Shared String→String consumer factory for main/race/txn lanes. The startup option is
     * the only knob that decides where a FRESH group starts: process-backlog → earliest
     * (replay everything), skip.records → latest (only new messages). Groups that already
     * have committed offsets always resume from there regardless.
     */
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        // The startup option maps to Kafka's auto.offset.reset for groups without commits yet.
        String autoOffsetReset = "skip".equals(startupMode) ? "latest" : "earliest";
        log.info("startup backlog option: {} (auto.offset.reset={})", startupMode, autoOffsetReset);
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.deserializer", StringDeserializer.class);
        props.put("value.deserializer", StringDeserializer.class);
        props.put("auto.offset.reset", autoOffsetReset);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * AVRO lane factory: value deserializer = Confluent KafkaAvroDeserializer against the
     * schema registry. Earliest reset so a cold read sees the first record. The deserializer
     * is lazy — it only talks to the registry when decoding a record, so the app starts fine
     * even when no registry is reachable.
     */
    @Bean
    public ConsumerFactory<String, GenericData.Record> avroConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.deserializer", StringDeserializer.class);
        props.put("value.deserializer", KafkaAvroDeserializer.class);
        props.put("schema.registry.url", schemaRegistryUrl);
        props.put("auto.offset.reset", "earliest");
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean(name = "avroListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, GenericData.Record> avroListenerContainerFactory(
            ConsumerFactory<String, GenericData.Record> avroConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, GenericData.Record> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(avroConsumerFactory);
        return factory;
    }

    @Bean
    public LabAvroTracker labAvroTracker() {
        return new LabAvroTracker();
    }

    /**
     * Main + race lanes. Named {@code kafkaListenerContainerFactory} so plain
     * {@code @KafkaListener} methods pick it up by default (no explicit containerFactory).
     *
     * <p>Three things are wired here, each a dial the lab turns via properties:
     * <ul>
     *   <li><b>ack strategy</b> — immediate (commit BEFORE processing: loss window),
     *       batch (commit every N records), or after-processed (default: commit once the
     *       handler returns cleanly);</li>
     *   <li><b>header filter</b> — records with x-lab-skip=true are dropped BEFORE payload
     *       deserialization and ACKed on purpose (they never reach a handler);</li>
     *   <li><b>error handler</b> — bounded retry with exponential backoff, then quarantine.</li>
     * </ul>
     */
    @Bean(name = "kafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory,
            RecordFilterStrategy<String, String> skipFilter,
            CommonErrorHandler commonErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        ContainerProperties cp = factory.getContainerProperties();
        // The acknowledgment dial: WHEN does the group offset advance past a record?
        switch (ackStrategy) {
            case "immediate" -> {
                cp.setAckMode(ContainerProperties.AckMode.MANUAL); // listener acks BEFORE processing
                log.info("ack strategy active: immediate (commit before processing — loss window = one in-flight record)");
            }
            case "batch" -> {
                cp.setAckMode(ContainerProperties.AckMode.COUNT);
                cp.setAckCount(batchSize);
                log.info("ack strategy active: batch (checkpoint every {} records)", batchSize);
            }
            default -> {
                cp.setAckMode(ContainerProperties.AckMode.MANUAL); // listener acks AFTER processing
                log.info("ack strategy active: after-processed (commit after each record is processed)");
            }
        }
        // Header filter runs BEFORE payload deserialization; filtered records are ACKed
        // on purpose (ackDiscarded) so the offset advances past them without a handler run.
        factory.setRecordFilterStrategy(skipFilter);
        factory.setAckDiscarded(true);
        // Bounded retry with exponential backoff, then quarantine to lab-rejected.
        factory.setCommonErrorHandler(commonErrorHandler);
        return factory;
    }

    /**
     * Txn lane factory (group lab-txn). Differs from the main factory in two ways:
     * <ul>
     *   <li>ack mode RECORD — the listener has no Acknowledgment parameter, and a record
     *       that was rolled back must STILL let the group offset advance past it;</li>
     *   <li>{@code AfterRollbackProcessor} — decides what happens to the record after a
     *       transaction rollback: leave it for a later poll, or discard it.</li>
     * </ul>
     */
    @Bean(name = "txnListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> txnListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory,
            CommonErrorHandler commonErrorHandler,
            AfterRollbackProcessor<String, String> afterRollbackProcessor) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        // Auto-commit per record: the listener has no Acknowledgment, and a discarded
        // (rolled-back) record must still let the group offset advance past it.
        ContainerProperties cp = factory.getContainerProperties();
        cp.setAckMode(ContainerProperties.AckMode.RECORD);
        factory.setCommonErrorHandler(commonErrorHandler);
        // Task 2.8 GREEN: rollback processor bean (retry-next-poll vs discard).
        factory.setAfterRollbackProcessor(afterRollbackProcessor);
        return factory;
    }

    /** Skip decision from the x-lab-skip header — evaluated before any payload decode. */
    @Bean
    public RecordFilterStrategy<String, String> skipFilter() {
        return record -> "true".equalsIgnoreCase(QuarantineWriter.headerValue(record, "x-lab-skip"));
    }

    /**
     * Retry policy for the main/race lanes: up to max-attempts with exponential backoff
     * (base × 2 each retry); when the budget is exhausted the recoverer quarantines the
     * record (reason "handler-throw") and commits past it — no infinite loop.
     */
    @Bean
    public CommonErrorHandler commonErrorHandler(QuarantineWriter quarantineWriter) {
        ExponentialBackOff backOff = new ExponentialBackOff();
        backOff.setInitialInterval(backoffBaseMs);
        backOff.setMultiplier(2.0);
        backOff.setMaxAttempts(maxAttempts);
        ConsumerRecordRecoverer recoverer = (record, ex) ->
                quarantineWriter.quarantine(record, "handler-throw", null);
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        handler.setCommitRecovered(true);
        handler.setAckAfterHandle(true);
        log.info("retry budget: max-attempts={} backoff-base={}ms factor=2 (then quarantine to lab-rejected)",
                maxAttempts, backoffBaseMs);
        return handler;
    }

    /**
     * Post-rollback decision for the txn lane:
     * <ul>
     *   <li>retry-next-poll — leave the offset where it is; a later poll reprocesses the
     *       record (a transient failure may succeed next time);</li>
     *   <li>discard — skip the record with a log line naming its offset.</li>
     * </ul>
     */
    @Bean
    public AfterRollbackProcessor<String, String> afterRollbackProcessor() {
        log.info("rollback mode active: {}", rollbackMode);
        return (records, consumer, container, ex, isBatch, eosMode) -> {
            for (var record : records) {
                if ("discard".equals(rollbackMode)) {
                    log.info("discarding {}@{}:{} after rollback (mode=discard)",
                            record.topic(), record.partition(), record.offset());
                } else {
                    log.info("reprocessing {}@{}:{} on a later poll (mode=retry-next-poll)",
                            record.topic(), record.partition(), record.offset());
                }
            }
        };
    }

    /**
     * Side-effect producer factory: shared by the quarantine writes (plain sends) and the
     * transactional ack-a/ack-b pushes of the txn lane. acks=all is required for transactions.
     */
    @Bean
    public ProducerFactory<String, String> sideEffectProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);
        // Transactions require the idempotent producer (acks=all).
        props.put("acks", "all");
        DefaultKafkaProducerFactory<String, String> factory = new DefaultKafkaProducerFactory<>(props);
        if (!transactionIdPrefix.isEmpty()) {
            factory.setTransactionIdPrefix(transactionIdPrefix);
        }
        return factory;
    }

    @Bean(destroyMethod = "destroy")
    public KafkaTemplate<String, String> sideEffectKafkaTemplate(ProducerFactory<String, String> sideEffectProducerFactory) {
        KafkaTemplate<String, String> template = new KafkaTemplate<>(sideEffectProducerFactory);
        if (!transactionIdPrefix.isEmpty()) {
            template.setTransactionIdPrefix(transactionIdPrefix);
            // Allow plain (non-transactional) sends too: the quarantine lane shares this bean.
            template.setAllowNonTransactional(true);
        }
        return template;
    }

    @Bean
    public KafkaTransactionManager labTxnTransactionManager(ProducerFactory<String, String> sideEffectProducerFactory) {
        return new KafkaTransactionManager(sideEffectProducerFactory);
    }

    /**
     * Chained transaction manager (allows nesting a transaction inside another) — marked
     * {@code @Primary} so it wins wherever a KafkaTransactionManager is autowired by type.
     */
    @Bean
    @Primary
    public ChainedKafkaTransactionManager<String, String> chainedTxnTransactionManager(
            KafkaTransactionManager labTxnTransactionManager) {
        return new ChainedKafkaTransactionManager<>(labTxnTransactionManager);
    }

    /**
     * Tags every listener-observation meter with the lab lane. spring-kafka 4.x's convention
     * hook for this is not reachable in this build, so an explicit MeterFilter guarantees
     * the tag appears on the published meters (visible via GET /lab/metrics).
     */
    @Bean
    public io.micrometer.core.instrument.config.MeterFilter labLaneTagFilter() {
        return io.micrometer.core.instrument.config.MeterFilter.forMeters(
                id -> id.getName().startsWith("spring.kafka.listener"),
                io.micrometer.core.instrument.config.MeterFilter.commonTags(
                        java.util.List.of(io.micrometer.core.instrument.Tag.of("lab.lane", "orders"))));
    }

    /** Read-only broker view (Task 2.2); also performs the startup topology check. */
    @Bean
    public LabKafkaAdmin labKafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put("bootstrap.servers", bootstrapServers);
        return new LabKafkaAdmin(configs);
    }

    @Bean
    public FailureInjector failureInjector() {
        return new FailureInjector();
    }

    @Bean
    public QuarantineWriter quarantineWriter(KafkaTemplate<String, String> sideEffectKafkaTemplate) {
        return new QuarantineWriter(sideEffectKafkaTemplate, new tools.jackson.databind.ObjectMapper(), rejectedTopic);
    }

    @Bean
    public LabLedger labLedger() {
        return new LabLedger();
    }

    @Bean
    public LabQueue labQueue() {
        return new LabQueue();
    }
}
