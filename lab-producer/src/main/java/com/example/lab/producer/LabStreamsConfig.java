package com.example.lab.producer;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;

/**
 * The Kafka Streams lane, hosted inside lab-producer. Pushes land on
 * {@code lab-pending}; this topology turns them into per-customer running totals on
 * {@code lab-accepted}, which GET /lab/accepted reads back.
 *
 * <p>Topology: {@code lab-pending} in, a STATELESS transform (extract the amount),
 * then a STATEFUL per-customer running total, out to {@code lab-accepted}:
 *
 * <pre>
 *   lab-pending ──(map: PendingOrder -> amount)──(process: running total)──&gt; lab-accepted
 *                                              └── KeyValueStore "lab-running-totals"
 * </pre>
 *
 * <p>The running total is a custom {@link Processor} with a persistent KeyValueStore.
 * It reads the previous total, folds in the new amount, writes the store and forwards
 * IMMEDIATELY via {@code context.forward(...)} — that is what makes every intermediate
 * total (10, 30, 60) visible on {@code lab-accepted} in push order. (The built-in
 * reduce operators in Kafka Streams 4.x route output through the state-store cache and
 * only emit the latest value per key at commit time, which would hide the intermediates.)
 *
 * <p>Wiring is Boot-managed: {@code @EnableKafkaStreams} + this {@link Topology} bean;
 * broker, application-id and extra Streams properties come from
 * {@code spring.kafka.streams.*} in application.properties (kafka-contract: no raw
 * Kafka client construction here).
 */
@Configuration
@EnableKafkaStreams
class LabStreamsConfig {

    static final String RUNNING_TOTAL_STORE = "lab-running-totals";

    @Value("${lab.topic.pending}")
    private String pendingTopic;

    @Value("${lab.topic.accepted}")
    private String acceptedTopic;

    @Bean
    Topology labStreamsTopology(StreamsBuilder builder) {
        // Persistent (RocksDB) key-value store: the running total per customer survives
        // task restarts; its changelog topic is lab-producer-lab-running-totals-changelog.
        // Must be registered on the builder BEFORE process() names it (4.x validates
        // the connection at build time).
        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(RUNNING_TOTAL_STORE),
                Serdes.String(), Serdes.Double()));

        // Input: JSON values (Boot's Jackson serializer on the push lane), key = customerId.
        // The serde builds its OWN deserializer, so the trusted package must be set
        // here (spring.json.trusted.packages only reaches Boot-managed clients).
        JacksonJsonSerde<PendingOrder> pendingSerde = new JacksonJsonSerde<>();
        pendingSerde.deserializer().addTrustedPackages("com.example.lab.producer");
        KStream<String, PendingOrder> pending = builder.stream(pendingTopic,
                Consumed.with(Serdes.String(), pendingSerde));

        // Stateless transform: keep only the amount; the key stays customerId.
        KStream<String, Double> amounts = pending.map(
                (customerId, order) -> org.apache.kafka.streams.KeyValue.pair(customerId, order.amount()));

        // Stateful per-customer running total (store registered above).
        KStream<String, AcceptedTotal> totals =
                amounts.process(new RunningTotalProcessorSupplier(), RUNNING_TOTAL_STORE);

        // Output: JSON values to lab-accepted (no deserialization happens here).
        totals.to(acceptedTopic, Produced.with(Serdes.String(), new JacksonJsonSerde<>()));

        return builder.build();
    }

    /**
     * Folds each amount into the per-customer running total and forwards EVERY result —
     * 10, then 30, then 60 for amounts 10/20/30 — so push order is observable on
     * {@code lab-accepted}.
     */
    static final class RunningTotalProcessorSupplier
            implements ProcessorSupplier<String, Double, String, AcceptedTotal> {

        @Override
        public Processor<String, Double, String, AcceptedTotal> get() {
            return new Processor<>() {
                private ProcessorContext<String, AcceptedTotal> context;
                private KeyValueStore<String, Double> store;

                @Override
                public void init(ProcessorContext<String, AcceptedTotal> context) {
                    this.context = context;
                    this.store = context.getStateStore(RUNNING_TOTAL_STORE);
                }

                @Override
                public void process(Record<String, Double> record) {
                    String customerId = record.key();
                    Double previous = store.get(customerId);
                    double total = (previous == null ? 0.0 : previous) + record.value();
                    store.put(customerId, total);
                    // Immediate forward: no cache-flush gate, so every intermediate total
                    // reaches lab-accepted in push order.
                    context.forward(record.withValue(new AcceptedTotal(customerId, total)));
                }
            };
        }
    }
}
