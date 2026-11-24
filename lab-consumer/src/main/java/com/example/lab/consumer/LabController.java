package com.example.lab.consumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * REST surface of the consumer side (lab-consumer, port 8092). Everything here is a
 * READ-ONLY view of broker state plus two lab control switches — no endpoint writes to
 * any topic. Read top-to-bottom as a tour:
 *
 * <ul>
 *   <li>{@code GET /lab/diagnostics} — the big one: every topic in the cluster, the
 *       group's committed offsets, and each partition's end offset ("next offset").</li>
 *   <li>{@code GET /lab/backlog} — per-partition lag = end offset − committed offset,
 *       plus the in-process queue gauge.</li>
 *   <li>{@code GET /lab/rejected} — everything currently sitting on the quarantine topic.</li>
 *   <li>{@code GET /lab/metrics} — Micrometer meters for the listener containers only.</li>
 *   <li>{@code GET /lab/avro} — OrderEvents decoded by the AVRO lane, in receive order.</li>
 *   <li>{@code GET /lab/health} — broker reachability + queue/ledger gauges.</li>
 *   <li>{@code POST /lab/inject-failures} — lab switch: make N% of processing attempts throw,
 *       so you can watch retry → backoff → quarantine happen.</li>
 *   <li>{@code POST /lab/listeners/competitor/off} — lab switch: stop one of the two
 *       race-lane listeners and let its group-mate pick up the work (rebalance demo).</li>
 * </ul>
 */
@RestController
class LabController {

    private static final Logger log = LoggerFactory.getLogger(LabController.class);
    private static final tools.jackson.databind.ObjectMapper READER = new tools.jackson.databind.ObjectMapper();

    private final LabKafkaAdmin admin;
    private final KafkaTemplate<String, String> template;
    private final ConsumerFactory<String, String> consumerFactory;
    private final LabLedger ledger;
    private final LabQueue queue;
    private final FailureInjector failureInjector;
    private final LabAvroTracker avroTracker;
    private final MeterRegistry meterRegistry;
    private final KafkaListenerEndpointRegistry listenerRegistry;
    private final String ordersTopic;
    private final String rebalanceTopic;
    private final String rejectedTopic;

    LabController(LabKafkaAdmin admin,
                  KafkaTemplate<String, String> sideEffectKafkaTemplate,
                  ConsumerFactory<String, String> consumerFactory,
                  LabLedger ledger,
                  LabQueue queue,
                  FailureInjector failureInjector,
                  LabAvroTracker avroTracker,
                  MeterRegistry meterRegistry,
                  KafkaListenerEndpointRegistry listenerRegistry,
                  @Value("${lab.topic.orders:lab-orders}") String ordersTopic,
                  @Value("${lab.topic.rebalance:lab-rebalance}") String rebalanceTopic,
                  @Value("${lab.topic.rejected:lab-rejected}") String rejectedTopic) {
        this.admin = admin;
        this.template = sideEffectKafkaTemplate;
        this.consumerFactory = consumerFactory;
        this.ledger = ledger;
        this.queue = queue;
        this.failureInjector = failureInjector;
        this.avroTracker = avroTracker;
        this.meterRegistry = meterRegistry;
        this.listenerRegistry = listenerRegistry;
        this.ordersTopic = ordersTopic;
        this.rebalanceTopic = rebalanceTopic;
        this.rejectedTopic = rejectedTopic;
    }

    record DiagnosticsDto(List<String> topics,
                          Map<String, Long> groupOffsets,
                          Map<String, Long> nextOffsets,
                          int queueSize,
                          int ledgerCount,
                          boolean hasConsumerOffsetsTopic,
                          boolean hasSchemasTopic) {
    }

    /**
     * The "where is my data" view: topics that exist, where the lab-consumer group has
     * committed, and where each partition ends. Keys are {@code topic@partition} because a
     * bare partition number is ambiguous across topics (both have a partition 0).
     */
    @GetMapping("/lab/diagnostics")
    public DiagnosticsDto diagnostics() {
        Set<String> topics = admin.topicNames();
        Map<TopicPartition, Long> next = admin.nextOffsets(List.of(ordersTopic, rebalanceTopic));
        Map<TopicPartition, Long> group = admin.groupOffsets("lab-consumer");
        // Key by topic@partition: two topics can share the same numeric partition (e.g. both have
        // a partition 0), so a bare partition number would let one topic clobber the other's offset.
        Map<String, Long> nextByPartition = new TreeMap<>();
        next.forEach((tp, offset) -> nextByPartition.put(topicPartitionKey(tp), offset));
        Map<String, Long> groupByPartition = new TreeMap<>();
        group.forEach((tp, offset) -> groupByPartition.put(topicPartitionKey(tp), offset));
        return new DiagnosticsDto(new ArrayList<>(topics), groupByPartition, nextByPartition,
                queue.inFlight(), ledger.count(),
                topics.contains("__consumer_offsets"), topics.contains("_schemas"));
    }

    /** Disambiguates partitions across topics: "lab-orders@0" vs "lab-rebalance@0". */
    private static String topicPartitionKey(TopicPartition tp) {
        return tp.topic() + "@" + tp.partition();
    }

    record BacklogDto(int queueSize, Map<String, Long> lag) {
    }

    /**
     * Lag per partition of the orders topic: end offset minus committed offset (end offset
     * itself when the group has never committed). queueSize is the IN-PROCESS gauge — records
     * received but not yet finished, which is a different question than broker-side lag.
     */
    @GetMapping("/lab/backlog")
    public BacklogDto backlog() {
        Map<TopicPartition, Long> next = admin.nextOffsets(List.of(ordersTopic));
        Map<TopicPartition, Long> group = admin.groupOffsets("lab-consumer");
        Map<String, Long> lag = new TreeMap<>();
        next.forEach((tp, end) -> {
            Long committed = group.get(tp);
            lag.put(String.valueOf(tp.partition()), committed == null ? end : Math.max(0, end - committed));
        });
        return new BacklogDto(queue.inFlight(), lag);
    }

    record RejectedDto(List<Map<String, Object>> records) {
    }

    /**
     * Read the quarantine topic from the beginning and return every rejected record with its
     * reason/attempts metadata. Uses a short-lived consumer (assign + seekToBeginning + poll)
     * rather than template.receive, which could only ever return the single offset-0 record.
     */
    @GetMapping("/lab/rejected")
    public RejectedDto rejected() {
        List<Map<String, Object>> out = new ArrayList<>();
        // Read every record currently on the quarantine topic from the beginning. The previous
        // template.receive(topic, 0, 0, timeout) could only ever return the single offset-0
        // record (and returned nothing when a fresh transient consumer missed its poll window).
        try (Consumer<String, String> reader = consumerFactory.createConsumer()) {
            Set<TopicPartition> partitions = reader.partitionsFor(rejectedTopic).stream()
                    .map(info -> new TopicPartition(rejectedTopic, info.partition()))
                    .collect(java.util.stream.Collectors.toSet());
            if (partitions.isEmpty()) {
                return new RejectedDto(out);
            }
            reader.assign(partitions);
            reader.seekToBeginning(partitions);
            long deadline = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < deadline) {
                var polled = reader.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : polled) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> body = READER.readValue(record.value(), Map.class);
                    out.add(body);
                }
                if (polled.isEmpty()) {
                    break; // drained from beginning to end
                }
            }
        } catch (Exception e) {
            log.warn("rejected read failed: {}", e.getMessage());
        }
        return new RejectedDto(out);
    }

    record InjectFailuresRequest(int percentage, String lane) {
    }

    record InjectFailuresResponse(int percentage, String lane) {
    }

    /** Lab switch: set the injected failure percentage (0–100%) so retries are observable. */
    @PostMapping("/lab/inject-failures")
    public InjectFailuresResponse injectFailures(@RequestBody InjectFailuresRequest request) {
        failureInjector.inject(request.percentage(), request.lane());
        log.info("failure injection active: {}% on lane {}", request.percentage(), request.lane());
        return new InjectFailuresResponse(request.percentage(), request.lane());
    }

    record CompetitorOffResponse(boolean stopped, int pending) {
    }

    /**
     * Task 2.10: stop the competitor listener (lab-race-b). The survivor
     * (lab-race-a, same group) re-reads from the group offsets and processes
     * whatever the competitor had not ACKed.
     */
    @PostMapping("/lab/listeners/competitor/off")
    public CompetitorOffResponse competitorOff() {
        var containers = listenerRegistry.getListenerContainersMatching(id -> id.equals("lab-race-b"));
        int stopped = 0;
        for (var container : containers) {
            container.stop();
            stopped++;
        }
        log.info("competitor listener stopped (containers={}); pending in-process={} — "
                + "with a multi-broker cluster the survivor would also re-fetch its assignment from the "
                + "controller after the rebalance (concept-only on this single-broker box)",
                stopped, queue.inFlight());
        return new CompetitorOffResponse(stopped > 0, queue.inFlight());
    }

    record MetricsDto(List<Map<String, Object>> meters) {
    }

    /** Micrometer meters for the listener containers only (messaging/kafka names). */
    @GetMapping("/lab/metrics")
    public MetricsDto metrics() {
        List<Map<String, Object>> out = new ArrayList<>();
        for (Meter meter : meterRegistry.getMeters()) {
            String id = meter.getId().toString();
            if (id.contains("messaging") || id.contains("kafka")) {
                Map<String, Object> entry = new TreeMap<>();
                entry.put("name", meter.getId().getName());
                entry.put("tags", meter.getId().getTags().stream()
                        .map(t -> t.getKey() + "=" + t.getValue()).toList());
                out.add(entry);
            }
        }
        return new MetricsDto(out);
    }

    record AvroDto(List<LabAvroTracker.AvroEntry> events) {
    }

    /** AVRO lane (Task 3.2): OrderEvents decoded from lab-avro, in receive order. */
    @GetMapping("/lab/avro")
    public AvroDto avro() {
        return new AvroDto(avroTracker.snapshot());
    }

    record HealthDto(boolean brokerReachable, int queueSize, int ledgerCount) {
    }

    /** Broker reachable + how much work is in flight / already applied. */
    @GetMapping("/lab/health")
    public HealthDto health() {
        return new HealthDto(admin.brokerReachable(), queue.inFlight(), ledger.count());
    }
}
