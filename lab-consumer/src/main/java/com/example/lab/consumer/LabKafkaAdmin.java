package com.example.lab.consumer;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * Read-only broker view: everything GET /lab/diagnostics, /lab/backlog and the startup
 * topology check need — topic names, end offsets, committed group offsets, consumer groups,
 * cluster reachability. No method here ever writes to a topic.
 *
 * <p>It subclasses Boot's own {@code KafkaAdmin} and reaches the admin client through its
 * protected {@code createAdmin()} factory — the sanctioned route; no raw
 * {@code org.apache.kafka.clients.admin.Admin} construction in app code.
 */
public class LabKafkaAdmin extends KafkaAdmin {

    public LabKafkaAdmin(Map<String, Object> configs) {
        super(configs);
    }

    /** All topic names currently in the cluster. */
    public Set<String> topicNames() {
        try (Admin admin = createAdmin()) {
            return admin.listTopics().names().get(10, java.util.concurrent.TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("topic listing failed", e);
        }
    }

    /** Partition end offset ("next offset") per partition of the given topics. */
    public Map<TopicPartition, Long> nextOffsets(Collection<String> topics) {
        Map<String, TopicDescription> described = describeTopics(topics.toArray(new String[0]));
        Map<TopicPartition, OffsetSpec> spec = new HashMap<>();
        for (TopicDescription description : described.values()) {
            for (TopicPartitionInfo info : description.partitions()) {
                spec.put(new TopicPartition(description.name(), info.partition()), OffsetSpec.latest());
            }
        }
        if (spec.isEmpty()) {
            return Map.of();
        }
        try (Admin admin = createAdmin()) {
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> infos =
                    admin.listOffsets(spec).all().get(10, java.util.concurrent.TimeUnit.SECONDS);
            Map<String, TopicPartition> topicPartitions = new TreeMap<>();
            infos.keySet().forEach(tp -> topicPartitions.put(tp.toString(), tp));
            Map<TopicPartition, Long> out = new java.util.LinkedHashMap<>();
            topicPartitions.forEach((name, tp) -> out.put(tp, infos.get(tp).offset()));
            return out;
        } catch (Exception e) {
            throw new IllegalStateException("listOffsets failed", e);
        }
    }

    /** Committed group offsets per partition; partitions without a committed offset are absent. */
    /**
     * Where the given consumer group has committed per partition; partitions the group
     * has never committed are simply absent from the map.
     */
    public Map<TopicPartition, Long> groupOffsets(String group) {
        try (Admin admin = createAdmin()) {
            ListConsumerGroupOffsetsResult result = admin.listConsumerGroupOffsets(group);
            Map<TopicPartition, OffsetAndMetadata> offsets =
                    result.partitionsToOffsetAndMetadata().get(10, java.util.concurrent.TimeUnit.SECONDS);
            Map<TopicPartition, Long> out = new HashMap<>();
            offsets.forEach((tp, om) -> out.put(tp, om.offset()));
            return out;
        } catch (Exception e) {
            throw new IllegalStateException("group offset query failed for " + group, e);
        }
    }

    /** All consumer group ids currently known to the broker. */
    public Set<String> consumerGroups() {
        try (Admin admin = createAdmin()) {
            return admin.listConsumerGroups().all().get(10, java.util.concurrent.TimeUnit.SECONDS)
                    .stream().map(ConsumerGroupListing::groupId).collect(java.util.stream.Collectors.toSet());
        } catch (Exception e) {
            throw new IllegalStateException("consumer group listing failed", e);
        }
    }

    /**
     * Startup gate: refuse to start when a required topic is missing. The error names each
     * missing topic plus the exact kafka-topics.sh command that would create it, so the
     * failure tells you how to fix it.
     */
    public void verifyRequiredTopics(List<String> required) {
        Set<String> existing = topicNames();
        List<String> missing = required.stream().filter(t -> !existing.contains(t)).toList();
        if (!missing.isEmpty()) {
            throw new IllegalStateException("missing required topic(s) " + missing + " — create with: "
                    + missing.stream().map(t -> "kafka-topics.sh --create --if-not-exists --bootstrap-server 127.0.0.1:9092 --topic " + t
                            + " --partitions 1 --replication-factor 1").collect(java.util.stream.Collectors.joining(" ; ")));
        }
    }

    /** True when the broker answers a cluster-id query within 5s (health endpoint). */
    public boolean brokerReachable() {
        try (Admin admin = createAdmin()) {
            String id = admin.describeCluster().clusterId().get(5, java.util.concurrent.TimeUnit.SECONDS);
            return id != null;
        } catch (Exception e) {
            return false;
        }
    }
}
