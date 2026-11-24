package com.example.lab.producer;

import java.util.List;

/**
 * One entry of GET /lab/diagnostics: a lab topic (or a sibling order-* topic
 * when requested), whether it exists, and its partitions with last-known
 * producer offsets (null offset when nothing pushed yet).
 */
public record LabTopicDiagnosticDto(String topic, Boolean exists, Integer partitions,
        List<LabPartitionDiagnosticDto> offsets) {
}
