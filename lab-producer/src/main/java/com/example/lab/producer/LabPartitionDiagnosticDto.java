package com.example.lab.producer;

/**
 * Per-partition entry for diagnostics: last known producer offset for that
 * partition from the push results of this process (null = nothing pushed yet).
 */
public record LabPartitionDiagnosticDto(Integer partition, Long nextOffset) {
}
