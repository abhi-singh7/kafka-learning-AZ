package com.example.lab.producer;

import java.util.List;

/**
 * Extra topic names diagnostics should also report (e.g. the sibling order-*
 * topics) so the namespace contrast is visible in one call.
 */
public record LabExtraTopicsDto(List<String> topics) {
}
