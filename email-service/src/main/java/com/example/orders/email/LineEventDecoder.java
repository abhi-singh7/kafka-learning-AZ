package com.example.orders.email;

import tools.jackson.databind.json.JsonMapper;

/**
 * Task 2.1 GREEN: Jackson 3 decode of a consumed line-event payload.
 * Returns a rejection (never throws) for undecodable JSON or a foreign
 * schemaVersion, so the listener can route it to the poison path.
 */
public final class LineEventDecoder {

    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    private LineEventDecoder() {
    }

    public static LineEventDecodeResult decode(String payload) {
        if (payload == null || payload.isBlank()) {
            return LineEventDecodeResult.rejected("json-decode");
        }
        OrderLineEventRecord record;
        try {
            record = MAPPER.readValue(payload, OrderLineEventRecord.class);
        } catch (Exception e) {
            return LineEventDecodeResult.rejected("json-decode");
        }
        if (record == null || isMissingCriticalField(record)) {
            return LineEventDecodeResult.rejected("json-decode");
        }
        if (!"1".equals(record.schemaVersion())) {
            return LineEventDecodeResult.rejected("schema-mismatch");
        }
        return LineEventDecodeResult.accepted(record);
    }

    private static boolean isMissingCriticalField(OrderLineEventRecord r) {
        return r.orderId() == null || r.customerId() == null || r.sku() == null
                || r.qty() == null || r.price() == null || r.lineIndex() == null
                || r.messageId() == null || r.createdAt() == null;
    }
}
