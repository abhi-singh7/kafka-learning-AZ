package com.example.orders.email;

/**
 * Result of decoding one consumed payload: either an accepted record or a
 * rejection reason (json-decode | schema-mismatch). Never throws — the
 * listener decides what to do with a rejection (poison path).
 */
public record LineEventDecodeResult(OrderLineEventRecord record, String rejectionReason) {

    public boolean isAccepted() {
        return record != null;
    }

    public static LineEventDecodeResult accepted(OrderLineEventRecord record) {
        return new LineEventDecodeResult(record, null);
    }

    public static LineEventDecodeResult rejected(String reason) {
        return new LineEventDecodeResult(null, reason);
    }
}
