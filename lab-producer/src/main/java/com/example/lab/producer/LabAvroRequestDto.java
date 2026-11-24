package com.example.lab.producer;

/**
 * Request body for the AVRO lane (POST /lab/avro). {@code orderId} may be omitted
 * (a UUID is generated); {@code discount} and {@code phoneNumber} are optional and, when
 * absent, the OrderEvent schema's defaults apply - which is exactly what makes old (v1)
 * data readable by a reader on the latest (v2) schema.
 */
public record LabAvroRequestDto(String orderId, String customer, Double amount,
        Double discount, String phoneNumber) {
}
