package com.example.orders.email;

import java.time.Instant;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Task 2.1 RED: decode + validate consumed line events (email-service's OWN
 * record class — deliberate duplication). Decodable payload → record.
 * Undecodable / foreign schemaVersion → rejection, NOT an exception.
 */
class LineEventDecoderTest {

    @Test
    void decodesWellFormedPayload() {
        String json = """
                {"schemaVersion":"1","orderId":"o-1","customerId":"acme","sku":"sku-a",
                 "qty":2,"price":9.99,"lineIndex":0,"messageId":"m-1","createdAt":"2025-01-01T00:00:00Z"}
                """;

        LineEventDecodeResult result = LineEventDecoder.decode(json);

        assertTrue(result.isAccepted());
        OrderLineEventRecord record = result.record();
        assertEquals("1", record.schemaVersion());
        assertEquals("o-1", record.orderId());
        assertEquals("acme", record.customerId());
        assertEquals("sku-a", record.sku());
        assertEquals(2, record.qty());
        assertEquals(9.99, record.price());
        assertEquals(0, record.lineIndex());
        assertEquals("m-1", record.messageId());
        assertNotNull(record.createdAt());
    }

    @Test
    void decodesTheExactShapeTheProducerPutsOnTheWire() {
        // The order-service JacksonJsonSerializer emits Instant with nanosecond
        // precision and no space; this is what actually arrives on order-created.
        String wire = """
                {"schemaVersion":"1","orderId":"6f0f0b1e-1111-4111-8888-888888888888","customerId":"acme","sku":"A-100","qty":2,"price":19.9,"lineIndex":1,"messageId":"m-2","createdAt":"2026-11-10T20:04:25.843396654Z"}
                """;

        LineEventDecodeResult result = LineEventDecoder.decode(wire);

        assertTrue(result.isAccepted(), "the producer's real wire shape must decode");
        OrderLineEventRecord record = result.record();
        assertEquals(1, record.lineIndex());
        assertEquals(19.9, record.price());
        assertNotNull(record.createdAt());
    }

    @Test
    void undecodablePayloadIsRejectedNotThrown() {
        LineEventDecodeResult result = LineEventDecoder.decode("this is not json {{{");

        assertFalse(result.isAccepted());
        assertEquals("json-decode", result.rejectionReason());
    }

    @Test
    void foreignSchemaVersionIsRejected() {
        String json = """
                {"schemaVersion":"2","orderId":"o-1","customerId":"acme","sku":"sku-a",
                 "qty":1,"price":1.0,"lineIndex":0,"messageId":"m-1","createdAt":"2025-01-01T00:00:00Z"}
                """;

        LineEventDecodeResult result = LineEventDecoder.decode(json);

        assertFalse(result.isAccepted());
        assertEquals("schema-mismatch", result.rejectionReason());
    }

    @Test
    void missingRequiredFieldIsRejected() {
        String json = """
                {"schemaVersion":"1","orderId":"o-1","customerId":"acme","sku":"sku-a",
                 "qty":1,"price":1.0,"lineIndex":0,"messageId":"m-1"}
                """;

        LineEventDecodeResult result = LineEventDecoder.decode(json);

        assertFalse(result.isAccepted());
        assertEquals("json-decode", result.rejectionReason());
    }
}
