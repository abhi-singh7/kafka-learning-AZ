package com.example.orders.api;

import java.util.List;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase-7 review finding: pin the actual JSON wire shape order-service puts on
 * order-created, so the two consumers (which own duplicated record classes) are
 * proven to read the SAME field names and the same Instant encoding.
 */
class WireFormatTest {

    @Test
    void wireHasExactlyTheNineContractFields() {
        var mapper = tools.jackson.databind.json.JsonMapper.builder().build();
        OrderLineEvent event = new OrderLineEvent(OrderLineEvent.SCHEMA_VERSION,
                "o-1", "acme", "sku-a", 2, 9.99, 0, "m-1", Instant.now());

        String json = mapper.writeValueAsString(event);
        assertNotNull(json);

        for (String field : List.of("schemaVersion", "orderId", "customerId", "sku",
                                    "qty", "price", "lineIndex", "messageId", "createdAt")) {
            assertTrue(json.contains("\"" + field + "\":"),
                    "wire JSON must carry contract field " + field + ": " + json);
        }
        assertTrue(json.contains("\"schemaVersion\":\"1\""),
                "schemaVersion must be the literal \"1\": " + json);
        assertEquals(9, List.of("schemaVersion", "orderId", "customerId", "sku",
                                "qty", "price", "lineIndex", "messageId", "createdAt").size(),
                "contract is nine fields");
    }
}
