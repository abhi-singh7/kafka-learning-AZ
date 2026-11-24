package com.example.orders.api;

import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Task 1.3 RED: split one order → N line-event messages (P1-3).
 * Pure mapper, no I/O. Fields per specs/kafka-contract.md (schemaVersion 1):
 * schemaVersion, orderId, customerId, sku, qty, price, lineIndex, messageId, createdAt.
 */
class OrderToLineEventsMapperTest {

    @Test
    void twoItemsProduceTwoMessagesWithContiguousLineIndexes() {
        OrderRequestDto request = new OrderRequestDto("acme", List.of(
                new OrderItemDto("sku-a", 1, 10.0),
                new OrderItemDto("sku-b", 2, 5.5)), null);

        List<OrderLineEvent> events = OrderToLineEventsMapper.toLineEvents(request, "order-1");

        assertEquals(2, events.size());
        assertEquals(0, events.get(0).lineIndex());
        assertEquals(1, events.get(1).lineIndex());
    }

    @Test
    void everyEventCarriesAllNineContractFields() {
        OrderRequestDto request = new OrderRequestDto("acme",
                List.of(new OrderItemDto("sku-a", 1, 10.0)), null);

        OrderLineEvent event = OrderToLineEventsMapper.toLineEvents(request, "order-1").get(0);

        assertEquals("1", event.schemaVersion());
        assertEquals("order-1", event.orderId());
        assertEquals("acme", event.customerId());
        assertEquals("sku-a", event.sku());
        assertEquals(1, event.qty());
        assertEquals(10.0, event.price());
        assertNotNull(event.messageId(), "messageId must be set by the producer");
        assertNotNull(event.createdAt(), "createdAt must be set by the producer");
    }

    @Test
    void messageIdsAreUniquePerLine() {
        OrderRequestDto request = new OrderRequestDto("acme", List.of(
                new OrderItemDto("a", 1, 1.0),
                new OrderItemDto("b", 2, 2.0)), null);

        List<OrderLineEvent> events = OrderToLineEventsMapper.toLineEvents(request, "order-1");
        assertEquals(2, events.stream().map(OrderLineEvent::messageId).distinct().count());
    }

    @Test
    void emptyItemsProduceNoMessages() {
        OrderRequestDto request = new OrderRequestDto("acme", List.of(), null);
        assertTrue(OrderToLineEventsMapper.toLineEvents(request, "order-1").isEmpty());
    }
}
