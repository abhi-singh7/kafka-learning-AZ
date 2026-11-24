package com.example.orders.api;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Task 1.3 GREEN: pure mapper from an accepted order to N line-event messages
 * (P1-3). No I/O — the caller decides where the events go.
 *
 * <p>{@code messageId} and {@code createdAt} are set here, by the producer,
 * per the contract; consumers only echo them back in ack/poison records.
 */
public final class OrderToLineEventsMapper {

    private OrderToLineEventsMapper() {
    }

    public static List<OrderLineEvent> toLineEvents(OrderRequestDto request, String orderId) {
        List<OrderLineEvent> events = new ArrayList<>();
        if (request == null || request.items() == null) {
            return events;
        }
        Instant createdAt = Instant.now();
        for (int i = 0; i < request.items().size(); i++) {
            OrderItemDto item = request.items().get(i);
            events.add(new OrderLineEvent(
                    OrderLineEvent.SCHEMA_VERSION,
                    orderId,
                    request.customerId(),
                    item.sku(),
                    item.qty(),
                    item.price(),
                    i,
                    UUID.randomUUID().toString(),
                    createdAt));
        }
        return events;
    }
}
