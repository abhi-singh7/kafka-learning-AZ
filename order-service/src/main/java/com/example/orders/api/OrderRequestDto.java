package com.example.orders.api;

import java.util.List;

/**
 * POST /orders request body. A client-supplied {@code orderId} is accepted on
 * the wire (so old clients don't get 400s) but MUST be ignored — the service
 * generates its own UUID (spec: "Accept an order over REST", decision A3).
 */
public record OrderRequestDto(
        String customerId,
        List<OrderItemDto> items,
        @jakarta.annotation.Nullable String orderId // read, then discarded
) {
}
