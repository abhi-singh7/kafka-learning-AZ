package com.example.orders.api;

import java.util.List;

/** One accepted order as listed by GET /orders. */
public record AcceptedOrderDto(String orderId, int lineCount) {
}
