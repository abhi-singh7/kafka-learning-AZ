package com.example.orders.api;

/**
 * One line item of an order. Immutable value object.
 */
// Jackson 3 (Boot 4): use Integer/Double wrappers so a missing field deserializes
// to null instead of failing with MismatchedInputException on primitives.
public record OrderItemDto(String sku, Integer qty, Double price) {
}
