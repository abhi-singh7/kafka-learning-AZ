package com.example.orders.api;

import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Task 1.1 RED: validation contract from specs/order-service.md — every invalid
 * shape must yield a 422-worthy field/reason pair, and valid input yields none.
 */
class OrderValidatorTest {

    private static OrderItemDto item(String sku, int qty, double price) {
        return new OrderItemDto(sku, qty, price);
    }

    @Test
    void validOrderHasNoErrors() {
        List<OrderValidationError> errors =
                OrderValidator.validate("acme", List.of(item("sku-1", 2, 9.99)));
        assertEquals(List.of(), errors);
    }

    @Test
    void emptyItemsIsRejected() {
        List<OrderValidationError> errors = OrderValidator.validate("acme", List.of());
        assertTrue(errors.stream().anyMatch(e -> e.field().equals("items")), "expected an 'items' error: " + errors);
    }

    @Test
    void nullItemsIsRejected() {
        List<OrderValidationError> errors = OrderValidator.validate("acme", null);
        assertTrue(errors.stream().anyMatch(e -> e.field().equals("items")));
    }

    @Test
    void zeroQuantityIsRejectedWithIndex() {
        List<OrderValidationError> errors =
                OrderValidator.validate("acme", List.of(item("sku-1", 0, 5.0)));
        assertTrue(errors.stream().anyMatch(e -> e.field().equals("items[0].qty")));
    }

    @Test
    void negativePriceIsRejectedWithIndex() {
        List<OrderValidationError> errors =
                OrderValidator.validate("acme", List.of(item("sku-1", 1, -0.5)));
        assertTrue(errors.stream().anyMatch(e -> e.field().equals("items[0].price")));
    }

    @Test
    void blankCustomerIdIsRejected() {
        List<OrderValidationError> errors = OrderValidator.validate("   ", List.of(item("sku-1", 1, 5.0)));
        assertTrue(errors.stream().anyMatch(e -> e.field().equals("customerId")));
    }

    @Test
    void oversizedCustomerIdIsRejected() {
        String longId = "x".repeat(201);
        List<OrderValidationError> errors = OrderValidator.validate(longId, List.of(item("sku-1", 1, 5.0)));
        assertTrue(errors.stream().anyMatch(e -> e.field().equals("customerId")));
    }

    @Test
    void blankSkuIsRejected() {
        List<OrderValidationError> errors =
                OrderValidator.validate("acme", List.of(item("", 1, 5.0)));
        assertTrue(errors.stream().anyMatch(e -> e.field().equals("items[0].sku")));
    }

    @Test
    void oversizedSkuIsRejected() {
        List<OrderValidationError> errors =
                OrderValidator.validate("acme", List.of(item("s".repeat(201), 1, 5.0)));
        assertTrue(errors.stream().anyMatch(e -> e.field().equals("items[0].sku")));
    }

    @Test
    void moreThanHundredItemsIsRejected() {
        List<OrderItemDto> items = java.util.stream.IntStream.range(0, 101)
                .mapToObj(i -> item("sku-" + i, 1, 1.0))
                .toList();
        List<OrderValidationError> errors = OrderValidator.validate("acme", items);
        assertTrue(errors.stream().anyMatch(e -> e.field().equals("items") && e.reason().contains("too many items")));
    }

    @Test
    void nullItemIsRejected() {
        List<OrderValidationError> errors = OrderValidator.validate("acme", java.util.Arrays.asList(new OrderItemDto[]{null}));
        assertTrue(errors.stream().anyMatch(e -> e.field().equals("items[0]")));
    }
}
