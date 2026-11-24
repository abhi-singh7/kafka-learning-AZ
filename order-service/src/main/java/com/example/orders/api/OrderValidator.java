package com.example.orders.api;

import java.util.ArrayList;
import java.util.List;

/**
 * Task 1.1 GREEN: pure validation contract from specs/order-service.md.
 * No I/O, no Spring — a static function so it is trivially unit-testable and
 * reusable by the REST controller (422 mapping) and future producers.
 */
public final class OrderValidator {

    private static final int MAX_CUSTOMER_ID_LENGTH = 200;
    private static final int MAX_SKU_LENGTH = 200;
    private static final int MAX_ITEMS_PER_ORDER = 100;

    private OrderValidator() {
    }

    /**
     * Validates an order payload. Returns an empty list when the order is valid,
     * otherwise one {@link OrderValidationError} per violated rule (all rules are
     * checked, not just the first failure).
     */
    public static List<OrderValidationError> validate(String customerId, List<OrderItemDto> items) {
        List<OrderValidationError> errors = new ArrayList<>();

        if (customerId == null || customerId.isBlank()) {
            errors.add(new OrderValidationError("customerId", "must not be blank"));
        } else if (customerId.length() > MAX_CUSTOMER_ID_LENGTH) {
            errors.add(new OrderValidationError("customerId",
                    "must be at most " + MAX_CUSTOMER_ID_LENGTH + " characters"));
        }

        if (items == null) {
            errors.add(new OrderValidationError("items", "must not be null"));
            return errors;
        }
        if (items.isEmpty()) {
            errors.add(new OrderValidationError("items", "must contain at least one item"));
            return errors;
        }
        if (items.size() > MAX_ITEMS_PER_ORDER) {
            errors.add(new OrderValidationError("items",
                    "too many items: at most " + MAX_ITEMS_PER_ORDER + " allowed"));
            return errors;
        }

        for (int i = 0; i < items.size(); i++) {
            OrderItemDto item = items.get(i);
            if (item == null) {
                errors.add(new OrderValidationError("items[" + i + "]", "must not be null"));
                continue;
            }
            if (item.sku() == null || item.sku().isBlank()) {
                errors.add(new OrderValidationError("items[" + i + "].sku", "must not be blank"));
            } else if (item.sku().length() > MAX_SKU_LENGTH) {
                errors.add(new OrderValidationError("items[" + i + "].sku",
                        "must be at most " + MAX_SKU_LENGTH + " characters"));
            }
            if (item.qty() == null || item.qty() <= 0) {
                errors.add(new OrderValidationError("items[" + i + "].qty", "must be greater than zero"));
            }
            if (item.price() == null || item.price() < 0) {
                errors.add(new OrderValidationError("items[" + i + "].price", "must not be negative"));
            }
        }

        return errors;
    }
}
