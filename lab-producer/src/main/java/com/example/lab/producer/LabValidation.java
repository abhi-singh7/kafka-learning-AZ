package com.example.lab.producer;

import java.util.ArrayList;
import java.util.List;

/**
 * Pure validation of an order payload: returns every violated rule as a
 * {@link ValidationFailure} (field + reason), or an empty list when valid.
 * No I/O, no Spring — a static function so it is trivially unit-testable and
 * reusable by the REST controller (422 mapping) and future producers.
 */
public final class LabValidation {

    private static final int MAX_CUSTOMER_ID_LENGTH = 200;
    private static final int MAX_SKU_LENGTH = 200;
    private static final int MAX_ITEMS_PER_ORDER = 100;
    private static final int MAX_QTY = 1_000;

    private LabValidation() {
    }

    /**
     * Validates an order payload. Returns an empty list when the order is valid,
     * otherwise one {@link ValidationFailure} per violated rule (all rules are
     * checked, not just the first failure).
     */
    public static List<ValidationFailure> validate(String customerId, List<LineDto> items) {
        List<ValidationFailure> errors = new ArrayList<>();

        if (customerId == null || customerId.isBlank()) {
            errors.add(new ValidationFailure("customerId", "must not be blank"));
        } else if (customerId.length() > MAX_CUSTOMER_ID_LENGTH) {
            errors.add(new ValidationFailure("customerId",
                    "must be at most " + MAX_CUSTOMER_ID_LENGTH + " characters"));
        }

        if (items == null) {
            errors.add(new ValidationFailure("items", "must not be null"));
            return errors;
        }
        if (items.isEmpty()) {
            errors.add(new ValidationFailure("items", "must contain at least one item"));
            return errors;
        }
        if (items.size() > MAX_ITEMS_PER_ORDER) {
            errors.add(new ValidationFailure("items",
                    "too many items: at most " + MAX_ITEMS_PER_ORDER + " allowed"));
            return errors;
        }

        for (int i = 0; i < items.size(); i++) {
            LineDto item = items.get(i);
            if (item == null) {
                errors.add(new ValidationFailure("items[" + i + "]", "must not be null"));
                continue;
            }
            if (item.sku() == null || item.sku().isBlank()) {
                errors.add(new ValidationFailure("items[" + i + "].sku", "must not be blank"));
            } else if (item.sku().length() > MAX_SKU_LENGTH) {
                errors.add(new ValidationFailure("items[" + i + "].sku",
                        "must be at most " + MAX_SKU_LENGTH + " characters"));
            }
            if (item.qty() == null || item.qty() <= 0) {
                errors.add(new ValidationFailure("items[" + i + "].qty", "must be greater than zero"));
            }
            if (item.price() == null || item.price() < 0) {
                errors.add(new ValidationFailure("items[" + i + "].price", "must not be negative"));
            }
        }

        return errors;
    }
}
