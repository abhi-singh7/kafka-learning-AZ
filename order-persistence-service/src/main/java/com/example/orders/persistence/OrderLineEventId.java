package com.example.orders.persistence;

import java.io.Serializable;

/** Composite primary key: (orderId, sku). */
public record OrderLineEventId(String orderId, String sku) implements Serializable {
}
