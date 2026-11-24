package com.example.orders.api;

/** GET /health shape: status + number of orders accepted this run. */
public record HealthDto(String status, int ordersAccepted) {
}
