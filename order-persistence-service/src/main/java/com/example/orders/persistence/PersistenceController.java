package com.example.orders.persistence;

import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * Task 3.4 GREEN: query surface for persisted order lines.
 * - GET /orders/{orderId}/lines → rows for that order, 404 when none
 * - GET /orders/lines → all rows (collection route), [] when none
 * - GET /health → DB reachability + row count
 * - GET /orders/failed → poison records observed by this service
 */
@RestController
class PersistenceController {

    private final OrderLineEventRepository repository;
    private final PoisonStore poisonStore;
    private final BrokerReachability brokerReachability;

    PersistenceController(OrderLineEventRepository repository, PoisonStore poisonStore,
                          BrokerReachability brokerReachability) {
        this.repository = repository;
        this.poisonStore = poisonStore;
        this.brokerReachability = brokerReachability;
    }

    @GetMapping("/orders/{orderId}/lines")
    public List<OrderLineEvent> linesForOrder(@PathVariable String orderId) {
        List<OrderLineEvent> rows = repository.findByOrderId(orderId);
        if (rows.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "no lines for order " + orderId);
        }
        return rows;
    }

    @GetMapping("/orders/lines")
    public List<OrderLineEvent> allLines() {
        return repository.findAll();
    }

    @GetMapping("/orders/failed")
    public List<PoisonStore.PoisonRecord> failedOrders() {
        return poisonStore.all();
    }

    @GetMapping("/health")
    public HealthDto health() {
        String database;
        long rows;
        try {
            rows = repository.count();
            database = "reachable";
        } catch (Exception e) {
            rows = -1;
            database = "unreachable";
        }
        String broker = brokerReachability.status();
        boolean up = "reachable".equals(database) && "reachable".equals(broker);
        return new HealthDto(up ? "UP" : "DEGRADED", broker, database, rows);
    }

    public record HealthDto(String status, String broker, String database, long rows) {
    }
}
