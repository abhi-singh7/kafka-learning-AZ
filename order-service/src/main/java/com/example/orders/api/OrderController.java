package com.example.orders.api;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * Task 1.4 GREEN: REST in, Kafka out.
 * - Validates before touching Kafka (Task 1.1)
 * - Service owns the orderId; client-supplied value ignored (A3)
 * - One message per line item, key = orderId (partitioning)
 * - Single KafkaTemplate for the process lifetime (Broker lifecycle requirement)
 */
@RestController
class OrderController {

    private static final Logger log = LoggerFactory.getLogger(OrderController.class);
    static final String TOPIC = "order-created";

    /** Orders accepted during this run, oldest first. */
    private final List<AcceptedOrderDto> accepted = new CopyOnWriteArrayList<>();

    private final KafkaTemplate<String, OrderLineEvent> kafkaTemplate;

    OrderController(KafkaTemplate<String, OrderLineEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/orders")
    @ResponseStatus(HttpStatus.CREATED)
    public OrderAcceptedResponse createOrder(@RequestBody OrderRequestDto request) {
        if (request == null || request.customerId() == null) {
            throw new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY, "customerId is required");
        }

        List<OrderValidationError> errors = OrderValidator.validate(request.customerId(), request.items());
        if (!errors.isEmpty()) {
            throw new UnprocessableEntityException(errors);
        }

        // A3: the service owns the orderId; any client-supplied value is ignored.
        String orderId = UUID.randomUUID().toString();

        // P1-3: one message per line item, key = orderId so all lines of an
        // order land on the same partition.
        List<OrderLineEvent> events = OrderToLineEventsMapper.toLineEvents(request, orderId);
        for (OrderLineEvent event : events) {
            kafkaTemplate.send(TOPIC, orderId, event);
        }

        accepted.add(new AcceptedOrderDto(orderId, request.items().size()));
        log.info("accepted order {} with {} line(s), published to {}", orderId, events.size(), TOPIC);

        return OrderAcceptedResponse.of(orderId, request.items().size(), TOPIC);
    }

    @GetMapping("/orders")
    public List<AcceptedOrderDto> listOrders() {
        return List.copyOf(accepted);
    }

    @GetMapping("/health")
    public HealthDto health() {
        return new HealthDto("UP", accepted.size());
    }

    /** 422 body: {"errors":[{"field","reason"}]} */
    public record UnprocessableEntityBody(List<OrderValidationError> errors) {
    }

    static final class UnprocessableEntityException extends ResponseStatusException {
        private final List<OrderValidationError> errors;

        UnprocessableEntityException(List<OrderValidationError> errors) {
            super(HttpStatus.UNPROCESSABLE_ENTITY, "order validation failed");
            this.errors = errors;
        }

        public List<OrderValidationError> getErrors() {
            return errors;
        }
    }
}
