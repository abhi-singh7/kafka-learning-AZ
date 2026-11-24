package com.example.orders.persistence;

import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Task 3.3 RED: persist exactly once per messageId (replay → no extra row);
 * three records of one order → three rows. Full context + embedded broker +
 * real Postgres (ordersdb). Asserts on row counts (the ack contract is covered
 * in Task 3.4) so the test is not sensitive to consumer offset timing.
 */
@SpringBootTest
class PersistenceIdempotencyTest {

    static KafkaTestHarness harness;

    @Autowired
    OrderLineEventRepository repository;

    @BeforeAll
    static void startBroker() {
        harness = new KafkaTestHarness("order-created", "order-persisted");
    }

    @AfterAll
    static void stopBroker() {
        if (harness != null) {
            harness.close();
        }
    }

    @DynamicPropertySource
    static void kafkaProps(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", () -> harness.bootstrapServers());
    }

    @Test
    void replayDoesNotCreateExtraRowAndThreeLinesGiveThreeRows() throws Exception {
        Thread.sleep(1500); // let the listener subscribe

        String orderId = "order-idem-1";
        repository.findByOrderId(orderId).forEach(repository::delete); // clean slate

        String payloadA = lineEvent(orderId, "sku-a", 1, "msg-a");
        harness.produce("order-created", orderId, payloadA);
        // replay the SAME messageId → must not create a second row
        harness.produce("order-created", orderId, payloadA);

        awaitRows(orderId, 1);
        long countA = repository.findByOrderId(orderId).stream()
                .filter(e -> "msg-a".equals(e.getMessageId())).count();
        assertEquals(1, countA, "replay must not create a second row for the same messageId");

        // three distinct lines of one order → three rows
        harness.produce("order-created", orderId, lineEvent(orderId, "sku-b", 2, "msg-b"));
        harness.produce("order-created", orderId, lineEvent(orderId, "sku-c", 3, "msg-c"));

        awaitRows(orderId, 3);
        assertEquals(3, repository.findByOrderId(orderId).size(),
                "three line events of one order must yield three rows");
    }

    private void awaitRows(String orderId, int expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            if (repository.findByOrderId(orderId).size() >= expected) {
                return;
            }
            Thread.sleep(250);
        }
    }

    private String lineEvent(String orderId, String sku, int qty, String messageId) {
        return """
                {"schemaVersion":"1","orderId":"%s","customerId":"acme","sku":"%s",
                 "qty":%d,"price":9.99,"lineIndex":0,"messageId":"%s","createdAt":"2025-01-01T00:00:00Z"}
                """.formatted(orderId, sku, qty, messageId);
    }
}
