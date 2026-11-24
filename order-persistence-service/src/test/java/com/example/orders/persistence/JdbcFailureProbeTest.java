package com.example.orders.persistence;

import java.util.List;
import java.math.BigDecimal;
import java.time.Instant;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/** Scratch: what does an over-sized JDBC value throw, and is a partial row written? */
@SpringBootTest
class JdbcFailureProbeTest {

    static KafkaTestHarness harness;

    @BeforeAll
    static void startBroker() {
        harness = new KafkaTestHarness("order-created", "order-persisted", "order-failed");
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

    @Autowired
    OrderLineEventRepository repository;

    @Test
    void probe() throws Exception {
        String huge = "z".repeat(400);
        OrderLineEvent entity = new OrderLineEvent("order-jdbc-probe", "acme", huge, 1,
                BigDecimal.ONE, 0, "msg-jdbc-probe", Instant.now());
        String thrown = "NO-THROW";
        try {
            repository.save(entity);
        } catch (Exception e) {
            thrown = "THREW: " + e.getClass();
        }
        List<OrderLineEvent> rows = repository.findByOrderId("order-jdbc-probe");
        org.junit.jupiter.api.Assertions.fail(thrown + " ROWS: " + rows.size());
    }
}
