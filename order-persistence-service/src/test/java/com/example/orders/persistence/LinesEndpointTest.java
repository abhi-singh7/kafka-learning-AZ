package com.example.orders.persistence;

import java.math.BigDecimal;
import java.time.Instant;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Task 3.4 RED (HTTP): query surface for persisted lines.
 * - GET /orders/{orderId}/lines → rows, 404 when none
 * - GET /orders/lines → collection route, [] (200) when none
 * - GET /health → DB reachable
 */
@SpringBootTest
@AutoConfigureMockMvc
class LinesEndpointTest {

    static KafkaTestHarness harness;

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

    @Autowired
    MockMvc mockMvc;

    @Autowired
    OrderLineEventRepository repository;

    @Test
    void knownOrderIdReturnsItsLines() throws Exception {
        String orderId = "order-lines-1";
        repository.findByOrderId(orderId).forEach(repository::delete);
        repository.save(new OrderLineEvent(orderId, "acme", "sku-a", 1,
                BigDecimal.TEN, 0, "m-l-1", Instant.now()));

        mockMvc.perform(get("/orders/" + orderId + "/lines"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(1))
                .andExpect(jsonPath("$[0].orderId").value(orderId))
                .andExpect(jsonPath("$[0].sku").value("sku-a"));
    }

    @Test
    void unknownOrderIdReturns404() throws Exception {
        mockMvc.perform(get("/orders/does-not-exist-xyz/lines"))
                .andExpect(status().isNotFound());
    }

    @Test
    void collectionRouteReturnsArray() throws Exception {
        mockMvc.perform(get("/orders/lines"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").isNumber());
    }

    @Test
    void healthReportsBrokerAndDatabaseReachable() throws Exception {
        mockMvc.perform(get("/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("UP"))
                .andExpect(jsonPath("$.broker").value("reachable"))
                .andExpect(jsonPath("$.database").value("reachable"));
    }
}
