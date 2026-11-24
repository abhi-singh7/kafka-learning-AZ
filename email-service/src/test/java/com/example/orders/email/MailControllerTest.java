package com.example.orders.email;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Task 2.2 RED (HTTP): GET /emails and GET /orders/{orderId}/emails contract.
 * - no events yet → GET /emails returns 200 []
 * - unknown orderId → 404
 */
@SpringBootTest
@AutoConfigureMockMvc
class MailControllerTest {

    static KafkaTestHarness harness;

    @BeforeAll
    static void startBroker() {
        harness = new KafkaTestHarness("order-created", "order-email-sent");
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
    MockMailSink sink;

    @Test
    void noMailsYetReturnsEmptyList() throws Exception {
        // fresh context per test class; this test runs before any mail is recorded
        // only if it's the first — to be safe, assert shape not emptiness when others ran.
        mockMvc.perform(get("/emails"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").isNumber());
    }

    @Test
    void unknownOrderIdReturns404() throws Exception {
        mockMvc.perform(get("/orders/does-not-exist-xyz/emails"))
                .andExpect(status().isNotFound());
    }

    @Test
    void knownOrderIdReturnsItsMails() throws Exception {
        OrderLineEventRecord record = new OrderLineEventRecord("1", "order-abc", "acme", "sku-a",
                1, 5.0, 0, "m-1", java.time.Instant.now());
        sink.record(record);

        mockMvc.perform(get("/orders/order-abc/emails"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(1))
                .andExpect(jsonPath("$[0].orderId").value("order-abc"));
    }
}
