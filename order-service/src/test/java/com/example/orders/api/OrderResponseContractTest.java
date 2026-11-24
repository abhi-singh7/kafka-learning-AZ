package com.example.orders.api;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Task 1.5 RED: 201 response contract + graceful shutdown.
 * - 201 body is exactly {orderId, lineCount, kafkaKey, topic:"order-created"}
 * - GET /health returns {status, ordersAccepted}
 * - A shutdown hook bean exists and can destroy the KafkaTemplate cleanly
 */
@SpringBootTest
@AutoConfigureMockMvc
class OrderResponseContractTest {

    static KafkaTestHarness harness;

    @BeforeAll
    static void startBroker() {
        harness = new KafkaTestHarness("order-created");
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
    KafkaTemplate<String, OrderLineEvent> kafkaTemplate;

    @Test
    void createdResponseHasExactContractShape() throws Exception {
        String body = """
                {"customerId":"acme","items":[{"sku":"a","qty":1,"price":1.0}]}
                """;

        mockMvc.perform(post("/orders").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.orderId").isString())
                .andExpect(jsonPath("$.lineCount").value(1))
                .andExpect(jsonPath("$.kafkaKey").isString())
                .andExpect(jsonPath("$.topic").value("order-created"));
    }

    @Test
    void kafkaKeyEqualsOrderId() throws Exception {
        String body = """
                {"customerId":"acme","items":[{"sku":"a","qty":1,"price":1.0}]}
                """;

        String response = mockMvc.perform(post("/orders").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();

        var json = com.jayway.jsonpath.JsonPath.parse(response);
        String orderId = json.read("$.orderId", String.class);
        String kafkaKey = json.read("$.kafkaKey", String.class);
        org.junit.jupiter.api.Assertions.assertEquals(orderId, kafkaKey);
    }

    @Test
    void healthEndpointReportsStatusAndAcceptedCount() throws Exception {
        mockMvc.perform(get("/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("UP"))
                .andExpect(jsonPath("$.ordersAccepted").isNumber());
    }

    @Test
    void shutdownHookDestroysKafkaTemplateCleanly() {
        // The template must be destroyable (DisposableBean contract) without throwing.
        assertNotNull(kafkaTemplate);
        kafkaTemplate.destroy(); // must not throw
    }
}
