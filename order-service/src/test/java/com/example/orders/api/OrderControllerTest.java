package com.example.orders.api;

import java.util.List;
import java.util.UUID;
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

import static org.hamcrest.Matchers.containsString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Task 1.2 RED: orderId ownership (A3) + accepted-order bookkeeping.
 * - POST /orders without orderId → response carries a service-generated UUID
 * - POST /orders WITH a client-supplied orderId → the generated one is returned,
 *   client value ignored
 * - GET /orders lists accepted orders (orderId + lineCount); empty until then
 */
@SpringBootTest
@AutoConfigureMockMvc
class OrderControllerTest {

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

    @Test
    void postOrderWithoutOrderIdReturnsGeneratedUuid() throws Exception {
        String body = """
                {"customerId":"acme","items":[{"sku":"sku-1","qty":2,"price":9.99}]}
                """;

        String response = mockMvc.perform(post("/orders").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.orderId").isNotEmpty())
                .andReturn().getResponse().getContentAsString();

        // extract the generated id and prove it is a UUID
        String orderId = com.jayway.jsonpath.JsonPath.parse(response).read("$.orderId", String.class);
        UUID.fromString(orderId); // throws if not a valid UUID
    }

    @Test
    void postOrderWithClientOrderIdIgnoresClientValue() throws Exception {
        String clientId = "client-supplied-id";
        String body = """
                {"customerId":"acme","orderId":"%s","items":[{"sku":"sku-1","qty":1,"price":5.0}]}
                """.formatted(clientId);

        String response = mockMvc.perform(post("/orders").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();

        String orderId = com.jayway.jsonpath.JsonPath.parse(response).read("$.orderId", String.class);
        org.junit.jupiter.api.Assertions.assertNotEquals(clientId, orderId, "client-supplied orderId must be ignored");
        UUID.fromString(orderId); // service-generated UUID
    }

    @Test
    void getOrdersListsAcceptedOrders() throws Exception {
        String body = """
                {"customerId":"acme","items":[{"sku":"a","qty":1,"price":1.0},{"sku":"b","qty":2,"price":2.0}]}
                """;

        mockMvc.perform(post("/orders").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isCreated());

        // GET /orders must contain the accepted order with its lineCount
        String response = mockMvc.perform(get("/orders"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        org.junit.jupiter.api.Assertions.assertTrue(response.contains("lineCount"), "GET /orders should list lineCount: " + response);
    }

    @Test
    void invalidOrderReturns422WithErrorShape() throws Exception {
        String body = """
                {"customerId":"acme","items":[]}
                """;

        mockMvc.perform(post("/orders").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.errors[0].field").value("items"));
    }
}
