package com.example.lab.producer;

import java.util.List;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Task 1.1/1.2 GREEN (validation contract): every rejected shape answers 422
 * with the {"errors":[{"field","reason"}]} body, and NOTHING was produced for
 * a rejected payload. Covers all rules from specs/order-service.md:
 * blank customerId, empty items, blank sku, qty<=0, missing qty, price<0,
 * malformed JSON.
 */
@SpringBootTest
@AutoConfigureMockMvc
class LabValidationTest {

    static LabKafkaTestHarness harness;

    @BeforeAll
    static void startBroker() {
        harness = new LabKafkaTestHarness(3, "lab-orders", "lab-throttle", "lab-ack-a",
                "lab-ack-b", "lab-pending", "lab-accepted", "order-created", "order-email-sent");
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
        registry.add("spring.kafka.streams.state-dir", () -> harness.stateDir());
    }

    @Autowired
    MockMvc mockMvc;

    private static final String VALID_BODY =
            "{\"customerId\":\"acme\",\"items\":[{\"sku\":\"a\",\"qty\":1,\"price\":1.0}]}";

    @Test
    void rejectedZeroQty() throws Exception {
        String body = "{\"customerId\":\"acme\",\"items\":[{\"sku\":\"a\",\"qty\":0,\"price\":1.0}]}";
        mockMvc.perform(post("/lab/orders").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().is(422))
                .andExpect(jsonPath("$.errors[0].field").value("items[0].qty"));
    }

    @Test
    void rejectedBlankSku() throws Exception {
        String body = "{\"customerId\":\"acme\",\"items\":[{\"sku\":\"\",\"qty\":1,\"price\":1.0}]}";
        mockMvc.perform(post("/lab/orders").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().is(422))
                .andExpect(jsonPath("$.errors[0].field").value("items[0].sku"));
    }

    @Test
    void rejectedMissingQty() throws Exception {
        String body = "{\"customerId\":\"acme\",\"items\":[{\"sku\":\"a\",\"price\":1.0}]}";
        mockMvc.perform(post("/lab/orders").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().is(422))
                .andExpect(jsonPath("$.errors[0].field").value("items[0].qty"));
    }

    @Test
    void rejectedNegativePrice() throws Exception {
        String body = "{\"customerId\":\"acme\",\"items\":[{\"sku\":\"a\",\"qty\":1,\"price\":-0.5}]}";
        mockMvc.perform(post("/lab/orders").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().is(422))
                .andExpect(jsonPath("$.errors[0].field").value("items[0].price"));
    }

    @Test
    void rejectedBlankCustomer() throws Exception {
        String body = "{\"customerId\":\"\",\"items\":[{\"sku\":\"a\",\"qty\":1,\"price\":1.0}]}";
        mockMvc.perform(post("/lab/orders").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().is(422))
                .andExpect(jsonPath("$.errors[0].field").value("customerId"));
    }

    @Test
    void rejectedEmptyItems() throws Exception {
        String body = "{\"customerId\":\"acme\",\"items\":[]}";
        mockMvc.perform(post("/lab/orders").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().is(422))
                .andExpect(jsonPath("$.errors[0].field").value("items"));
    }

    @Test
    void malformedJsonIs422Not500() throws Exception {
        mockMvc.perform(post("/lab/orders").contentType(MediaType.APPLICATION_JSON).content("{oops"))
                .andExpect(status().is(422))
                .andExpect(jsonPath("$.errors[0].field").value("body"));
    }

    @Test
    void nothingProducedForRejectedPayloads() throws Exception {
        // the broker must hold ZERO records on lab-orders after all the rejections above
        assertEquals(0, harness.consumeUpTo("lab-orders", 1, 1500).size(),
                "no records may reach lab-orders for rejected payloads");
    }
}
