package com.example.orders.email;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Task 2.4 RED (HTTP): GET /orders/failed exposes poison records. */
@SpringBootTest
@AutoConfigureMockMvc
class FailedOrdersEndpointTest {

    static KafkaTestHarness harness;

    @BeforeAll
    static void startBroker() {
        harness = new KafkaTestHarness("order-created", "order-email-sent", "order-failed");
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
    PoisonStore poisonStore;

    @Test
    void failedOrdersReturnsPoisonRecords() throws Exception {
        poisonStore.record("email-service", "order-created", "json-decode", "corrupt-payload-xyz");

        mockMvc.perform(get("/orders/failed"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].detectedBy").value("email-service"))
                .andExpect(jsonPath("$[0].reason").value("json-decode"))
                .andExpect(jsonPath("$[0].rawPayload").value("corrupt-payload-xyz"));
    }

    @Test
    void failedOrdersTruncatesRawPayloadTo1000Chars() throws Exception {
        String big = "x".repeat(1500);
        PoisonStore.PoisonRecord rec = poisonStore.record("email-service", "order-created", "json-decode", big);
        assertEquals(1000, rec.rawPayload().length(),
                "rawPayload must be truncated to 1000 characters");

        mockMvc.perform(get("/orders/failed"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").isNumber());
    }
}
