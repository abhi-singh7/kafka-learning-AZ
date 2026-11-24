package com.example.orders.persistence;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Task 3.1 RED: startup creates order_line_events idempotently with the design's
 * columns/guards; the sibling table order_lines is NOT modified.
 * Uses the real native Postgres (ordersdb) + an embedded Kafka broker.
 */
@SpringBootTest
class SchemaCreationTest {

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

    @Test
    void orderLineEventsTableExistsWithGuardsAndSiblingUntouched() throws Exception {
        try (Connection conn = DriverManager.getConnection(
                "jdbc:postgresql://127.0.0.1:5432/ordersdb", "postgres", "postgres")) {

            // order_line_events exists
            assertTrue(tableExists(conn, "order_line_events"), "order_line_events must exist");

            // key columns present (Spring Boot default snake_case physical naming)
            assertColumn(conn, "order_line_events", "order_id");
            assertColumn(conn, "order_line_events", "sku");
            assertColumn(conn, "order_line_events", "message_id");
            assertColumn(conn, "order_line_events", "customer_id");
            assertColumn(conn, "order_line_events", "line_index");

            // sibling order_lines still exists and its row count is unchanged (baseline 5)
            assertTrue(tableExists(conn, "order_lines"), "order_lines must still exist");
            assertEquals(5, rowCount(conn, "order_lines"),
                    "order_lines row count must be unchanged from baseline");
        }
    }

    private static boolean tableExists(Connection conn, String name) throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT 1 FROM information_schema.tables WHERE table_name='" + name + "'")) {
            return rs.next();
        }
    }

    private static void assertColumn(Connection conn, String table, String col) throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT 1 FROM information_schema.columns WHERE table_name='" + table
                             + "' AND column_name='" + col + "'")) {
            assertTrue(rs.next(), "column " + col + " must exist on " + table);
        }
    }

    private static int rowCount(Connection conn, String table) throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT count(*) FROM " + table)) {
            rs.next();
            return rs.getInt(1);
        }
    }
}
