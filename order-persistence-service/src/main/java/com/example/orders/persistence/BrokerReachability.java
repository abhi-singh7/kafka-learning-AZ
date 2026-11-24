package com.example.orders.persistence;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.stereotype.Component;

/**
 * Reports whether the configured broker is currently reachable, for GET /health.
 * Never throws: an unreachable broker is reported, not raised.
 */
@Component
public class BrokerReachability {

    private final String bootstrapServers;

    BrokerReachability(@org.springframework.beans.factory.annotation.Value("${spring.kafka.bootstrap-servers}")
                       String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String status() {
        try (Admin admin = Admin.create(java.util.Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            admin.listTopics().names().get(2, TimeUnit.SECONDS);
            return "reachable";
        } catch (Exception e) {
            return "unreachable";
        }
    }
}
