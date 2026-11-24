package com.example.orders.email;

import java.util.List;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.KafkaException;

/**
 * Task 2.5 GREEN: fail-fast topic probe. Verifies the required topics exist at
 * startup; if any is missing, throws a readable RuntimeException naming the
 * topic and the exact create command to run.
 */
public class TopicStartupProbe {

    private final String bootstrapServers;

    public TopicStartupProbe(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public void verifyRequiredTopics(List<String> topics) {
        try (Admin admin = Admin.create(java.util.Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            var existing = admin.listTopics().names().get(5, java.util.concurrent.TimeUnit.SECONDS);
            for (String topic : topics) {
                if (!existing.contains(topic)) {
                    throw new KafkaException(String.format(
                            "Required Kafka topic '%s' does not exist. Create it with:%n" +
                            "  kafka-topics.sh --create --if-not-exists --topic %s --bootstrap-server %s",
                            topic, topic, bootstrapServers));
                }
            }
        } catch (KafkaException e) {
            throw e;
        } catch (Exception e) {
            throw new KafkaException("Failed to probe Kafka topics at " + bootstrapServers + ": " + e.getMessage(), e);
        }
    }
}
