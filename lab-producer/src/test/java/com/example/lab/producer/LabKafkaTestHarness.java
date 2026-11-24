package com.example.lab.producer;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

/**
 * Test-only embedded Kafka broker (hermetic, no docker). One instance per test.
 * Provides raw produce/consume helpers so tests never need the full Spring context.
 */
public final class LabKafkaTestHarness implements AutoCloseable {

    private final EmbeddedKafkaBroker broker;

    // Each test context boots its own Kafka Streams instance (the lane is part of the
    // producer app). Surefire runs all contexts in one JVM, so every context needs an
    // ISOLATED state directory or the process-id file collides across instances.
    private final java.nio.file.Path stateDir = newStreamsStateDir();

    private static java.nio.file.Path newStreamsStateDir() {
        try {
            return java.nio.file.Files.createTempDirectory("lab-streams-state-");
        } catch (java.io.IOException e) {
            throw new IllegalStateException("cannot create Kafka Streams state dir", e);
        }
    }

    /** Starts a single-broker KRaft cluster and creates the given topics (1 partition each). */
    public LabKafkaTestHarness(String... topics) {
        this.broker = new EmbeddedKafkaKraftBroker(1, 1, topics)
                .brokerProperties(java.util.Map.of(
                        "__transaction_state.replication.factor", "1",
                        "__transaction_state.partition.count", "2"));
        this.broker.afterPropertiesSet();
    }


    /** Starts a single-broker KRaft cluster with N partitions per topic. */
    public LabKafkaTestHarness(int partitionCount, String... topics) {
        this.broker = new EmbeddedKafkaKraftBroker(1, partitionCount, topics)
                .brokerProperties(java.util.Map.of(
                        "__transaction_state.replication.factor", "1",
                        "__transaction_state.partition.count", "2"));
        this.broker.afterPropertiesSet();
    }
    /** Pre-creates a topic with replication 1 (needed for __transaction_state:
     * a single-broker KRaft cluster cannot honour kafka-clients' RF=3 request). */
    public void createTopic(String topic, int partitions, short replication) {
        org.springframework.kafka.core.KafkaAdmin admin =
                new org.springframework.kafka.core.KafkaAdmin(java.util.Map.of(
                        "bootstrap.servers", broker.getBrokersAsString()));
        admin.setAutoCreate(false);
        admin.createOrModifyTopics(new org.apache.kafka.clients.admin.NewTopic(topic, partitions, replication));
    }

    /**
     * Pre-creates a topic with the {@code compact} cleanup policy - required for the Confluent
     * Schema Registry's own store topic (it refuses to start against a non-compacted one).
     */
    public void createCompactTopic(String topic, int partitions, short replication) {
        org.springframework.kafka.core.KafkaAdmin admin =
                new org.springframework.kafka.core.KafkaAdmin(java.util.Map.of(
                        "bootstrap.servers", broker.getBrokersAsString()));
        admin.setAutoCreate(false);
        admin.createOrModifyTopics(new org.apache.kafka.clients.admin.NewTopic(topic, partitions, replication)
                .configs(java.util.Map.of("cleanup.policy", "compact")));
    }

    public String bootstrapServers() {
        return broker.getBrokersAsString();
    }

    /** Per-context Kafka Streams state directory (isolates concurrent test contexts). */
    public String stateDir() {
        return stateDir.toString();
    }

    /** Sends one string message to topic with the given key. */
    public void produce(String topic, String key, String value) {
        try (KafkaProducer<String, String> p = new KafkaProducer<>(java.util.Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class))) {
            p.send(new ProducerRecord<>(topic, key, value)).get(10, java.util.concurrent.TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("produce failed", e);
        }
    }

    /** Blocks up to timeoutMs for one message on topic; returns its value or null. */
    public String consumeOne(String topic, long timeoutMs) {
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(java.util.Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "harness-" + System.nanoTime(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
            c.subscribe(Collections.singleton(topic));
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = c.poll(Duration.ofMillis(250));
                for (ConsumerRecord<String, String> r : records) {
                    if (topic.equals(r.topic())) {
                        return r.value();
                    }
                }
            }
            return null;
        }
    }

    /** Consumes up to maxRecords messages from a topic (for asserting message counts). */
    public List<String> consumeUpTo(String topic, int maxRecords, long timeoutMs) {
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(java.util.Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "harness-" + System.nanoTime(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
            c.subscribe(Collections.singleton(topic));
            List<String> out = new java.util.ArrayList<>();
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (out.size() < maxRecords && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = c.poll(Duration.ofMillis(250));
                for (ConsumerRecord<String, String> r : records) {
                    if (topic.equals(r.topic())) {
                        out.add(r.value());
                    }
                }
            }
            return out;
        }
    }

    /** Consume up to maxRecords and return the sorted header-key list per record (routing of contract headers). */
    public java.util.List<java.util.List<String>> consumeHeaderKeys(String topic, int maxRecords, long timeoutMs) {
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(java.util.Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "harness-headers-" + System.nanoTime(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
            c.subscribe(Collections.singleton(topic));
            java.util.ArrayList<java.util.List<String>> out = new java.util.ArrayList<>();
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (out.size() < maxRecords && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = c.poll(Duration.ofMillis(250));
                for (ConsumerRecord<String, String> r : records) {
                    if (topic.equals(r.topic())) {
                        java.util.ArrayList<String> keys = new java.util.ArrayList<>();
                        for (Header h : r.headers()) {
                            keys.add(h.key());
                        }
                        java.util.Collections.sort(keys);
                        out.add(keys);
                    }
                }
            }
            return out;
        }
    }

    /** Consume up to maxRecords messages, exposing partition/offset/key/value so tests can assert routing. */
    public List<LinePart> consumeParts(String topic, int maxRecords, long timeoutMs) {
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(java.util.Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "harness-" + System.nanoTime(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
            c.subscribe(Collections.singleton(topic));
            List<LinePart> out = new java.util.ArrayList<LinePart>();
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (out.size() < maxRecords && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = c.poll(Duration.ofMillis(250));
                for (ConsumerRecord<String, String> r : records) {
                    if (topic.equals(r.topic())) {
                        out.add(new LinePart(r.partition(), r.offset(), r.key(), r.value()));
                    }
                }
            }
            return out;
        }
    }

    // ---- Task 3.2: standalone Confluent Schema Registry (separate JVM) -----------------
    // The SR cannot run inside this Spring/Logback JVM (javax vs jakarta JAX-RS + reload4j
    // conflicts), so the harness launches it as its own process using the clean classpath that
    // exec-maven-plugin materialises into target/schema-registry, pointed at THIS embedded broker.
    private Process schemaRegistryProcess;

    /**
     * Starts a standalone Confluent Schema Registry against this embedded broker and returns its
     * base URL (http://localhost:&lt;port&gt;) once it is ready. The caller must pre-create the store
     * topic (see {@link #createTopic}) because the KRaft test broker does not auto-create topics.
     */
    public String startSchemaRegistry(String storeTopic) {
        int port = freePort();
        String url = "http://localhost:" + port;
        try {
            Path props = Files.createTempFile("lab-sr-", ".properties");
            Properties p = new Properties();
            p.setProperty("listeners", url);
            p.setProperty("host.name", "localhost");
            // Protocol-prefixed endpoint is required to match kafkastore.security.protocol.
            p.setProperty("kafkastore.bootstrap.servers", "PLAINTEXT://" + bootstrapServers());
            p.setProperty("kafkastore.topic", storeTopic);
            p.setProperty("kafkastore.topic.replication.factor", "1");
            p.setProperty("schema.compatibility.level", "BACKWARD");
            try (java.io.Writer w = Files.newBufferedWriter(props)) { p.store(w, null); }

            File dir = new File("target/schema-registry");
            List<String> jars = new ArrayList<>();
            for (File f : dir.listFiles((d, n) -> n.endsWith(".jar"))) {
                jars.add(f.getAbsolutePath());
            }
            if (jars.isEmpty()) {
                throw new IllegalStateException(
                        "schema-registry classpath not built at " + dir.getAbsolutePath()
                                + " - run `mvn process-test-resources` first");
            }
            String cp = String.join(File.pathSeparator, jars);
            String java = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
            Path log = Files.createTempFile("lab-sr-log-", ".log");
            ProcessBuilder pb = new ProcessBuilder(java, "-cp", cp,
                    "io.confluent.kafka.schemaregistry.rest.Main", props.toString());
            pb.redirectErrorStream(true);
            pb.redirectOutput(ProcessBuilder.Redirect.to(log.toFile()));
            schemaRegistryProcess = pb.start();
        } catch (java.io.IOException e) {
            throw new IllegalStateException("failed to start schema registry", e);
        }

        long deadline = System.currentTimeMillis() + 45_000;
        while (System.currentTimeMillis() < deadline) {
            if (!schemaRegistryProcess.isAlive()) {
                throw new IllegalStateException("schema registry process exited early");
            }
            if (httpGetCode(url + "/subjects") == 200) {
                return url;
            }
            sleepQuietly(500);
        }
        stopSchemaRegistry();
        throw new IllegalStateException("schema registry did not become ready at " + url);
    }

    /** Stops the standalone schema registry process started by {@link #startSchemaRegistry}. */
    public void stopSchemaRegistry() {
        if (schemaRegistryProcess == null) {
            return;
        }
        schemaRegistryProcess.destroy();
        try {
            if (!schemaRegistryProcess.waitFor(5, java.util.concurrent.TimeUnit.SECONDS)) {
                schemaRegistryProcess.destroyForcibly();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        schemaRegistryProcess = null;
    }

    private static int freePort() {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        } catch (java.io.IOException e) {
            throw new IllegalStateException("cannot find a free port", e);
        }
    }

    /** Returns the HTTP status code for GET url, or 0 if the connection fails. */
    private static int httpGetCode(String url) {
        try {
            HttpURLConnection c = (HttpURLConnection) new URL(url).openConnection();
            c.setConnectTimeout(1500);
            c.setReadTimeout(2500);
            int code = c.getResponseCode();
            c.disconnect();
            return code;
        } catch (java.io.IOException e) {
            return 0;
        }
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        stopSchemaRegistry();
        broker.destroy();
    }
}
