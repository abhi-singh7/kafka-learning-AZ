package com.example.lab.producer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Task 3.2 - AVRO lane with a real (standalone) Confluent Schema Registry and schema evolution.
 *
 * <p>The registry runs as a SEPARATE JVM process (see {@link LabKafkaTestHarness#startSchemaRegistry})
 * pointed at the embedded broker, because it cannot share this Spring/Logback test JVM. The tests
 * themselves use raw Kafka clients with the Confluent Avro serializer - which is allowed in
 * test-harness code (the app wires the same serializers via {@code spring.kafka.*} properties).
 */
class LabAvroLaneTest {

    private static final String TOPIC = "lab-avro";
    private static final String STORE_TOPIC = "lab-sr-store";
    private static final String SUBJECT = TOPIC + "-value";

    // v1: the original three fields.
    private static final String V1_SCHEMA = """
            {"type":"record","name":"OrderEvent","namespace":"com.example.lab.avro",
             "fields":[
               {"name":"orderId","type":"string"},
               {"name":"customer","type":"string"},
               {"name":"amount","type":"double"}]}
            """;
    // v2: adds discount + phoneNumber, each with a default so old (v1) data reads cleanly.
    private static final String V2_SCHEMA = """
            {"type":"record","name":"OrderEvent","namespace":"com.example.lab.avro",
             "fields":[
               {"name":"orderId","type":"string"},
               {"name":"customer","type":"string"},
               {"name":"amount","type":"double"},
               {"name":"discount","type":"double","default":0.0},
               {"name":"phoneNumber","type":"string","default":"123-456-7890"}]}
            """;

    private LabKafkaTestHarness h;
    private String srUrl;

    @BeforeEach
    void setUp() {
        h = new LabKafkaTestHarness(TOPIC);
        h.createCompactTopic(STORE_TOPIC, 1, (short) 1);
        srUrl = h.startSchemaRegistry(STORE_TOPIC);
    }

    @AfterEach
    void tearDown() {
        if (h != null) {
            h.close(); // also stops the schema registry
        }
    }

    private GenericRecord v1(String orderId, double amount) {
        Schema s = new Schema.Parser().parse(V1_SCHEMA);
        GenericData.Record r = new GenericData.Record(s);
        r.put("orderId", orderId);
        r.put("customer", "cust-" + orderId);
        r.put("amount", amount);
        return r;
    }

    private GenericRecord v2(String orderId, double amount, double discount, String phone) {
        Schema s = new Schema.Parser().parse(V2_SCHEMA);
        GenericData.Record r = new GenericData.Record(s);
        r.put("orderId", orderId);
        r.put("customer", "cust-" + orderId);
        r.put("amount", amount);
        r.put("discount", discount);
        r.put("phoneNumber", phone);
        return r;
    }

    private void produceAvro(String sr, GenericRecord... records) throws Exception {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, h.bootstrapServers());
        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        cfg.put("schema.registry.url", sr);
        try (KafkaProducer<String, GenericRecord> p = new KafkaProducer<>(cfg)) {
            for (GenericRecord r : records) {
                p.send(new ProducerRecord<>(TOPIC, "cust-" + r.get("orderId"), r))
                        .get(15, java.util.concurrent.TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void v1ThenV2_registryHoldsTwoVersions_andOldReaderFillsDefaults() throws Exception {
        // Push one v1 record then one v2 record -> the registry must register two versions.
        produceAvro(srUrl, v1("o-1", 10.5), v2("o-2", 20.5, 1.5, "555-0001"));

        // /subjects/<subject>/versions -> exactly two version ids (1 and 2).
        String versions = httpGet(srUrl + "/subjects/" + SUBJECT + "/versions");
        assertTrue(versions.contains("1") && versions.contains("2"),
                "expected two versions, got: " + versions);
        assertTrue(!versions.contains("3"), "expected only two versions, got: " + versions);

        // _schemas holds BOTH distinct schemas (id 1 lacks discount; id 2 has it).
        String s1 = httpGet(srUrl + "/schemas/ids/1");
        String s2 = httpGet(srUrl + "/schemas/ids/2");
        assertTrue(!s1.contains("discount"), "v1 schema should have no discount field: " + s1);
        assertTrue(s2.contains("discount"), "v2 schema should have the discount field: " + s2);

        // Old reader: decode the v1-written record with the LATEST (v2) schema. Avro's resolving
        // decoder fills the new fields from their defaults because the writer had no such fields.
        byte[] envelope = consumeFirstRawValue(TOPIC);
        int writerId = envelopeSchemaId(envelope); // 1 -> written with v1
        assertEquals(1, writerId, "the first record must have been written with schema id 1");

        Schema writer = parseSchema(s1);
        Schema reader = parseSchema(s2); // latest registered schema
        GenericRecord decoded = resolveDecode(envelope, writer, reader);

        assertEquals("o-1", decoded.get("orderId").toString());
        assertEquals(0.0, (double) decoded.get("discount"), 1e-9,
                "v1 record read with the latest schema must get discount default 0.0");
        assertEquals("123-456-7890", decoded.get("phoneNumber").toString(),
                "v1 record read with the latest schema must get phoneNumber default");
    }

    @Test
    void registryUnreachable_startupFailsFast_namingRegistryUrl() throws Exception {
        // An unresolvable host (reserved .invalid TLD) makes the failure name the registry URL:
        // the UnknownHostException carries the exact host we configured.
        String badHost = "no-such-host-lab.invalid";
        String badUrl = "http://" + badHost + ":8081";
        java.util.Properties props = new java.util.Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, h.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put("schema.registry.url", badUrl);
        try (KafkaProducer<String, GenericRecord> p = new KafkaProducer<>(props)) {
            RuntimeException ex = assertThrows(RuntimeException.class,
                    () -> p.send(new ProducerRecord<>(TOPIC, "k", v1("o-x", 1.0)))
                            .get(15, java.util.concurrent.TimeUnit.SECONDS),
                    "an unreachable registry must fail fast on first use");
            assertTrue(exceptionText(ex).contains(badHost),
                    "failure should name the registry URL; got: " + exceptionText(ex));
        }
    }

    /** Reads the raw Confluent Avro envelope (magic byte + schema id + binary record) of the first message. */
    private byte[] consumeFirstRawValue(String topic) {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, h.bootstrapServers());
        cfg.put(ConsumerConfig.GROUP_ID_CONFIG, "avro-raw-" + System.nanoTime());
        cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        try (KafkaConsumer<String, byte[]> c = new KafkaConsumer<>(cfg)) {
            c.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + 15_000;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, byte[]> records = c.poll(Duration.ofMillis(250));
                for (ConsumerRecord<String, byte[]> r : records) {
                    if (topic.equals(r.topic())) {
                        return r.value();
                    }
                }
            }
        }
        throw new IllegalStateException("no message arrived on " + topic);
    }

    /** Confluent envelope layout: [0]=magic, [1..4]=big-endian schema id, [5..]=record bytes. */
    private static int envelopeSchemaId(byte[] env) {
        return ByteBuffer.wrap(env, 1, 4).getInt();
    }

    /** Decodes the record bytes of an envelope using writer->reader schema resolution (fills defaults). */
    private static GenericRecord resolveDecode(byte[] env, Schema writer, Schema reader)
            throws java.io.IOException {
        org.apache.avro.io.Decoder bin = DecoderFactory.get()
                .binaryDecoder(env, 5, env.length - 5, null);
        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(writer, reader);
        return datumReader.read(null, bin);
    }

    private static Schema parseSchema(String registryBody) {
        // The registry returns {"schema":"<escaped avro json>"}; unwrap and unescape it.
        String marker = "\"schema\":\"";
        int start = registryBody.indexOf(marker) + marker.length();
        int end = registryBody.lastIndexOf('"');
        String inner = registryBody.substring(start, end).replace("\\\"", "\"");
        return new Schema.Parser().parse(inner);
    }

    private static String httpGet(String url) throws Exception {
        HttpURLConnection c = (HttpURLConnection) new URL(url).openConnection();
        c.setConnectTimeout(2000);
        c.setReadTimeout(4000);
        int code = c.getResponseCode();
        byte[] body = readAll(code >= 400 ? c.getErrorStream() : c.getInputStream());
        c.disconnect();
        return new String(body, StandardCharsets.UTF_8);
    }

    private static byte[] readAll(java.io.InputStream in) throws java.io.IOException {
        if (in == null) {
            return new byte[0];
        }
        try (in) {
            return in.readAllBytes();
        }
    }

    /** Concatenates a throwable chain's messages so we can assert the URL was named. */
    private static String exceptionText(Throwable t) {
        StringBuilder sb = new StringBuilder();
        while (t != null) {
            if (t.getMessage() != null) {
                sb.append(t.getMessage()).append(' ');
            }
            t = t.getCause();
        }
        return sb.toString();
    }
}
