/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy;

import static java.lang.Integer.parseInt;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.debezium.kafka.KafkaCluster;
import io.kroxylicious.proxy.bootstrap.FilterChainFactory;
import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.internal.filter.ByteBufferTransformation;
import io.kroxylicious.proxy.util.SystemTest;

@SystemTest
public class KrpcFilterIntegrationTest {

    private static final String TOPIC_1 = "my-test-topic";
    private static final String TOPIC_2 = "other-test-topic";
    private static final String PLAINTEXT = "Hello, world!";
    private static final byte[] TOPIC_1_CIPHERTEXT = { (byte) 0x3d, (byte) 0x5a, (byte) 0x61, (byte) 0x61, (byte) 0x64, (byte) 0x21, (byte) 0x15, (byte) 0x6c,
            (byte) 0x64, (byte) 0x67, (byte) 0x61, (byte) 0x59, (byte) 0x16 };
    private static final byte[] TOPIC_2_CIPHERTEXT = { (byte) 0xffffffa7, (byte) 0xffffffc4, (byte) 0xffffffcb, (byte) 0xffffffcb, (byte) 0xffffffce, (byte) 0xffffff8b,
            (byte) 0x7f, (byte) 0xffffffd6, (byte) 0xffffffce, (byte) 0xffffffd1, (byte) 0xffffffcb, (byte) 0xffffffc3, (byte) 0xffffff80 };

    @BeforeAll
    public static void checkReversibleEncryption() {
        // The precise details of the cipher don't matter
        // What matters is that it the ciphertext key depends on the topic name
        // and that decode() is the inverse of encode()
        assertArrayEquals(TOPIC_1_CIPHERTEXT, encode(TOPIC_1, ByteBuffer.wrap(PLAINTEXT.getBytes(StandardCharsets.UTF_8))).array());
        assertEquals(PLAINTEXT, new String(decode(TOPIC_1, ByteBuffer.wrap(TOPIC_1_CIPHERTEXT)).array(), StandardCharsets.UTF_8));
        assertArrayEquals(TOPIC_2_CIPHERTEXT, encode(TOPIC_2, ByteBuffer.wrap(PLAINTEXT.getBytes(StandardCharsets.UTF_8))).array());
        assertEquals(PLAINTEXT, new String(decode(TOPIC_2, ByteBuffer.wrap(TOPIC_2_CIPHERTEXT)).array(), StandardCharsets.UTF_8));
    }

    public static class TestEncoder implements ByteBufferTransformation {

        @Override
        public ByteBuffer transform(String topicName, ByteBuffer in) {
            return encode(topicName, in);
        }
    }

    private static ByteBuffer encode(String topicName, ByteBuffer in) {
        var out = ByteBuffer.allocate(in.limit());
        byte rot = (byte) (topicName.hashCode() % Byte.MAX_VALUE);
        // System.out.println("Encode Rot " + rot);
        for (int index = 0; index < in.limit(); index++) {
            byte b = in.get(index);
            byte rotated = (byte) (b + rot);
            // System.out.println("rotate(" + b + ", " + rot + "): " + rotated);
            out.put(index, rotated);
        }
        return out;
    }

    public static class TestDecoder implements ByteBufferTransformation {

        @Override
        public ByteBuffer transform(String topicName, ByteBuffer in) {
            return decode(topicName, in);
        }
    }

    private static ByteBuffer decode(String topicName, ByteBuffer in) {
        var out = ByteBuffer.allocate(in.limit());
        out.limit(in.limit());
        byte rot = (byte) -(topicName.hashCode() % Byte.MAX_VALUE);
        // System.out.println("Decode Rot " + rot);
        for (int index = 0; index < in.limit(); index++) {
            byte b = in.get(index);
            byte rotated = (byte) (b + rot);
            // System.out.println("rotate(" + b + ", " + rot + "): " + rotated);
            out.put(index, rotated);
        }
        return out;
    }

    @Test
    public void shouldPassThroughRecordUnchanged() throws Exception {
        String proxyHost = "localhost";
        int proxyPort = 9192;
        String proxyAddress = String.format("%s:%d", proxyHost, proxyPort);

        String brokerList = startKafkaCluster();

        try (var admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList))) {
            admin.createTopics(List.of(new NewTopic(TOPIC_1, 1, (short) 1))).all().get();
        }

        String config = """
                clusters:
                  demo:
                    address: localhost:9092
                filters:
                - type: ApiVersions
                - type: BrokerAddress
                  config:
                    proxyHost: localhost
                    proxyPort: 9192
                """;

        FilterChainFactory filterChainFactory = new FilterChainFactory(new ConfigParser().parseConfiguration(config));

        var proxy = startProxy(proxyHost, proxyPort, brokerList, filterChainFactory);

        var producer = new KafkaProducer<String, String>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, proxyAddress,
                ProducerConfig.CLIENT_ID_CONFIG, "shouldPassThroughRecordUnchanged",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000));
        producer.send(new ProducerRecord<>(TOPIC_1, "my-key", "Hello, world!")).get();
        producer.close();

        var consumer = new KafkaConsumer<String, String>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, proxyAddress,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "my-group-id",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
        consumer.subscribe(Set.of(TOPIC_1));
        var records = consumer.poll(Duration.ofSeconds(10));
        consumer.close();
        assertEquals(1, records.count());
        assertEquals("Hello, world!", records.iterator().next().value());

        // shutdown the proxy
        proxy.shutdown();
    }

    @Test
    public void shouldModifyProduceMessage() throws Exception {
        String proxyHost = "localhost";
        int proxyPort = 9192;
        String proxyAddress = String.format("%s:%d", proxyHost, proxyPort);

        String brokerList = startKafkaCluster();

        try (var admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList))) {
            admin.createTopics(List.of(
                    new NewTopic(TOPIC_1, 1, (short) 1),
                    new NewTopic(TOPIC_2, 1, (short) 1))).all().get();
        }

        String config = """
                clusters:
                  demo:
                    address: localhost:9092
                filters:
                - type: ApiVersions
                - type: BrokerAddress
                  config:
                    proxyHost: localhost
                    proxyPort: 9192
                - type: ProduceRequestTransformation
                  config:
                    transformation: io.kroxylicious.proxy.KrpcFilterIntegrationTest$TestEncoder
                """;

        FilterChainFactory filterChainFactory = new FilterChainFactory(new ConfigParser().parseConfiguration(config));

        var proxy = startProxy(proxyHost, proxyPort, brokerList,
                filterChainFactory);
        try {
            try (var producer = new KafkaProducer<String, String>(Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, proxyAddress,
                    ProducerConfig.CLIENT_ID_CONFIG, "shouldModifyProduceMessage",
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                    ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000))) {
                producer.send(new ProducerRecord<>(TOPIC_1, "my-key", PLAINTEXT)).get();
                producer.send(new ProducerRecord<>(TOPIC_2, "my-key", PLAINTEXT)).get();
                producer.flush();
            }

            ConsumerRecords<String, byte[]> records1;
            ConsumerRecords<String, byte[]> records2;
            try (var consumer = new KafkaConsumer<String, byte[]>(Map.of(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, proxyAddress,
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class,
                    ConsumerConfig.GROUP_ID_CONFIG, "my-group-id",
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))) {
                consumer.subscribe(Set.of(TOPIC_1));
                records1 = consumer.poll(Duration.ofSeconds(10));
                consumer.subscribe(Set.of(TOPIC_2));
                records2 = consumer.poll(Duration.ofSeconds(10));
            }

            assertEquals(1, records1.count());
            assertArrayEquals(TOPIC_1_CIPHERTEXT, records1.iterator().next().value());
            assertEquals(1, records2.count());
            assertArrayEquals(TOPIC_2_CIPHERTEXT, records2.iterator().next().value());

        }
        finally {
            // shutdown the proxy
            proxy.shutdown();
        }
    }

    private void printBuf(ByteBuffer buffer) {
        for (int index = 0; index < 256; index++) {
            byte i = buffer.get(index);
            if (i < 16) {
                System.out.print("0");
            }
            System.out.print(Integer.toUnsignedString(i, 16));
            System.out.print(" ");
        }
        System.out.println();
    }

    @Test
    public void shouldModifyFetchMessage() throws Exception {
        String proxyHost = "localhost";
        int proxyPort = 9192;
        String proxyAddress = String.format("%s:%d", proxyHost, proxyPort);

        String brokerList = startKafkaCluster();

        try (var admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList))) {
            admin.createTopics(List.of(
                    new NewTopic(TOPIC_1, 1, (short) 1),
                    new NewTopic(TOPIC_2, 1, (short) 1))).all().get();
        }

        String config = """
                clusters:
                  demo:
                    address: localhost:9092
                filters:
                - type: ApiVersions
                - type: BrokerAddress
                  config:
                    proxyHost: localhost
                    proxyPort: 9192
                - type: FetchResponseTransformation
                  config:
                    transformation: io.kroxylicious.proxy.KrpcFilterIntegrationTest$TestDecoder
                """;

        FilterChainFactory filterChainFactory = new FilterChainFactory(new ConfigParser().parseConfiguration(config));

        var proxy = startProxy(proxyHost, proxyPort, brokerList,
                filterChainFactory);
        try {

            try (var producer = new KafkaProducer<String, byte[]>(Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, proxyAddress,
                    ProducerConfig.CLIENT_ID_CONFIG, "shouldModifyFetchMessage",
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                    ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000))) {

                producer.send(new ProducerRecord<>(TOPIC_1, "my-key", TOPIC_1_CIPHERTEXT)).get();
                producer.send(new ProducerRecord<>(TOPIC_2, "my-key", TOPIC_2_CIPHERTEXT)).get();
            }

            ConsumerRecords<String, String> records1;
            ConsumerRecords<String, String> records2;
            try (var consumer = new KafkaConsumer<String, String>(Map.of(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, proxyAddress,
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                    ConsumerConfig.GROUP_ID_CONFIG, "my-group-id",
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))) {
                consumer.subscribe(Set.of(TOPIC_1));

                records1 = consumer.poll(Duration.ofSeconds(100));

                consumer.subscribe(Set.of(TOPIC_2));

                records2 = consumer.poll(Duration.ofSeconds(100));
            }
            assertEquals(1, records1.count());
            assertEquals(1, records2.count());
            assertEquals(List.of(PLAINTEXT, PLAINTEXT),
                    List.of(records1.iterator().next().value(),
                            records2.iterator().next().value()));

        }
        finally {
            // shutdown the proxy
            proxy.shutdown();
        }
    }

    private KafkaProxy startProxy(String proxyHost,
                                  int proxyPort,
                                  String brokerList,
                                  FilterChainFactory filterChainFactory)
            throws InterruptedException {
        String[] hostPort = brokerList.split(",")[0].split(":");

        KafkaProxy kafkaProxy = new KafkaProxy(proxyHost,
                proxyPort,
                hostPort[0],
                parseInt(hostPort[1]),
                true,
                true,
                false,
                filterChainFactory);
        kafkaProxy.startup();
        return kafkaProxy;
    }

    private String startKafkaCluster() throws IOException {
        var kafkaCluster = new KafkaCluster()
                .addBrokers(1)
                .usingDirectory(Files.createTempDirectory(KrpcFilterIntegrationTest.class.getName()).toFile())
                // .withKafkaConfiguration()
                .startup();

        return kafkaCluster.brokerList();
    }
}
