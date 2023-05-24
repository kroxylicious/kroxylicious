/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.kroxylicious.proxy.filter.CreateTopicRejectFilter;
import io.kroxylicious.proxy.internal.filter.ByteBufferTransformation;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.clients.CloseableConsumer;
import io.kroxylicious.testing.kafka.clients.CloseableProducer;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import static io.kroxylicious.proxy.Utils.startProxy;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(KafkaClusterExtension.class)
public class KrpcFilterIT {

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
        for (int index = 0; index < in.limit(); index++) {
            byte b = in.get(index);
            byte rotated = (byte) (b + rot);
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
        for (int index = 0; index < in.limit(); index++) {
            byte b = in.get(index);
            byte rotated = (byte) (b + rot);
            out.put(index, rotated);
        }
        return out;
    }

    @Test
    public void shouldPassThroughRecordUnchanged(KafkaCluster cluster, Admin admin) throws Exception {
        var proxyAddress = HostPort.parse("localhost:9192");

        admin.createTopics(List.of(new NewTopic(TOPIC_1, 1, (short) 1))).all().get();

        var config = baseConfigBuilder(proxyAddress, cluster.getBootstrapServers()).build().toYaml();

        try (var proxy = startProxy(config)) {
            try (var producer = CloseableProducer.<String, String> create(Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, proxyAddress.toString(),
                    ProducerConfig.CLIENT_ID_CONFIG, "shouldPassThroughRecordUnchanged",
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                    ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000))) {
                producer.send(new ProducerRecord<>(TOPIC_1, "my-key", "Hello, world!")).get();
            }

            try (var consumer = CloseableConsumer.<String, String> create(Map.of(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, proxyAddress.toString(),
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                    ConsumerConfig.GROUP_ID_CONFIG, "my-group-id",
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))) {
                consumer.subscribe(Set.of(TOPIC_1));
                var records = consumer.poll(Duration.ofSeconds(10));
                consumer.close();
                assertEquals(1, records.count());
                assertEquals("Hello, world!", records.iterator().next().value());
            }
        }
    }

    @Test
    public void requestFiltersCanRespondWithoutProxying(KafkaCluster cluster, Admin admin) throws Exception {
        var proxyAddress = HostPort.parse("localhost:9192");

        var config = baseConfigBuilder(proxyAddress, cluster.getBootstrapServers())
                .addNewFilter().withType("CreateTopicRejectFilter").endFilter().build().toYaml();

        try (var ignored = startProxy(config)) {
            try (var proxyAdmin = Admin.create(Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, proxyAddress.toString()))) {
                Assertions.assertThatExceptionOfType(ExecutionException.class)
                        .isThrownBy(() -> {
                            proxyAdmin.createTopics(List.of(new NewTopic(TOPIC_1, 1, (short) 1))).all().get();
                        })
                        .withCauseInstanceOf(InvalidTopicException.class)
                        .havingCause()
                        .withMessage(CreateTopicRejectFilter.ERROR_MESSAGE);

                // check no topic created on the cluster
                Set<String> names = admin.listTopics().names().get(10, TimeUnit.SECONDS);
                // remove once https://github.com/kroxylicious/kroxylicious-junit5-extension/issues/114 is fixed.
                names = new HashSet<>(names);
                names.removeIf(n -> n.startsWith("__org_kroxylicious_testing"));
                assertEquals(Set.of(), names);

            }
        }
    }

    @Test
    public void shouldModifyProduceMessage(KafkaCluster cluster, Admin admin) throws Exception {
        var proxyAddress = HostPort.parse("localhost:9192");

        admin.createTopics(List.of(
                new NewTopic(TOPIC_1, 1, (short) 1),
                new NewTopic(TOPIC_2, 1, (short) 1))).all().get();

        var config = baseConfigBuilder(proxyAddress, cluster.getBootstrapServers())
                .addNewFilter().withType("ProduceRequestTransformation").withConfig(Map.of("transformation", TestEncoder.class.getName())).endFilter()
                .build()
                .toYaml();

        try (var proxy = startProxy(config)) {
            try (var producer = CloseableProducer.create(Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, proxyAddress.toString(),
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
            try (var consumer = CloseableConsumer.<String, byte[]> create(Map.of(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, proxyAddress.toString(),
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
    }

    @Test
    public void shouldModifyFetchMessage(KafkaCluster cluster, Admin admin) throws Exception {
        var proxyAddress = HostPort.parse("localhost:9192");

        admin.createTopics(List.of(
                new NewTopic(TOPIC_1, 1, (short) 1),
                new NewTopic(TOPIC_2, 1, (short) 1))).all().get();

        var config = baseConfigBuilder(proxyAddress, cluster.getBootstrapServers())
                .addNewFilter().withType("FetchResponseTransformation").withConfig(Map.of("transformation", TestDecoder.class.getName())).endFilter()
                .build()
                .toYaml();

        try (var proxy = startProxy(config)) {
            try (var producer = CloseableProducer.create(Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, proxyAddress.toString(),
                    ProducerConfig.CLIENT_ID_CONFIG, "shouldModifyFetchMessage",
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                    ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000))) {

                producer.send(new ProducerRecord<>(TOPIC_1, "my-key", TOPIC_1_CIPHERTEXT)).get();
                producer.send(new ProducerRecord<>(TOPIC_2, "my-key", TOPIC_2_CIPHERTEXT)).get();
            }

            ConsumerRecords<String, String> records1;
            ConsumerRecords<String, String> records2;
            try (var consumer = CloseableConsumer.<String, String> create(Map.of(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, proxyAddress.toString(),
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
    }

    private static KroxyConfigBuilder baseConfigBuilder(HostPort proxyAddress, String bootstrapServers) {
        return KroxyConfig.builder()
                .addToVirtualClusters("demo", new VirtualClusterBuilder()
                        .withNewTargetCluster()
                        .withBootstrapServers(bootstrapServers)
                        .endTargetCluster()
                        .withNewClusterEndpointConfigProvider()
                        .withType("PortPerBroker")
                        .withConfig(Map.of("bootstrapAddress", proxyAddress.toString(),
                                "brokerAddressPattern", "localhost:$(portNumber)"))
                        .endClusterEndpointConfigProvider()
                        .build())
                .addNewFilter().withType("ApiVersions").endFilter();
    }

}
