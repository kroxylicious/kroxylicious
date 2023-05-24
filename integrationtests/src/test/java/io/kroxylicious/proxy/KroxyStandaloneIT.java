/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.clients.CloseableConsumer;
import io.kroxylicious.testing.kafka.clients.CloseableProducer;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * This test exists to check that the main method works as expected
 */
@ExtendWith(KafkaClusterExtension.class)
public class KroxyStandaloneIT {

    private static final String TOPIC_1 = "my-test-topic";
    private static final String TOPIC_2 = "other-test-topic";
    private static final String PLAINTEXT = "Hello, world!";

    @Test
    public void shouldProxyWhenRunAsStandaloneProcess(KafkaCluster cluster, Admin admin, @TempDir Path tempDir) throws Exception {
        var proxyAddress = HostPort.parse("localhost:9192");

        admin.createTopics(List.of(
                new NewTopic(TOPIC_1, 1, (short) 1),
                new NewTopic(TOPIC_2, 1, (short) 1))).all().get();

        var config = baseConfigBuilder(proxyAddress, cluster.getBootstrapServers())
                .build()
                .toYaml();

        try (var ignored = startProxy(config, tempDir)) {
            try (var producer = CloseableProducer.<String, String> create(Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, proxyAddress.toString(),
                    ProducerConfig.CLIENT_ID_CONFIG, "shouldModifyProduceMessage",
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                    ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000))) {
                producer.send(new ProducerRecord<>(TOPIC_1, "my-key", PLAINTEXT)).get();
                producer.send(new ProducerRecord<>(TOPIC_2, "my-key", PLAINTEXT)).get();
                producer.flush();
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
                records1 = consumer.poll(Duration.ofSeconds(10));
                consumer.subscribe(Set.of(TOPIC_2));
                records2 = consumer.poll(Duration.ofSeconds(10));
            }

            assertEquals(1, records1.count());
            assertEquals(PLAINTEXT, records1.iterator().next().value());
            assertEquals(1, records2.count());
            assertEquals(PLAINTEXT, records2.iterator().next().value());
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
                        .withConfig(Map.of("bootstrapAddress", proxyAddress.toString()))
                        .endClusterEndpointConfigProvider()
                        .build())
                .addNewFilter().withType("ApiVersions").endFilter();
    }

    /**
     * Start proxy as a subprocess, so we have a way to signal it to shut down. If we instead run the
     * main method in-process, when we terminate it, the backing netty services remain running and
     * prevent the test from completing.
     */
    public static AutoCloseable startProxy(String config, Path tempDir) throws IOException {
        Path configPath = tempDir.resolve("config.yaml");
        Files.writeString(configPath, config);
        String java = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
        String classpath = System.getProperty("java.class.path");
        var processBuilder = new ProcessBuilder(java, "-cp", classpath, Kroxylicious.class.getName(), "-c", configPath.toString()).inheritIO();
        try {
            Process start = processBuilder.start();
            return start::destroy;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
