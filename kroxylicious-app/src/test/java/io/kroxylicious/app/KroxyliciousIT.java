/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.app;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * This test exists to check that the main method works as expected
 */
@ExtendWith(KafkaClusterExtension.class)
class KroxyliciousIT {

    private static final String TOPIC_1 = "my-test-topic";
    private static final String TOPIC_2 = "other-test-topic";
    private static final String PLAINTEXT = "Hello, world!";

    @Test
    void shouldProxyWhenRunAsStandaloneProcess(
            KafkaCluster cluster,
            Admin admin,
            @TempDir
            Path tempDir
    ) throws Exception {
        var proxyAddress = HostPort.parse("localhost:9192");

        admin.createTopics(
                List.of(
                        new NewTopic(TOPIC_1, 1, (short) 1),
                        new NewTopic(TOPIC_2, 1, (short) 1)
                )
        ).all().get();

        try (var tester = kroxyliciousTester(proxy(cluster), new SubprocessKroxyliciousFactory(tempDir));
                var producer = tester.producer(
                        Map.of(
                                ProducerConfig.CLIENT_ID_CONFIG,
                                "shouldModifyProduceMessage",
                                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
                                3_600_000
                        )
                );
                var consumer = tester.consumer()) {
            producer.send(new ProducerRecord<>(TOPIC_1, "my-key", PLAINTEXT)).get();
            producer.send(new ProducerRecord<>(TOPIC_2, "my-key", PLAINTEXT)).get();
            producer.flush();

            consumer.subscribe(Set.of(TOPIC_1));
            ConsumerRecords<String, String> records1 = consumer.poll(Duration.ofSeconds(10));
            consumer.subscribe(Set.of(TOPIC_2));
            ConsumerRecords<String, String> records2 = consumer.poll(Duration.ofSeconds(10));

            assertEquals(1, records1.count());
            assertEquals(PLAINTEXT, records1.iterator().next().value());
            assertEquals(1, records2.count());
            assertEquals(PLAINTEXT, records2.iterator().next().value());
        }
    }

    private record SubprocessKroxyliciousFactory(Path tempDir) implements Function<Configuration, AutoCloseable> {

        @Override
        public AutoCloseable apply(Configuration config) {
            try {
                Path configPath = tempDir.resolve("config.yaml");
                Files.writeString(configPath, new ConfigParser().toYaml(config));
                String java = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
                String classpath = System.getProperty("java.class.path");
                var processBuilder = new ProcessBuilder(java, "-cp", classpath, "io.kroxylicious.app.Kroxylicious", "-c", configPath.toString()).inheritIO();
                Process start = processBuilder.start();
                return () -> {
                    start.destroy();
                    start.onExit().get(10, TimeUnit.SECONDS);
                };
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
