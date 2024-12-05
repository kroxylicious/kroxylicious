/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.app;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import io.kroxylicious.proxy.ProxyEnvironment;
import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static io.kroxylicious.test.tester.KroxyliciousTesters.newBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * This test exists to check that the main method works as expected
 */
@ExtendWith(KafkaClusterExtension.class)
class KroxyliciousIT {

    private static final String TOPIC_1 = "my-test-topic";
    private static final String TOPIC_2 = "other-test-topic";
    private static final String PLAINTEXT = "Hello, world!";
    private static final ProxyEnvironment DEFAULT_ENV = ProxyEnvironment.PRODUCTION;

    @Test
    void shouldFailToStartWithInternalConfigurationsByDefault(@TempDir Path tempDir) throws IOException {
        SubprocessKroxyliciousFactory kroxyliciousFactory = new SubprocessKroxyliciousFactory(tempDir, (environment, processBuilder) -> {
            // no-op so that io is not inherited
        }, List.of());
        var tester = kroxyliciousTester(proxy("fake:9092").withInternal(Map.of("a", "b")), kroxyliciousFactory);
        Process lastProcess = kroxyliciousFactory.lastProcess;
        assertThat(lastProcess).isNotNull();
        assertThat(lastProcess.onExit()).succeedsWithin(5, TimeUnit.SECONDS);
        byte[] bytes = lastProcess.getInputStream().readAllBytes();
        String output = new String(bytes, StandardCharsets.UTF_8);
        assertThat(output).contains("internal configuration for proxy present in production environment");
        tester.close();
    }

    @Test
    void shouldFailToStartInExplicitProductionEnvironmentWithInternalConfigurations(@TempDir Path tempDir) throws IOException {
        SubprocessKroxyliciousFactory kroxyliciousFactory = new SubprocessKroxyliciousFactory(tempDir, (environment, processBuilder) -> {
            processBuilder.environment().put(Kroxylicious.KROXYLICIOUS_ENVIRONMENT, environment.name());
        }, List.of());
        var tester = kroxyliciousTester(proxy("fake:9092").withInternal(Map.of("a", "b")), kroxyliciousFactory);
        Process lastProcess = kroxyliciousFactory.lastProcess;
        assertThat(lastProcess).isNotNull();
        assertThat(lastProcess.onExit()).succeedsWithin(5, TimeUnit.SECONDS);
        byte[] bytes = lastProcess.getInputStream().readAllBytes();
        String output = new String(bytes, StandardCharsets.UTF_8);
        assertThat(output).contains("internal configuration for proxy present in production environment");
        tester.close();
    }

    @Test
    void shouldStartInDevelopmentEnvironmentWithInternalConfigurationEnvVar(KafkaCluster cluster, Admin admin, @TempDir Path tempDir) throws Exception {
        admin.createTopics(List.of(
                new NewTopic(TOPIC_1, 1, (short) 1),
                new NewTopic(TOPIC_2, 1, (short) 1))).all().get();

        try (var tester = newBuilder(proxy(cluster).withInternal(Map.of("a", "b")))
                .setKroxyliciousFactory(new SubprocessKroxyliciousFactory(tempDir))
                .setEnvironment(ProxyEnvironment.DEVELOPMENT)
                .createDefaultKroxyliciousTester();
                var producer = tester.producer(Map.of(
                        ProducerConfig.CLIENT_ID_CONFIG, "shouldModifyProduceMessage",
                        ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000));
                var consumer = tester.consumer()) {
            assertProxies(producer, consumer);
        }
    }

    @Test
    void shouldStartInDevelopmentEnvironmentWithInternalConfigurationSystemProp(KafkaCluster cluster, Admin admin, @TempDir Path tempDir) throws Exception {
        admin.createTopics(List.of(
                new NewTopic(TOPIC_1, 1, (short) 1),
                new NewTopic(TOPIC_2, 1, (short) 1))).all().get();

        try (var tester = newBuilder(proxy(cluster).withInternal(Map.of("a", "b")))
                .setKroxyliciousFactory(new SubprocessKroxyliciousFactory(tempDir, (environment, processBuilder) -> processBuilder.inheritIO(),
                        List.of("-D" + Kroxylicious.KROXYLICIOUS_ENVIRONMENT + "=" + ProxyEnvironment.DEVELOPMENT)))
                .createDefaultKroxyliciousTester();
                var producer = tester.producer(Map.of(
                        ProducerConfig.CLIENT_ID_CONFIG, "shouldModifyProduceMessage",
                        ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000));
                var consumer = tester.consumer()) {
            assertProxies(producer, consumer);
        }
    }

    @Test
    void shouldProxyWhenRunAsStandaloneProcess(KafkaCluster cluster, Admin admin, @TempDir Path tempDir) throws Exception {
        admin.createTopics(List.of(
                new NewTopic(TOPIC_1, 1, (short) 1),
                new NewTopic(TOPIC_2, 1, (short) 1))).all().get();

        try (var tester = kroxyliciousTester(proxy(cluster), new SubprocessKroxyliciousFactory(tempDir));
                var producer = tester.producer(Map.of(
                        ProducerConfig.CLIENT_ID_CONFIG, "shouldModifyProduceMessage",
                        ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000));
                var consumer = tester.consumer()) {
            assertProxies(producer, consumer);
        }
    }

    private static void assertProxies(Producer<String, String> producer, Consumer<String, String> consumer)
            throws InterruptedException, ExecutionException {
        producer.send(new ProducerRecord<>(KroxyliciousIT.TOPIC_1, "my-key", KroxyliciousIT.PLAINTEXT)).get();
        producer.send(new ProducerRecord<>(KroxyliciousIT.TOPIC_2, "my-key", KroxyliciousIT.PLAINTEXT)).get();
        producer.flush();

        consumer.subscribe(Set.of(KroxyliciousIT.TOPIC_1));
        ConsumerRecords<String, String> records1 = consumer.poll(Duration.ofSeconds(10));
        consumer.subscribe(Set.of(KroxyliciousIT.TOPIC_2));
        ConsumerRecords<String, String> records2 = consumer.poll(Duration.ofSeconds(10));

        assertEquals(1, records1.count());
        assertEquals(KroxyliciousIT.PLAINTEXT, records1.iterator().next().value());
        assertEquals(1, records2.count());
        assertEquals(KroxyliciousIT.PLAINTEXT, records2.iterator().next().value());
    }

    private static class SubprocessKroxyliciousFactory implements BiFunction<ProxyEnvironment, Configuration, AutoCloseable> {

        private final Path tempDir;
        private final java.util.function.BiConsumer<ProxyEnvironment, ProcessBuilder> processBuilderModifier;
        private final List<String> jvmArgs;
        private Process lastProcess;

        SubprocessKroxyliciousFactory(Path tempDir) {
            this(tempDir, (env, processBuilder) -> {
                processBuilder.inheritIO();
                if (env != DEFAULT_ENV) {
                    processBuilder.environment().put(Kroxylicious.KROXYLICIOUS_ENVIRONMENT, env.name());
                }
            }, List.of());
        }

        SubprocessKroxyliciousFactory(Path tempDir, java.util.function.BiConsumer<ProxyEnvironment, ProcessBuilder> processBuilderModifier, List<String> jvmArgs) {
            this.tempDir = tempDir;
            this.processBuilderModifier = processBuilderModifier;
            this.jvmArgs = jvmArgs;
        }

        @Override
        public AutoCloseable apply(ProxyEnvironment environment, Configuration config) {
            try {
                Path configPath = tempDir.resolve("config.yaml");
                Files.writeString(configPath, new ConfigParser().toYaml(config));
                String java = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
                String classpath = System.getProperty("java.class.path");
                List<String> command = new ArrayList<>();
                command.add(java);
                command.addAll(jvmArgs);
                command.addAll(List.of("-cp", classpath, "io.kroxylicious.app.Kroxylicious", "-c", configPath.toString()));
                var processBuilder = new ProcessBuilder(command);
                processBuilderModifier.accept(environment, processBuilder);
                lastProcess = processBuilder.start();
                return () -> {
                    lastProcess.destroy();
                    lastProcess.onExit().get(10, TimeUnit.SECONDS);
                };
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
