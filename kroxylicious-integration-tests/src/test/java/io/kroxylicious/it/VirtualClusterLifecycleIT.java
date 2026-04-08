/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.proxy.LifecycleException;
import io.kroxylicious.proxy.config.OnVirtualClusterStopped;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.proxy.config.VirtualClusterFailurePolicy;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.test.tester.KroxyliciousConfigUtils;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for the virtual cluster lifecycle state machine,
 * including graceful shutdown with connection draining and partial failure tolerance.
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class VirtualClusterLifecycleIT extends BaseIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(VirtualClusterLifecycleIT.class);

    @Test
    void shouldProduceAndConsumeWithLifecycleManagement(KafkaCluster cluster, Topic topic) {
        var config = proxy(cluster);
        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer();
                var consumer = tester.consumer()) {

            consumer.subscribe(Set.of(topic.name()));

            assertThat(producer.send(new ProducerRecord<>(topic.name(), "lifecycle-key", "lifecycle-value")))
                    .succeedsWithin(Duration.ofSeconds(5));

            var records = consumer.poll(Duration.ofSeconds(10));
            assertThat(records).hasSize(1);
            assertThat(records.iterator().next().value()).isEqualTo("lifecycle-value");
        }
    }

    @Test
    void shouldNotLoseAckedMessagesOnGracefulShutdown(KafkaCluster cluster, Topic topic) {
        int messageCount = 50;
        var config = proxy(cluster)
                .withDrainTimeout(Duration.ofSeconds(10));

        // Phase 1: Produce messages through the proxy and verify they are all acked
        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(Map.of(
                        ProducerConfig.ACKS_CONFIG, "all",
                        ProducerConfig.LINGER_MS_CONFIG, 0))) {
            List<Future<RecordMetadata>> futures = new ArrayList<>();
            for (int i = 0; i < messageCount; i++) {
                futures.add(producer.send(new ProducerRecord<>(topic.name(), "key-" + i, "value-" + i)));
            }
            // Verify all sends were acked before the proxy shuts down
            for (int i = 0; i < futures.size(); i++) {
                assertThat(futures.get(i))
                        .as("Message %d should be acked", i)
                        .succeedsWithin(Duration.ofSeconds(5));
            }
        }
        // tester.close() triggers graceful shutdown with drain

        // Phase 2: Verify all messages are present on the broker via direct consumer
        try (var directConsumer = new KafkaConsumer<String, String>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "verify-drain-" + System.nanoTime(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()))) {

            directConsumer.subscribe(Set.of(topic.name()));

            List<ConsumerRecord<String, String>> allRecords = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 15_000;
            while (allRecords.size() < messageCount && System.currentTimeMillis() < deadline) {
                var records = directConsumer.poll(Duration.ofSeconds(2));
                records.forEach(allRecords::add);
            }

            assertThat(allRecords)
                    .as("All %d messages should be present on the broker after graceful shutdown", messageCount)
                    .hasSize(messageCount);

            // Verify message ordering and content
            for (int i = 0; i < messageCount; i++) {
                assertThat(allRecords.get(i).key()).isEqualTo("key-" + i);
                assertThat(allRecords.get(i).value()).isEqualTo("value-" + i);
            }
        }
    }

    @Test
    void shouldDrainInFlightMessagesBeforeClosingConnections(KafkaCluster cluster, Topic topic) throws Exception {
        int totalMessages = 100;
        var config = proxy(cluster)
                .withDrainTimeout(Duration.ofSeconds(15));

        AtomicBoolean producerRunning = new AtomicBoolean(true);
        List<Future<RecordMetadata>> sendFutures = new ArrayList<>();

        try (var tester = kroxyliciousTester(config)) {
            var producer = tester.producer(Map.of(
                    ProducerConfig.ACKS_CONFIG, "all",
                    ProducerConfig.LINGER_MS_CONFIG, 0,
                    ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000));

            // Send messages in a background thread
            CompletableFuture<Void> producerTask = CompletableFuture.runAsync(() -> {
                for (int i = 0; i < totalMessages && producerRunning.get(); i++) {
                    try {
                        sendFutures.add(producer.send(new ProducerRecord<>(topic.name(), "key-" + i, "value-" + i)));
                        // Small delay to spread messages over time
                        Thread.sleep(10);
                    }
                    catch (Exception e) {
                        LOGGER.debug("Producer send interrupted at message {}: {}", i, e.getMessage());
                        break;
                    }
                }
            });

            // Let some messages flow through
            Thread.sleep(500);

            // Stop the producer from sending more
            producerRunning.set(false);
            producerTask.get(5, TimeUnit.SECONDS);

            // Flush any remaining buffered messages
            producer.flush();
        }
        // tester.close() triggers drain — in-flight messages should complete

        // Count how many messages were actually acked
        int ackedCount = 0;
        for (Future<RecordMetadata> future : sendFutures) {
            try {
                future.get(1, TimeUnit.SECONDS);
                ackedCount++;
            }
            catch (Exception e) {
                // Message was not acked — acceptable if drain timed out
                LOGGER.debug("Message not acked: {}", e.getMessage());
            }
        }

        LOGGER.info("Acked {} out of {} messages", ackedCount, sendFutures.size());

        // Verify that all acked messages are present on the broker
        try (var directConsumer = new KafkaConsumer<String, String>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "verify-inflight-" + System.nanoTime(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()))) {

            directConsumer.subscribe(Set.of(topic.name()));

            List<ConsumerRecord<String, String>> allRecords = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 15_000;
            while (allRecords.size() < ackedCount && System.currentTimeMillis() < deadline) {
                var records = directConsumer.poll(Duration.ofSeconds(2));
                records.forEach(allRecords::add);
            }

            assertThat(allRecords)
                    .as("All %d acked messages should be present on the broker", ackedCount)
                    .hasSize(ackedCount);
        }
    }

    @Test
    void shouldStartWithRemainingPolicyWhenOneVcFails(KafkaCluster cluster, Topic topic) throws Exception {
        // Bind a port on wildcard address to create a conflict for the failing VC.
        // Must use wildcard (0.0.0.0) since Netty binds to wildcard — loopback-only binds won't conflict.
        try (var occupiedSocket = new ServerSocket(0)) {
            int occupiedPort = occupiedSocket.getLocalPort();

            // Build config with two VCs:
            // "good-vc" points at the real cluster on a free port
            // "bad-vc" tries to bind to the occupied port — will fail at startup
            var config = KroxyliciousConfigUtils.baseConfigurationBuilder()
                    .addToVirtualClusters(new VirtualClusterBuilder()
                            .withName("good-vc")
                            .withNewTargetCluster()
                            .withBootstrapServers(cluster.getBootstrapServers())
                            .endTargetCluster()
                            .addToGateways(KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder(
                                    new HostPort("localhost", 9192))
                                    .build())
                            .build())
                    .addToVirtualClusters(new VirtualClusterBuilder()
                            .withName("bad-vc")
                            .withNewTargetCluster()
                            .withBootstrapServers(cluster.getBootstrapServers())
                            .endTargetCluster()
                            .addToGateways(KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder(
                                    new HostPort("localhost", occupiedPort))
                                    .build())
                            .build())
                    .withOnVirtualClusterStopped(new OnVirtualClusterStopped(VirtualClusterFailurePolicy.SUCCESSFUL));

            // The proxy should start successfully despite one VC failing
            try (var tester = kroxyliciousTester(config);
                    var producer = tester.producer("good-vc");
                    var consumer = tester.consumer("good-vc")) {

                consumer.subscribe(Set.of(topic.name()));

                assertThat(producer.send(new ProducerRecord<>(topic.name(), "partial-key", "partial-value")))
                        .succeedsWithin(Duration.ofSeconds(5));

                var records = consumer.poll(Duration.ofSeconds(10));
                assertThat(records).hasSize(1);
                assertThat(records.iterator().next().value()).isEqualTo("partial-value");
            }
        }
    }

    @Test
    void shouldFailStartupWithNonePolicyWhenOneVcFails(KafkaCluster cluster) throws Exception {
        // Bind a port on wildcard address to prevent Netty from binding
        try (var occupiedSocket = new ServerSocket(0)) {
            int occupiedPort = occupiedSocket.getLocalPort();

            var config = KroxyliciousConfigUtils.baseConfigurationBuilder()
                    .addToVirtualClusters(new VirtualClusterBuilder()
                            .withName("good-vc")
                            .withNewTargetCluster()
                            .withBootstrapServers(cluster.getBootstrapServers())
                            .endTargetCluster()
                            .addToGateways(KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder(
                                    new HostPort("localhost", 9192))
                                    .build())
                            .build())
                    .addToVirtualClusters(new VirtualClusterBuilder()
                            .withName("bad-vc")
                            .withNewTargetCluster()
                            .withBootstrapServers(cluster.getBootstrapServers())
                            .endTargetCluster()
                            .addToGateways(KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder(
                                    new HostPort("localhost", occupiedPort))
                                    .build())
                            .build())
                    .withOnVirtualClusterStopped(new OnVirtualClusterStopped(VirtualClusterFailurePolicy.NONE));

            // With NONE policy, any VC failure should cause startup to fail
            assertThatThrownBy(() -> kroxyliciousTester(config).close())
                    .isInstanceOf(LifecycleException.class)
                    .hasMessageContaining("failed to start");
        }
    }

    @Test
    void shouldFailStartupByDefaultWhenOneVcFails(KafkaCluster cluster) throws Exception {
        // Default policy is NONE — verify backward compatibility
        try (var occupiedSocket = new ServerSocket(0)) {
            int occupiedPort = occupiedSocket.getLocalPort();

            var config = KroxyliciousConfigUtils.baseConfigurationBuilder()
                    .addToVirtualClusters(new VirtualClusterBuilder()
                            .withName("good-vc")
                            .withNewTargetCluster()
                            .withBootstrapServers(cluster.getBootstrapServers())
                            .endTargetCluster()
                            .addToGateways(KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder(
                                    new HostPort("localhost", 9192))
                                    .build())
                            .build())
                    .addToVirtualClusters(new VirtualClusterBuilder()
                            .withName("bad-vc")
                            .withNewTargetCluster()
                            .withBootstrapServers(cluster.getBootstrapServers())
                            .endTargetCluster()
                            .addToGateways(KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder(
                                    new HostPort("localhost", occupiedPort))
                                    .build())
                            .build());
            // No onVirtualClusterFailure set — defaults to NONE

            assertThatThrownBy(() -> kroxyliciousTester(config).close())
                    .isInstanceOf(LifecycleException.class);
        }
    }

    @Test
    void shouldConsumeAndProduceMultipleMessagesReliably(KafkaCluster cluster, Topic topic) {
        int messageCount = 200;
        var config = proxy(cluster);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(Map.of(
                        ProducerConfig.ACKS_CONFIG, "all",
                        ProducerConfig.BATCH_SIZE_CONFIG, 1024));
                var consumer = tester.consumer()) {

            consumer.subscribe(Set.of(topic.name()));

            // Produce all messages
            List<Future<RecordMetadata>> futures = new ArrayList<>();
            for (int i = 0; i < messageCount; i++) {
                futures.add(producer.send(new ProducerRecord<>(topic.name(), "key-" + i, "value-" + i)));
            }

            // Verify all acked
            for (var future : futures) {
                assertThat(future).succeedsWithin(Duration.ofSeconds(10));
            }

            // Consume all messages
            List<ConsumerRecord<String, String>> allRecords = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 30_000;
            while (allRecords.size() < messageCount && System.currentTimeMillis() < deadline) {
                var records = consumer.poll(Duration.ofSeconds(2));
                records.forEach(allRecords::add);
            }

            assertThat(allRecords)
                    .as("All %d messages should be consumed through the proxy", messageCount)
                    .hasSize(messageCount);
        }
    }

    @Test
    void shouldFailStartupWhenAllVcsFail() throws Exception {
        // Occupy two ports with wide separation to avoid PortIdentifiesNode range collisions.
        // PortIdentifiesNode assigns broker ports as bootstrap+1, bootstrap+2, etc.
        // A separation of 100 ports ensures the ranges don't overlap.
        try (var socket1 = new ServerSocket(0)) {
            int port1 = socket1.getLocalPort();
            int port2 = port1 + 100;
            try (var socket2 = new ServerSocket(port2)) {

                var config = KroxyliciousConfigUtils.baseConfigurationBuilder()
                        .addToVirtualClusters(new VirtualClusterBuilder()
                                .withName("bad-vc-1")
                                .withNewTargetCluster()
                                .withBootstrapServers("localhost:9092")
                                .endTargetCluster()
                                .addToGateways(KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder(
                                        new HostPort("localhost", port1))
                                        .build())
                                .build())
                        .addToVirtualClusters(new VirtualClusterBuilder()
                                .withName("bad-vc-2")
                                .withNewTargetCluster()
                                .withBootstrapServers("localhost:9092")
                                .endTargetCluster()
                                .addToGateways(KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder(
                                        new HostPort("localhost", port2))
                                        .build())
                                .build())
                        .withOnVirtualClusterStopped(new OnVirtualClusterStopped(VirtualClusterFailurePolicy.SUCCESSFUL));

                // Even with SUCCESSFUL policy, all VCs failing should cause startup to fail
                assertThatThrownBy(() -> kroxyliciousTester(config).close())
                        .isInstanceOf(LifecycleException.class)
                        .hasMessageContaining("All");
            }
        }
    }
}
