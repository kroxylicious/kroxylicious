/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.baseConfigurationBuilder;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for configuration hot-reload via {@code tester.applyConfiguration()}.
 */
@ExtendWith(KafkaClusterExtension.class)
class ConfigurationReloadIT extends BaseIT {

    @Test
    void noOpReloadShouldSucceedAndPreserveConnectivity(KafkaCluster cluster, Topic topic) throws Exception {
        var config = proxy(cluster);

        try (var tester = kroxyliciousTester(config)) {
            // Verify initial connectivity
            try (var producer = tester.producer(Map.of(CLIENT_ID_CONFIG, "reload-producer", DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000));
                    var consumer = tester.consumer(Map.of(GROUP_ID_CONFIG, "reload-group", AUTO_OFFSET_RESET_CONFIG, "earliest"))) {
                producer.send(new ProducerRecord<>(topic.name(), "key1", "before-reload")).get(10, TimeUnit.SECONDS);
                consumer.subscribe(Set.of(topic.name()));
                var records = consumer.poll(Duration.ofSeconds(10));
                assertThat(records.iterator())
                        .toIterable()
                        .hasSize(1)
                        .map(ConsumerRecord::value)
                        .containsExactly("before-reload");
            }

            // Apply the same configuration (no-op reload)
            var result = tester.applyConfiguration(config.build()).get(30, TimeUnit.SECONDS);
            assertThat(result.isSuccess()).isTrue();

            // Verify connectivity still works after reload
            try (var producer = tester.producer(Map.of(CLIENT_ID_CONFIG, "reload-producer-2", DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000));
                    var consumer = tester.consumer(Map.of(GROUP_ID_CONFIG, "reload-group-2", AUTO_OFFSET_RESET_CONFIG, "earliest"))) {
                producer.send(new ProducerRecord<>(topic.name(), "key2", "after-reload")).get(10, TimeUnit.SECONDS);
                consumer.subscribe(Set.of(topic.name()));
                var records = consumer.poll(Duration.ofSeconds(10));
                assertThat(records.iterator())
                        .toIterable()
                        .map(ConsumerRecord::value)
                        .contains("after-reload");
            }
        }
    }

    @Test
    void reloadWithFilterAddedShouldApplyTransformation(KafkaCluster cluster, Topic topic) throws Exception {
        // Start with no filters
        var initialConfig = proxy(cluster);

        try (var tester = kroxyliciousTester(initialConfig)) {
            // Verify initial pass-through behavior (no filter)
            try (var producer = tester.producer(Map.of(CLIENT_ID_CONFIG, "filter-producer", DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000));
                    var consumer = tester.consumer(Map.of(GROUP_ID_CONFIG, "filter-group-1", AUTO_OFFSET_RESET_CONFIG, "earliest"))) {
                producer.send(new ProducerRecord<>(topic.name(), "key1", "hello")).get(10, TimeUnit.SECONDS);
                consumer.subscribe(Set.of(topic.name()));
                var records = consumer.poll(Duration.ofSeconds(10));
                assertThat(records.iterator())
                        .toIterable()
                        .hasSize(1)
                        .map(ConsumerRecord::value)
                        .containsExactly("hello");
            }

            // Build new config with a ProduceRequestTransformation filter that replaces "hello" with "HELLO"
            NamedFilterDefinition uppercaseFilter = new NamedFilterDefinitionBuilder("uppercase", "ProduceRequestTransformation")
                    .withConfig("transformation", "Replacing", "transformationConfig", Map.of("targetPattern", "hello", "replacementValue", "HELLO"))
                    .build();

            ConfigurationBuilder newConfig = proxy(cluster)
                    .addToFilterDefinitions(uppercaseFilter)
                    .addToDefaultFilters(uppercaseFilter.name());

            // Apply the new configuration with filter
            var result = tester.applyConfiguration(newConfig.build()).get(30, TimeUnit.SECONDS);
            assertThat(result.isSuccess()).isTrue();

            // Verify the filter is now active — the transformation should replace "hello" with "HELLO".
            // The consumer reads from "earliest", so it sees both the pre-reload record (unmodified)
            // and the post-reload record (transformed), proving the filter is active after reload.
            try (var producer = tester.producer(Map.of(CLIENT_ID_CONFIG, "filter-producer-2", DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000));
                    var consumer = tester.consumer(Map.of(GROUP_ID_CONFIG, "filter-group-2", AUTO_OFFSET_RESET_CONFIG, "earliest"))) {
                producer.send(new ProducerRecord<>(topic.name(), "key2", "hello")).get(10, TimeUnit.SECONDS);
                consumer.subscribe(Set.of(topic.name()));
                var records = consumer.poll(Duration.ofSeconds(10));
                assertThat(records.iterator())
                        .toIterable()
                        .hasSize(2)
                        .map(ConsumerRecord::value)
                        .containsExactly("hello", "HELLO");
            }
        }
    }

    @Test
    void reloadRemovingClusterShouldSucceed(KafkaCluster cluster) throws Exception {
        // Start with 2 virtual clusters pointing at the same Kafka cluster
        var config = proxy(cluster.getBootstrapServers(), "cluster0", "cluster1");

        try (var tester = kroxyliciousTester(config)) {
            // Verify both clusters are reachable
            try (var admin0 = tester.admin("cluster0");
                    var admin1 = tester.admin("cluster1")) {
                assertThat(admin0.describeCluster().clusterId())
                        .succeedsWithin(10, TimeUnit.SECONDS, InstanceOfAssertFactories.STRING)
                        .isNotBlank();
                assertThat(admin1.describeCluster().clusterId())
                        .succeedsWithin(10, TimeUnit.SECONDS, InstanceOfAssertFactories.STRING)
                        .isNotBlank();
            }

            // Reload with only cluster0 (removing cluster1)
            var newConfig = proxy(cluster.getBootstrapServers(), "cluster0").build();
            var result = tester.applyConfiguration(newConfig).get(30, TimeUnit.SECONDS);
            assertThat(result.isSuccess()).isTrue();

            // Verify cluster0 still works after the reload
            try (var admin0 = tester.admin("cluster0")) {
                assertThat(admin0.describeCluster().clusterId())
                        .succeedsWithin(10, TimeUnit.SECONDS, InstanceOfAssertFactories.STRING)
                        .isNotBlank();
            }
        }
    }

    @Test
    void reloadAddingClusterShouldSucceed(KafkaCluster cluster) throws Exception {
        // Start with a single virtual cluster "demo" on port 9192
        var config = proxy(cluster);

        try (var tester = kroxyliciousTester(config)) {
            // Verify initial cluster works
            try (var admin = tester.admin()) {
                assertThat(admin.describeCluster().clusterId())
                        .succeedsWithin(10, TimeUnit.SECONDS, InstanceOfAssertFactories.STRING)
                        .isNotBlank();
            }

            // Reload with 2 clusters: "demo" on 9192 (unchanged) + "extra" on 9202 (new)
            var newConfig = proxy(cluster.getBootstrapServers(), "demo", "extra").build();
            var result = tester.applyConfiguration(newConfig).get(30, TimeUnit.SECONDS);
            assertThat(result.isSuccess()).isTrue();

            // Verify original cluster still works via the tester
            try (var admin = tester.admin()) {
                assertThat(admin.describeCluster().clusterId())
                        .succeedsWithin(10, TimeUnit.SECONDS, InstanceOfAssertFactories.STRING)
                        .isNotBlank();
            }

            // Verify the newly added cluster is reachable by connecting directly to its bootstrap (port 9202)
            try (var admin = Admin.create(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9202"))) {
                assertThat(admin.describeCluster().clusterId())
                        .succeedsWithin(10, TimeUnit.SECONDS, InstanceOfAssertFactories.STRING)
                        .isNotBlank();
            }
        }
    }

    @Test
    void reloadWithPortConflictShouldRollbackAndPreserveProxy(KafkaCluster cluster) throws Exception {
        // Start with "demo" on port 9192
        var config = proxy(cluster);

        try (var tester = kroxyliciousTester(config)) {
            // Verify initial connectivity
            try (var admin = tester.admin()) {
                assertThat(admin.describeCluster().clusterId())
                        .succeedsWithin(10, TimeUnit.SECONDS, InstanceOfAssertFactories.STRING)
                        .isNotBlank();
            }

            // Build a conflicting config: "demo" on port 9192 (unchanged) + "extra" ALSO on port 9192.
            // The Configuration constructor doesn't check port conflicts (that's PortConflictDetector's
            // job at startup), so this builds successfully. The failure occurs at runtime when the
            // change handler tries to bind "extra" to port 9192, which is already bound by "demo".
            var conflictingConfig = baseConfigurationBuilder()
                    .addToVirtualClusters(new VirtualClusterBuilder()
                            .withName("demo")
                            .withNewTargetCluster().withBootstrapServers(cluster.getBootstrapServers()).endTargetCluster()
                            .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(new HostPort("localhost", 9192)).build())
                            .build())
                    .addToVirtualClusters(new VirtualClusterBuilder()
                            .withName("extra")
                            .withNewTargetCluster().withBootstrapServers(cluster.getBootstrapServers()).endTargetCluster()
                            .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(new HostPort("localhost", 9192)).build())
                            .build())
                    .build();

            // Apply conflicting config — the port bind fails during handleConfigurationChange(),
            // which is caught by the .exceptionally() handler triggering rollback, then
            // re-thrown as a CompletionException so the future completes exceptionally.
            assertThat(tester.applyConfiguration(conflictingConfig))
                    .failsWithin(30, TimeUnit.SECONDS);

            // Verify the proxy STILL works — the old FilterChainFactory was preserved (rollback)
            try (var admin = tester.admin()) {
                assertThat(admin.describeCluster().clusterId())
                        .succeedsWithin(10, TimeUnit.SECONDS, InstanceOfAssertFactories.STRING)
                        .isNotBlank();
            }
        }
    }
}
