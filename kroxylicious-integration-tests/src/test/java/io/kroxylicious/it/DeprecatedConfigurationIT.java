/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.time.Duration;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.testing.integration.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for the deprecated configuration model, where a virtual cluster inlines its
 * {@code targetCluster} rather than referencing a top-level cluster definition. These tests ensure
 * backward compatibility during the deprecation period.
 *
 * @see <a href="https://github.com/kroxylicious/kroxylicious/issues/4924">Issue #4924</a>
 */
@ExtendWith(KafkaClusterExtension.class)
class DeprecatedConfigurationIT extends BaseIT {

    /**
     * Proves that a proxy configured in the deprecated style - with the target cluster inlined into the
     * virtual cluster rather than referenced from a top-level cluster definition - still proxies traffic.
     */
    @Test
    @SuppressWarnings("deprecation")
    void deprecatedInlineTargetCluster_shouldProxyTraffic(@BrokerCluster KafkaCluster cluster, Topic topic) {
        // Given
        // @formatter:off
        var config = new ConfigurationBuilder()
                .addNewVirtualCluster()
                    .withName(KroxyliciousConfigUtils.DEFAULT_VIRTUAL_CLUSTER)
                    .withNewTargetCluster()
                        .withBootstrapServers(cluster.getBootstrapServers())
                    .endTargetCluster()
                    .addToGateways(KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder(
                            KroxyliciousConfigUtils.OS_ASSIGNED_BOOTSTRAP).build())
                .endVirtualCluster();
        // @formatter:on

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer();
                var consumer = tester.consumer()) {

            // When
            assertThat(producer.send(new ProducerRecord<>(topic.name(), "my-key", "Hello, world!")))
                    .succeedsWithin(Duration.ofSeconds(10));

            // Then
            consumer.subscribe(Set.of(topic.name()));
            assertThat(consumer.poll(Duration.ofSeconds(10)).iterator())
                    .toIterable()
                    .singleElement()
                    .extracting(ConsumerRecord::key, ConsumerRecord::value)
                    .containsExactly("my-key", "Hello, world!");
        }
    }
}
