/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.filter.protocollogging.ProtocolLogging;
import io.kroxylicious.testing.integration.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.testing.integration.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class ProtocolLoggingFilterIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    @Test
    void trafficRoundTripsCorrectlyWithFilterInChain(@BrokerCluster KafkaCluster cluster, Topic topic) {
        // Given
        var filterDef = new NamedFilterDefinitionBuilder(
                "protocol-logger", ProtocolLogging.class.getName())
                .build();
        var proxyConfig = proxy(cluster);
        proxyConfig.addToFilterDefinitions(filterDef);
        proxyConfig.addToDefaultFilters(filterDef.name());

        // When
        try (var tester = kroxyliciousTester(proxyConfig);
                var producer = tester.producer();
                var consumer = tester.consumer(Serdes.String(), Serdes.String(),
                        Map.of(ConsumerConfig.GROUP_ID_CONFIG, "round-trip-group",
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))) {

            assertThat(producer.send(new ProducerRecord<>(topic.name(), "k1", "v1")))
                    .succeedsWithin(TIMEOUT);
            assertThat(producer.send(new ProducerRecord<>(topic.name(), "k2", "v2")))
                    .succeedsWithin(TIMEOUT);

            consumer.subscribe(List.of(topic.name()));

            // Then
            var records = consumer.poll(TIMEOUT).records(topic.name());
            assertThat(records)
                    .hasSize(2)
                    .extracting(ConsumerRecord::key, ConsumerRecord::value)
                    .containsExactly(
                            org.assertj.core.groups.Tuple.tuple("k1", "v1"),
                            org.assertj.core.groups.Tuple.tuple("k2", "v2"));
        }
    }

    /** Verifies apiKeyNames config is accepted and traffic is unaffected; gating behaviour covered by unit tests. */
    @Test
    void filterAcceptsApiKeyGatingConfig(@BrokerCluster KafkaCluster cluster, Topic topic) {
        // Given
        var filterDef = new NamedFilterDefinitionBuilder(
                "protocol-logger", ProtocolLogging.class.getName())
                .withConfig("apiKeyNames", List.of("METADATA"))
                .build();
        var proxyConfig = proxy(cluster);
        proxyConfig.addToFilterDefinitions(filterDef);
        proxyConfig.addToDefaultFilters(filterDef.name());

        // When
        try (var tester = kroxyliciousTester(proxyConfig);
                var producer = tester.producer();
                var consumer = tester.consumer(Serdes.String(), Serdes.String(),
                        Map.of(ConsumerConfig.GROUP_ID_CONFIG, "gating-group",
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))) {

            assertThat(producer.send(new ProducerRecord<>(topic.name(), "k1", "gated")))
                    .succeedsWithin(TIMEOUT);

            consumer.subscribe(List.of(topic.name()));

            // Then
            var records = consumer.poll(TIMEOUT).records(topic.name());
            assertThat(records)
                    .hasSize(1)
                    .first()
                    .extracting(ConsumerRecord::value)
                    .isEqualTo("gated");
        }
    }

    /** Verifies maxBodyChars config is accepted and traffic is unaffected; truncation behaviour covered by unit tests. */
    @Test
    void filterAcceptsMaxBodyCharsConfig(@BrokerCluster KafkaCluster cluster, Topic topic) {
        // Given
        var filterDef = new NamedFilterDefinitionBuilder(
                "protocol-logger", ProtocolLogging.class.getName())
                .withConfig("maxBodyChars", 32)
                .build();
        var proxyConfig = proxy(cluster);
        proxyConfig.addToFilterDefinitions(filterDef);
        proxyConfig.addToDefaultFilters(filterDef.name());

        // When
        try (var tester = kroxyliciousTester(proxyConfig);
                var producer = tester.producer();
                var consumer = tester.consumer(Serdes.String(), Serdes.String(),
                        Map.of(ConsumerConfig.GROUP_ID_CONFIG, "truncation-group",
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))) {

            assertThat(producer.send(new ProducerRecord<>(topic.name(), "k1", "truncated-test")))
                    .succeedsWithin(TIMEOUT);

            consumer.subscribe(List.of(topic.name()));

            // Then
            var records = consumer.poll(TIMEOUT).records(topic.name());
            assertThat(records)
                    .hasSize(1)
                    .first()
                    .extracting(ConsumerRecord::value)
                    .isEqualTo("truncated-test");
        }
    }

    /** Verifies logLevel config is accepted and traffic is unaffected; level gating covered by unit tests. */
    @Test
    void filterAcceptsLogLevelConfig(@BrokerCluster KafkaCluster cluster, Topic topic) {
        // Given
        var filterDef = new NamedFilterDefinitionBuilder(
                "protocol-logger", ProtocolLogging.class.getName())
                .withConfig("logLevel", "TRACE")
                .build();
        var proxyConfig = proxy(cluster);
        proxyConfig.addToFilterDefinitions(filterDef);
        proxyConfig.addToDefaultFilters(filterDef.name());

        // When
        try (var tester = kroxyliciousTester(proxyConfig);
                var producer = tester.producer();
                var consumer = tester.consumer(Serdes.String(), Serdes.String(),
                        Map.of(ConsumerConfig.GROUP_ID_CONFIG, "level-group",
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))) {

            assertThat(producer.send(new ProducerRecord<>(topic.name(), "k1", "level-test")))
                    .succeedsWithin(TIMEOUT);

            consumer.subscribe(List.of(topic.name()));

            // Then
            var records = consumer.poll(TIMEOUT).records(topic.name());
            assertThat(records)
                    .hasSize(1)
                    .first()
                    .extracting(ConsumerRecord::value)
                    .isEqualTo("level-test");
        }
    }

}
