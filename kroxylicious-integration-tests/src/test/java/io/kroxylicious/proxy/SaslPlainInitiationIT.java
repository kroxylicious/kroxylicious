/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.proxy.testplugins.SaslPlainInitiation;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.SaslMechanism;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class SaslPlainInitiationIT {
    @Test
    void shouldInitiate(@SaslMechanism(principals = { @SaslMechanism.Principal(user = "alice", password = "alice-secret") }) KafkaCluster cluster,
                        Topic topic) {

        NamedFilterDefinition saslInitiation = new NamedFilterDefinitionBuilder(
                SaslPlainInitiation.class.getName(),
                SaslPlainInitiation.class.getName())
                .withConfig("username", "alice",
                        "password", "alice-secret")
                .build();
        var config = proxy(cluster)
                .addToFilterDefinitions(saslInitiation)
                .addToDefaultFilters(saslInitiation.name());

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(Map.of());
                var consumer = tester
                        .consumer(Serdes.String(), Serdes.ByteArray(), Map.of(
                                GROUP_ID_CONFIG, "my-group-id",
                                AUTO_OFFSET_RESET_CONFIG, "earliest"))) {
            assertThat(producer.send(new ProducerRecord<>(topic.name(), "my-key", "my-value")))
                    .succeedsWithin(Duration.ofSeconds(5));

            consumer.subscribe(Set.of(topic.name()));
            var records = consumer.poll(Duration.ofSeconds(10));

            assertThat(records).hasSize(1);
            assertThat(records.records(topic.name()))
                    .as("topic %s records", topic.name())
                    .singleElement()
                    .extracting(ConsumerRecord::value)
                    .as("record value")
                    .extracting(String::new)
                    .isEqualTo("my-value");
        }
    }

    @Test
    void shouldHandleInvalidPassword(@SaslMechanism(principals = { @SaslMechanism.Principal(user = "alice", password = "alice-secret") }) KafkaCluster cluster) {

        NamedFilterDefinition saslInitiation = new NamedFilterDefinitionBuilder(
                SaslPlainInitiation.class.getName(),
                SaslPlainInitiation.class.getName())
                .withConfig(
                        "username", "alice",
                        "password", "WRONG-PASSWORD")
                .build();
        var config = proxy(cluster)
                .addToFilterDefinitions(saslInitiation)
                .addToDefaultFilters(saslInitiation.name());

        try (var tester = kroxyliciousTester(config);
                var producer = tester.admin(Map.of(
                        CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, "1000",
                        CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG, "1000",
                        CommonClientConfigs.METADATA_RECOVERY_STRATEGY_CONFIG, "none"))) {
            assertThat(producer.listTopics().names())
                    .failsWithin(Duration.ofSeconds(5))
                    .withThrowableThat()
                    .isExactlyInstanceOf(ExecutionException.class)
                    .havingCause()
                    .isExactlyInstanceOf(TimeoutException.class);
        }
    }

    @Test
    void shouldHandleNoCommonMechanism(@SaslMechanism(value = "SCRAM-SHA-256",
                                               principals = { @SaslMechanism.Principal(user = "alice", password = "alice-secret") }) KafkaCluster cluster)
            throws Exception {

        NamedFilterDefinition saslInitiation = new NamedFilterDefinitionBuilder(
                SaslPlainInitiation.class.getName(),
                SaslPlainInitiation.class.getName())
                .withConfig(
                        "username", "alice",
                        "password", "WRONG-PASSWORD")
                .build();
        var config = proxy(cluster)
                .addToFilterDefinitions(saslInitiation)
                .addToDefaultFilters(saslInitiation.name());

        try (var tester = kroxyliciousTester(config);
                var producer = tester.admin(Map.of(
                        CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, "1000",
                        CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG, "1000",
                        CommonClientConfigs.METADATA_RECOVERY_STRATEGY_CONFIG, "none"))) {
            assertThat(producer.listTopics().names())
                    .failsWithin(Duration.ofSeconds(5))
                    .withThrowableThat()
                    .isExactlyInstanceOf(ExecutionException.class)
                    .havingCause()
                    .isExactlyInstanceOf(TimeoutException.class);
        }
    }
}
