/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.filter.connectionexpiration.ConnectionExpiration;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.test.tester.FilesystemSnapshotBuilder;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test demonstrating the config2 parsing pipeline end-to-end:
 * filesystem files → FilesystemSnapshot → ProxyConfigParser → Configuration → KafkaProxy.
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class Config2ConnectionExpirationIT {

    private static final Duration EXPIRATION_DURATION = Duration.ofSeconds(2);
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    @TempDir
    Path configDir;

    @Test
    void shouldProxyViaConfig2ParsedConfiguration(
                                                  @BrokerCluster KafkaCluster cluster,
                                                  Topic topic) {
        String filterInstanceYaml = """
                name: connection-expiration
                type: %s
                version: v1alpha1
                config:
                  maxAge: "2s"
                """.formatted(ConnectionExpiration.class.getName());

        Configuration configuration = new FilesystemSnapshotBuilder(configDir)
                .proxyConfig(cluster.getBootstrapServers(), "connection-expiration")
                .addPluginInstance(
                        FilterFactory.class,
                        "connection-expiration",
                        filterInstanceYaml)
                .buildConfiguration();

        try (var tester = kroxyliciousTester(configuration);
                var producer = tester.producer();
                var consumer = tester.consumer(Serdes.String(), Serdes.String(),
                        Map.of(ConsumerConfig.GROUP_ID_CONFIG, "test-group",
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))) {
            // Produce a message before the connection expires
            assertThat(producer.send(new ProducerRecord<>(topic.name(), "key1", "value1")))
                    .succeedsWithin(TIMEOUT);

            // Wait for the connection to expire
            await().pollDelay(EXPIRATION_DURATION.plusSeconds(1)).atMost(TIMEOUT).until(() -> true);

            // Produce another message after expiration — the client should reconnect
            assertThat(producer.send(new ProducerRecord<>(topic.name(), "key2", "value2")))
                    .succeedsWithin(TIMEOUT);

            // Consume both messages to verify the proxy worked throughout
            consumer.subscribe(List.of(topic.name()));
            assertThat(consumer.poll(TIMEOUT).records(topic.name())).hasSize(2);
        }
    }
}
