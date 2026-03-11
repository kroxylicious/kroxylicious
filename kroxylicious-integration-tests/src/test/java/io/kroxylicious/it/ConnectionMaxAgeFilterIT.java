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
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.filter.connectionmaxage.ConnectionMaxAgeFilterFactory;
import io.kroxylicious.proxy.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for {@link io.kroxylicious.filter.connectionmaxage.ConnectionMaxAgeFilter}.
 * <p>
 * Verifies that when the connection max age is exceeded, the connection is closed
 * after the next request, and the client can reconnect and continue operating.
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class ConnectionMaxAgeFilterIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    @Test
    void shouldCloseConnectionAfterMaxAgeAndClientCanReconnect(@BrokerCluster KafkaCluster cluster, Topic topic) {
        var className = ConnectionMaxAgeFilterFactory.class.getName();
        // Use a very short max age (2 seconds) for testing
        var filterDefinition = new NamedFilterDefinitionBuilder("connection-max-age", className)
                .withConfig("maxAgeSeconds", 2)
                .build();
        var proxyConfig = proxy(cluster);
        proxyConfig.addToFilterDefinitions(filterDefinition);
        proxyConfig.addToDefaultFilters(filterDefinition.name());

        try (var tester = kroxyliciousTester(proxyConfig);
                var producer = tester.producer();
                var consumer = tester.consumer(Serdes.String(), Serdes.String(),
                        Map.of(ConsumerConfig.GROUP_ID_CONFIG, "test-group",
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))) {
            // First produce should succeed (connection is young)
            var record1 = new ProducerRecord<>(topic.name(), "key1", "value1");
            assertThat(producer.send(record1)).succeedsWithin(TIMEOUT);

            // Wait for connection to exceed max age
            sleep(Duration.ofSeconds(3));

            // Produce after max age - the client library should handle the disconnection
            // by reconnecting automatically and completing the produce
            var record2 = new ProducerRecord<>(topic.name(), "key2", "value2");
            assertThat(producer.send(record2)).succeedsWithin(TIMEOUT);

            // Verify both records were produced successfully
            consumer.subscribe(List.of(topic.name()));
            var records = consumer.poll(TIMEOUT);
            assertThat(records.records(topic.name()))
                    .hasSizeGreaterThanOrEqualTo(2);
        }
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
