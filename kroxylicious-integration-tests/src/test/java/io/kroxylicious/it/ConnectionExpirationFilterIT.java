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

import io.kroxylicious.filter.connectionexpiration.ConnectionExpiration;
import io.kroxylicious.testing.integration.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.testing.integration.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class ConnectionExpirationFilterIT {

    private static final Duration EXPIRATION_DURATION = Duration.ofSeconds(2);
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    @Test
    void shouldCloseConnectionAfterExpirationAndClientCanReconnect(@BrokerCluster KafkaCluster cluster, Topic topic) {
        var filterDefinition = new NamedFilterDefinitionBuilder(
                "connection-expiration", ConnectionExpiration.class.getName())
                .withConfig("maxAge", "2s")
                .build();
        var proxyConfig = proxy(cluster);
        proxyConfig.addToFilterDefinitions(filterDefinition);
        proxyConfig.addToDefaultFilters(filterDefinition.name());

        try (var tester = kroxyliciousTester(proxyConfig);
                var producer = tester.producer();
                var consumer = tester.consumer(Serdes.String(), Serdes.String(),
                        Map.of(ConsumerConfig.GROUP_ID_CONFIG, "test-group",
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))) {
            assertThat(producer.send(new ProducerRecord<>(topic.name(), "key1", "value1")))
                    .succeedsWithin(TIMEOUT);

            await().pollDelay(EXPIRATION_DURATION.plusSeconds(1)).atMost(TIMEOUT).until(() -> true);

            assertThat(producer.send(new ProducerRecord<>(topic.name(), "key2", "value2")))
                    .succeedsWithin(TIMEOUT);

            consumer.subscribe(List.of(topic.name()));
            assertThat(consumer.poll(TIMEOUT).records(topic.name())).hasSize(2);
        }
    }
}
