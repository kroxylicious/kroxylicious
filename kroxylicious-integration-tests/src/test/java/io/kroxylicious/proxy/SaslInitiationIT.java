/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.time.Duration;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.proxy.testplugins.ClientTlsAwareLawyer;
import io.kroxylicious.proxy.testplugins.SaslPlainInitiation;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.SaslMechanism;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class SaslInitiationIT {
    @Test
    void shouldInitiate(@SaslMechanism(principals = { @SaslMechanism.Principal(user = "alice", password = "alice-secret") }) KafkaCluster cluster,
                        Topic topic)
            throws Exception {
        String testName = "shouldInitiate";

        NamedFilterDefinition saslInitiation = new NamedFilterDefinitionBuilder(
                SaslPlainInitiation.class.getName(),
                SaslPlainInitiation.class.getName())
                .build();
        var config = proxy(cluster)
                .addToFilterDefinitions(saslInitiation)
                .addToDefaultFilters(saslInitiation.name());

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(Map.of(
                        CLIENT_ID_CONFIG, testName + "-producer",
                        DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000));
                var consumer = tester
                        .consumer(Serdes.String(), Serdes.ByteArray(), Map.of(
                                CLIENT_ID_CONFIG, testName + "-consumer",
                                GROUP_ID_CONFIG, "my-group-id",
                                AUTO_OFFSET_RESET_CONFIG, "earliest"))) {
            producer.send(new ProducerRecord<>(topic.name(), "my-key", "my-value")).get();

            producer.flush();

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
}
