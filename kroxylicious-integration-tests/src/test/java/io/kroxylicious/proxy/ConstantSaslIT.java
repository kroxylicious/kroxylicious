/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.time.Duration;
import java.util.Map;
import java.util.Set;

import javax.security.auth.login.LoginException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.serialization.Serdes;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.proxy.testplugins.ClientAuthAwareLawyerFilter;
import io.kroxylicious.proxy.testplugins.ClientTlsAwareLawyer;
import io.kroxylicious.proxy.testplugins.ConstantSasl;
import io.kroxylicious.test.assertj.KafkaAssertions;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class ConstantSaslIT {

    private static void doAThing(KafkaCluster cluster,
                                 Topic topic,
                                 Map<String, Object> filterConfig,
                                 boolean expectedClientSaslPresent,
                                 @Nullable String expectedAuthorizedId,
                                 @Nullable String expectedMechanism) {
        NamedFilterDefinition saslInspection = new NamedFilterDefinitionBuilder(
                ConstantSasl.class.getName(),
                ConstantSasl.class.getName())
                .withConfig(filterConfig)
                .build();
        NamedFilterDefinition lawyer = new NamedFilterDefinitionBuilder(
                ClientTlsAwareLawyer.class.getName(),
                ClientTlsAwareLawyer.class.getName())
                .build();
        var config = proxy(cluster)
                .addToFilterDefinitions(saslInspection, lawyer)
                .addToDefaultFilters(saslInspection.name(), lawyer.name());

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer();
                var consumer = tester
                        .consumer(Serdes.String(), Serdes.ByteArray(), Map.of(
                                ConsumerConfig.GROUP_ID_CONFIG, "my-group-id",
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                                ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "1"))) {

            assertThat(producer.send(new ProducerRecord<>(topic.name(), "my-key", "my-value")))
                    .succeedsWithin(Duration.ofSeconds(5));

            producer.flush();

            consumer.subscribe(Set.of(topic.name()));
            var records = consumer.poll(Duration.ofSeconds(10));

            assertThat(records).hasSize(1);
            var recordHeaders = assertThat(records.records(topic.name()))
                    .as("topic %s records", topic.name())
                    .singleElement()
                    .asInstanceOf(new InstanceOfAssertFactory<>(ConsumerRecord.class, KafkaAssertions::assertThat))
                    .headers();
            recordHeaders.singleHeaderWithKey(ClientAuthAwareLawyerFilter.HEADER_KEY_CLIENT_SASL_CONTEXT_PRESENT)
                    .value().containsExactly(expectedClientSaslPresent ? (byte) 1 : (byte) 0);
            recordHeaders.singleHeaderWithKey(ClientAuthAwareLawyerFilter.HEADER_KEY_CLIENT_SASL_AUTHORIZATION_ID)
                    .hasValueEqualTo(expectedAuthorizedId);
            recordHeaders.singleHeaderWithKey(ClientAuthAwareLawyerFilter.HEADER_KEY_CLIENT_SASL_MECH_NAME)
                    .hasValueEqualTo(expectedMechanism);

        }
    }

    @Test
    void shouldHavePresentClientSaslContextWhenAuthSuccessful(
                                                              KafkaCluster cluster,
                                                              Topic topic) {
        String mechanism = "FOO";
        String authorizedId = "bob";
        Map<String, Object> filterConfig = Map.of("api", ApiKeys.PRODUCE,
                "mechanism", mechanism,
                "authorizedId", authorizedId);
        doAThing(cluster, topic, filterConfig, true, authorizedId, mechanism);
    }

    @Test
    void shouldHaveEmptyClientSaslContextWhenAuthFailed(
                                                        KafkaCluster cluster,
                                                        Topic topic) {
        String mechanism = "FOO";
        String authorizedId = "bob";
        Map<String, Object> filterConfig = Map.of("api", ApiKeys.PRODUCE,
                "mechanism", mechanism,
                "authorizedId", authorizedId,
                "exceptionClassName", LoginException.class.getName());
        doAThing(cluster, topic, filterConfig, false, null, null);
    }
}
