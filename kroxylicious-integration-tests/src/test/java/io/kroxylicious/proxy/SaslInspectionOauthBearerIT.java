/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.time.Duration;
import java.util.Set;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.junit.jupiter.api.Test;

import io.kroxylicious.filters.sasl.inspection.SaslInspection;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.proxy.testplugins.ClientAuthAwareLawyer;
import io.kroxylicious.proxy.testplugins.ClientAuthAwareLawyerFilter;
import io.kroxylicious.test.assertj.KafkaAssertions;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for the SASL Inspection Filter for SASL OAUTHBEARER (<a href="https://datatracker.ietf.org/doc/html/rfc7628">RFC-7628</a>.)
 */

class SaslInspectionOauthBearerIT extends BaseOauthBearerIT {

    @Test
    void shouldAuthenticateSuccessfully(Topic topic) {
        try (var tester = kroxyliciousTester(getConfiguredProxyBuilder());
                var producer = tester.producer(getProducerConfig());
                var consumer = tester.consumer(getConsumerConfig());) {
            assertThat(producer.send(new ProducerRecord<>(topic.name(), "my-key", "my-value")))
                    .succeedsWithin(Duration.ofSeconds(5));

            consumer.subscribe(Set.of(topic.name()));
            var records = consumer.poll(Duration.ofSeconds(10));

            assertThat(records).hasSize(1);
            var recordHeaders = assertThat(records.records(topic.name()))
                    .as("topic %s records", topic.name())
                    .singleElement()
                    .asInstanceOf(new InstanceOfAssertFactory<>(ConsumerRecord.class, KafkaAssertions::assertThat))
                    .headers();

            recordHeaders.singleHeaderWithKey(ClientAuthAwareLawyerFilter.HEADER_KEY_CLIENT_SASL_CONTEXT_PRESENT)
                    .hasByteValueSatisfying(val -> assertThat(val).isEqualTo(ClientAuthAwareLawyerFilter.TRUE));

            recordHeaders.singleHeaderWithKey(ClientAuthAwareLawyerFilter.HEADER_KEY_CLIENT_SASL_AUTHORIZATION_ID)
                    .hasValueEqualTo(CLIENT_ID);
        }
    }

    private ConfigurationBuilder getConfiguredProxyBuilder() {
        var namedFilterDefinitionBuilder = new NamedFilterDefinitionBuilder(
                SaslInspection.class.getName(),
                SaslInspection.class.getName());

        var saslInspection = namedFilterDefinitionBuilder.build();
        var lawyer = new NamedFilterDefinitionBuilder(
                ClientAuthAwareLawyer.class.getName(),
                ClientAuthAwareLawyer.class.getName())
                .build();
        return proxy(cluster)
                .addToFilterDefinitions(saslInspection, lawyer)
                .addToDefaultFilters(saslInspection.name(), lawyer.name());
    }

    /**
     * Pings the cluster in order to assert connectivity. We don't care about the result.
     * @param admin admin
     */
    private KafkaFuture<?> performClusterOperation(Admin admin) {
        return admin.describeCluster().nodes();
    }


}
