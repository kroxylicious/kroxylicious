/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.serialization.Serdes;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.kroxylicious.filters.sasl.inspection.SaslInspection;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.proxy.testplugins.ClientAuthAwareLawyer;
import io.kroxylicious.proxy.testplugins.ClientAuthAwareLawyerFilter;
import io.kroxylicious.test.assertj.KafkaAssertions;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerConfig;
import io.kroxylicious.testing.kafka.common.SaslMechanism;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for the SASL Inspection Filter for SASL OAUTHBEARER (<a href="https://datatracker.ietf.org/doc/html/rfc7628">RFC-7628</a>.)
 * @see SaslInspectionIT
 */
class SaslInspectionOauthBearerIT extends BaseOauthBearerIT {

    @Test
    void shouldAuthenticateSuccessfully(@SaslMechanism(value = OAuthBearerLoginModule.OAUTHBEARER_MECHANISM) @BrokerConfig(name = "listener.name.external.oauthbearer.sasl.server.callback.handler.class", value = "org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler") @BrokerConfig(name = "listener.name.external.sasl.oauthbearer.jwt.validator.class", value = "org.apache.kafka.common.security.oauthbearer.BrokerJwtValidator") @BrokerConfig(name = "listener.name.external.sasl.oauthbearer.jwks.endpoint.url", value = JWKS_ENDPOINT_URL) @BrokerConfig(name = "listener.name.external.sasl.oauthbearer.expected.audience", value = EXPECTED_AUDIENCE) KafkaCluster cluster,
                                        Topic topic) {
        try (var tester = kroxyliciousTester(getConfiguredProxyBuilder(cluster));
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

    @Test
    void shouldFailAuthenticationWithBadToken(@SaslMechanism(value = OAuthBearerLoginModule.OAUTHBEARER_MECHANISM) @BrokerConfig(name = "listener.name.external.oauthbearer.sasl.server.callback.handler.class", value = "org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler") @BrokerConfig(name = "listener.name.external.sasl.oauthbearer.jwt.validator.class", value = "org.apache.kafka.common.security.oauthbearer.BrokerJwtValidator") @BrokerConfig(name = "listener.name.external.sasl.oauthbearer.jwks.endpoint.url", value = JWKS_ENDPOINT_URL) @BrokerConfig(name = "listener.name.external.sasl.oauthbearer.expected.audience", value = EXPECTED_AUDIENCE) KafkaCluster cluster,
                                              @TempDir Path tempdir)
            throws Exception {
        var badTokenFile = Files.createTempFile(tempdir, "badtoken", "b64");
        Files.writeString(badTokenFile, NO_KEYID_TOKEN);
        var config = getClientConfig(badTokenFile.toUri());

        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, JWKS_ENDPOINT_URL + "," + badTokenFile.toUri());

        try (var tester = kroxyliciousTester(getConfiguredProxyBuilder(cluster));
                var admin = tester.admin(config)) {

            assertThat(performClusterOperation(admin))
                    .failsWithin(10, TimeUnit.SECONDS)
                    .withThrowableOfType(ExecutionException.class)
                    .withCauseInstanceOf(SaslAuthenticationException.class)
                    .withMessageContaining("invalid_token");
        }

    }

    @Test
    void shouldFailAuthenticationWithLongSinceExpiredToken(@SaslMechanism(value = OAuthBearerLoginModule.OAUTHBEARER_MECHANISM) @BrokerConfig(name = "listener.name.external.oauthbearer.sasl.server.callback.handler.class", value = "org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler") @BrokerConfig(name = "listener.name.external.sasl.oauthbearer.jwt.validator.class", value = "org.apache.kafka.common.security.oauthbearer.BrokerJwtValidator") @BrokerConfig(name = "listener.name.external.sasl.oauthbearer.jwks.endpoint.url", value = JWKS_ENDPOINT_URL) @BrokerConfig(name = "listener.name.external.sasl.oauthbearer.expected.audience", value = EXPECTED_AUDIENCE) KafkaCluster cluster,
                                                           @TempDir Path tempdir)
            throws Exception {
        var badTokenFile = Files.createTempFile(tempdir, "expiredtoken", "b64");
        Files.writeString(badTokenFile, LONG_SINCE_EXPIRED_TOKEN);
        var config = getClientConfig(badTokenFile.toUri());

        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, JWKS_ENDPOINT_URL + "," + badTokenFile.toUri());

        try (var tester = kroxyliciousTester(getConfiguredProxyBuilder(cluster));
                var admin = tester.admin(config)) {

            assertThat(performClusterOperation(admin))
                    .failsWithin(10, TimeUnit.SECONDS)
                    .withThrowableOfType(ExecutionException.class)
                    .withCauseInstanceOf(SaslAuthenticationException.class)
                    .withMessageContaining("invalid_token");
        }
    }

    @Test
    @SuppressWarnings("java:S2925") // Can't test Kafka re-auth (KIP-368) without Thread#sleep.
    void shouldReauthenticateSuccessfully(@SaslMechanism(value = OAuthBearerLoginModule.OAUTHBEARER_MECHANISM) @BrokerConfig(name = "connections.max.reauth.ms", value = "5000") @BrokerConfig(name = "listener.name.external.oauthbearer.sasl.server.callback.handler.class", value = "org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler") @BrokerConfig(name = "listener.name.external.sasl.oauthbearer.jwt.validator.class", value = "org.apache.kafka.common.security.oauthbearer.BrokerJwtValidator") @BrokerConfig(name = "listener.name.external.sasl.oauthbearer.jwks.endpoint.url", value = JWKS_ENDPOINT_URL) @BrokerConfig(name = "listener.name.external.sasl.oauthbearer.expected.audience", value = EXPECTED_AUDIENCE) KafkaCluster cluster,
                                          Topic topic)
            throws Exception {

        var config = getConfiguredProxyBuilder(cluster);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(getProducerConfig());
                var consumer = tester
                        .consumer(Serdes.String(), Serdes.ByteArray(), getConsumerConfig())) {
            int batchNumOneBased = 1;
            while (batchNumOneBased <= 2) {
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

                recordHeaders.singleHeaderWithKey(ClientAuthAwareLawyerFilter.HEADER_KEY_CLIENT_SASL_AUTHORIZATION_ID)
                        .hasValueEqualTo(CLIENT_ID);
                recordHeaders.singleHeaderWithKey(ClientAuthAwareLawyerFilter.HEADER_KEY_CLIENT_SASL_MECH_NAME)
                        .hasValueEqualTo(OAuthBearerLoginModule.OAUTHBEARER_MECHANISM);

                if (batchNumOneBased < 2) {
                    Thread.sleep(10_000);
                }
                batchNumOneBased += 1;
            }
        }
    }

    private ConfigurationBuilder getConfiguredProxyBuilder(KafkaCluster cluster) {
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
