/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter.oauthbearer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.internals.secured.VerificationKeyResolverFactory;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.kroxylicious.proxy.BaseOauthBearerIT;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.proxy.testplugins.ClientAuthAwareLawyer;
import io.kroxylicious.proxy.testplugins.ClientAuthAwareLawyerFilter;
import io.kroxylicious.test.assertj.KafkaAssertions;
import io.kroxylicious.test.tester.SimpleMetricAssert;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerConfig;
import io.kroxylicious.testing.kafka.common.SaslMechanism;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.proxy.internal.util.Metrics.API_KEY_LABEL;
import static io.kroxylicious.proxy.internal.util.Metrics.NODE_ID_LABEL;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for OauthBearerValidation filter that uses wires the filter and Kafka Cluster to the
 * same test OAuth server.  It uses metric observations taken at the proxy to verify that the filter
 * is correctly protecting the broker from bad tokens.
 */
class OauthBearerValidationIT extends BaseOauthBearerIT {

    @SaslMechanism(value = OAuthBearerLoginModule.OAUTHBEARER_MECHANISM)
    @BrokerConfig(name = "listener.name.external.oauthbearer.sasl.server.callback.handler.class", value = "org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler")
    @BrokerConfig(name = "listener.name.external.sasl.oauthbearer.jwt.validator.class", value = "org.apache.kafka.common.security.oauthbearer.BrokerJwtValidator")
    @BrokerConfig(name = "listener.name.external.sasl.oauthbearer.jwks.endpoint.url", value = JWKS_ENDPOINT_URL)
    @BrokerConfig(name = "listener.name.external.sasl.oauthbearer.expected.audience", value = EXPECTED_AUDIENCE)
    protected KafkaCluster cluster;

    @AfterEach
    void afterEach() throws Exception {
        workaroundKafka17134();
    }

    private void workaroundKafka17134() throws Exception {
        // https://issues.apache.org/jira/browse/KAFKA-17134
        // Workaround for defect in Kafka where closed VerificationKeyResolvers get left in the
        // cache. This is impactful to the proxy because its config is constant (so cache hits).
        // The reason kafka broker doesn't suffer this itself is because the config is different between
        // clusters instances (port numbers, log dir etc. are different).
        var cacheField = VerificationKeyResolverFactory.class.getDeclaredField("CACHE");
        cacheField.setAccessible(true);
        ((Map<?, ?>) cacheField.get(null)).clear();
    }

    @Test
    void successfulAuthWithValidToken() {
        var config = getClientConfig(TOKEN_ENDPOINT_URL);

        try (var tester = kroxyliciousTester(getConfiguredProxyBuilder());
                var admin = tester.admin(config);
                var ahc = tester.getManagementClient()) {
            assertThat(performClusterOperation(admin))
                    .succeedsWithin(10, TimeUnit.SECONDS)
                    .isNotNull();

            var allMetrics = ahc.scrapeMetrics();

            SimpleMetricAssert.assertThat(allMetrics)
                    .withFailMessage("Expecting proxy to have seen at least one handshake request from the downstream")
                    .withUniqueMetric("kroxylicious_client_to_proxy_request_total", Map.of(
                            API_KEY_LABEL, ApiKeys.SASL_HANDSHAKE.name(),
                            NODE_ID_LABEL, "bootstrap"))
                    .value()
                    .isEqualTo(1);

            SimpleMetricAssert.assertThat(allMetrics)
                    .withFailMessage("Expecting proxy to have seen at least one handshake request to the upstream")
                    .withUniqueMetric("kroxylicious_proxy_to_server_request_total", Map.of(
                            API_KEY_LABEL, ApiKeys.SASL_HANDSHAKE.name(),
                            NODE_ID_LABEL, "bootstrap"))
                    .value()
                    .isEqualTo(1);

            SimpleMetricAssert.assertThat(allMetrics)
                    .withFailMessage("Expecting proxy to have seen at least one authenticate request from downstream")
                    .withUniqueMetric("kroxylicious_client_to_proxy_request_total", Map.of(
                            API_KEY_LABEL, ApiKeys.SASL_AUTHENTICATE.name(),
                            NODE_ID_LABEL, "bootstrap"))
                    .value()
                    .isEqualTo(1);

            SimpleMetricAssert.assertThat(allMetrics)
                    .withFailMessage("Expecting proxy to have seen at least one authenticate request to the upstream")
                    .withUniqueMetric("kroxylicious_proxy_to_server_request_total", Map.of(
                            API_KEY_LABEL, ApiKeys.SASL_AUTHENTICATE.name(),
                            NODE_ID_LABEL, "bootstrap"))
                    .value()
                    .isEqualTo(1);
        }
    }

    @Test
    void shouldProduceAndConsume(Topic topic) {
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

    @Test
    void authWithExpiredTokenIsNotForwardedToServer(@TempDir Path tempdir) throws Exception {
        var badTokenFile = Files.createTempFile(tempdir, "expiredtoken", "b64");
        Files.writeString(badTokenFile, LONG_SINCE_EXPIRED_TOKEN);
        var config = getClientConfig(badTokenFile.toUri());

        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, JWKS_ENDPOINT_URL + "," + badTokenFile.toUri());

        try (var tester = kroxyliciousTester(getConfiguredProxyBuilder());
                var admin = tester.admin(config);
                var ahc = tester.getManagementClient()) {

            assertThat(performClusterOperation(admin))
                    .failsWithin(10, TimeUnit.SECONDS)
                    .withThrowableOfType(ExecutionException.class)
                    .withCauseInstanceOf(SaslAuthenticationException.class)
                    .withMessageContaining("invalid_token");

            var allMetrics = ahc.scrapeMetrics();

            SimpleMetricAssert.assertThat(allMetrics)
                    .withFailMessage("Expecting proxy to have seen at least one handshake request from downstream")
                    .withUniqueMetric("kroxylicious_client_to_proxy_request_total", Map.of(
                            API_KEY_LABEL, ApiKeys.SASL_AUTHENTICATE.name(),
                            NODE_ID_LABEL, "bootstrap"))
                    .value()
                    .isEqualTo(1);

            SimpleMetricAssert.assertThat(allMetrics)
                    .withFailMessage("Expecting proxy to have seen no authenticate requests to the upstream")
                    .filterByName("kroxylicious_proxy_to_server_request_total")
                    .hasNoMetricMatching("kroxylicious_proxy_to_server_request_total", Map.of(
                            API_KEY_LABEL, ApiKeys.SASL_AUTHENTICATE.name(),
                            NODE_ID_LABEL, "bootstrap"));
        }

    }

    private ConfigurationBuilder getConfiguredProxyBuilder() {
        NamedFilterDefinition oauthValidationFilter = new NamedFilterDefinitionBuilder(
                "oauth",
                OauthBearerValidation.class.getName())
                .withConfig("jwksEndpointUrl", JWKS_ENDPOINT_URL,
                        "expectedAudience", EXPECTED_AUDIENCE)
                .build();
        NamedFilterDefinition lawyer = new NamedFilterDefinitionBuilder(
                ClientAuthAwareLawyer.class.getName(),
                ClientAuthAwareLawyer.class.getName())
                .build();

        return proxy(cluster)
                .withNewManagement()
                .withNewEndpoints()
                .withNewPrometheus()
                .endPrometheus()
                .endEndpoints()
                .endManagement()
                .addToFilterDefinitions(oauthValidationFilter, lawyer)
                .addToDefaultFilters(oauthValidationFilter.name(), lawyer.name());
    }

    /**
     * Pings the cluster in order to assert connectivity. We don't care about the result.
     * @param admin admin
     */
    private KafkaFuture<?> performClusterOperation(Admin admin) {
        return admin.describeCluster().nodes();
    }

}
