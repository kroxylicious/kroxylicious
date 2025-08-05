/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter.oauthbearer;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junitpioneer.jupiter.RestoreSystemProperties;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.proxy.testplugins.ClientAuthAwareLawyerFilter;
import io.kroxylicious.proxy.testplugins.ClientTlsAwareLawyer;
import io.kroxylicious.test.assertj.KafkaAssertions;
import io.kroxylicious.test.tester.SimpleMetric;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerConfig;
import io.kroxylicious.testing.kafka.common.SaslMechanism;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;

/**
 * Integration test for OauthBearerValidation filter that uses wires the filter and Kafka Cluster to the
 * same test OAuth server.  It uses metric observations taken at the proxy to verify that the filter
 * is correctly protecting the broker from bad tokens.
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
@EnabledIf(value = "isDockerAvailable", disabledReason = "docker unavailable")
@RestoreSystemProperties
class OauthBearerValidationIT {

    private static final DockerImageName DOCKER_IMAGE_NAME = DockerImageName.parse("ghcr.io/navikt/mock-oauth2-server:2.2.1");

    private static final int OAUTH_SERVER_PORT = 28089;
    private static final String JWKS_ENDPOINT_URL = "http://localhost:" + OAUTH_SERVER_PORT + "/default/jwks";
    private static final URI OAUTH_ENDPOINT_URL = URI.create(JWKS_ENDPOINT_URL).resolve("/");
    private static final URI TOKEN_ENDPOINT_URL = OAUTH_ENDPOINT_URL.resolve("default/token");
    private static final String EXPECTED_AUDIENCE = "default";
    private static final String BAD_TOKEN = "eyJraWQiOiJkZWZhdWx0IiwidHlwIjoiSldUIiwiYWxnIjoiUlMyNTYifQ."
            + "eyJzdWIiOiJjbGllbnRJZElnbm9yZSIsImF1ZCI6ImRlZmF1bHQiLCJuYmYiOjE3MjA3MjIyOTAsImF6cCI6ImNsaWVudElkSWdub3JlIiwiaXNzIjoiaHR0cDovL2xvY2FsaG9zdDoyODA4OS9kZWZhdWx0IiwiZXhwIjoxNzIwNzI1ODkwLCJpYXQiOjE3MjA3MjIyOTAsImp0aSI6IjE0MGZjMmFhLWRjZmQtNDE1Mi05MWJmLWQyMDZiM2M1MzAxZiIsInRpZCI6ImRlZmF1bHQifQ."
            + "I4LUI4Lp6XwUzD-UN2LDRfiNPCvhHDQJVH_LCE4gkuY5UxrRMJ8H3C9408zmjGPRChWmKU4aRIy8rtSPbfRmDLenoM91dr1mDy8B01TZohEOACnAxBSvsN73-cNUUvaRzZmMeUkGbmgEtGhqZ2d3MELe7bm0fEyuNRyM9fv-AahGm551hchMe3bzeYjbcBKuatKYoiHuPTX_HuNF4AAwI_vz_lYzHliKDmPRJwpsaMUaCtsfUKbSzpRPe6X2FWWkkgOLtCD-W14sp3r8z2KrHryH_ILz2MtPvIlvmkJE8U0CRaVyQ-6L2sL-iUdXoUGTHmV384X2R-cy5gKFhN3Ibg"; // notsecret
    private static final String KROXYLICIOUS_PAYLOAD_SIZE_BYTES_COUNT_METRIC = "kroxylicious_payload_size_bytes_count";
    private static final Predicate<SimpleMetric> UPSTREAM_SASL_HANDSHAKE_LABELS_PREDICATE = m -> m.name().equals(KROXYLICIOUS_PAYLOAD_SIZE_BYTES_COUNT_METRIC)
            && m.labels().entrySet().containsAll(
                    Map.of("ApiKey", "SASL_HANDSHAKE", "flowing", "upstream").entrySet());
    private static final Predicate<SimpleMetric> DOWNSTREAM_SASL_AUTHENTICATE_PREDICATE = m -> m.name().equals(KROXYLICIOUS_PAYLOAD_SIZE_BYTES_COUNT_METRIC)
            && m.labels().entrySet().containsAll(
                    Map.of("ApiKey", "SASL_AUTHENTICATE", "flowing", "downstream").entrySet());
    private static final String ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG = "org.apache.kafka.sasl.oauthbearer.allowed.urls";
    @SaslMechanism(value = OAuthBearerLoginModule.OAUTHBEARER_MECHANISM)
    @BrokerConfig(name = "listener.name.external.sasl.oauthbearer.jwks.endpoint.url", value = JWKS_ENDPOINT_URL)
    @BrokerConfig(name = "listener.name.external.sasl.oauthbearer.expected.audience", value = EXPECTED_AUDIENCE)
    @BrokerConfig(name = "listener.name.external.oauthbearer.sasl.server.callback.handler.class", value = "org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler")
    KafkaCluster cluster;
    private static OauthServerContainer oauthServer;

    @BeforeAll
    static void beforeAll() {
        // Kafka 4.0 requires that the org.apache.kafka.sasl.oauthbearer.allowed.urls sys property is set in order to use Oauth Bearer.
        // The Kafka Broker and Proxy requires that JWKS_ENDPOINT_URL is in the allow list.
        // The Kafka Client requires that TOKEN_ENDPOINT_URL is in the allow list.
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG, JWKS_ENDPOINT_URL + "," + TOKEN_ENDPOINT_URL);

        oauthServer = new OauthServerContainer(OauthBearerValidationIT.DOCKER_IMAGE_NAME);
        oauthServer.setWaitStrategy(new LogMessageWaitStrategy().withRegEx(".*started server on address.*"));
        oauthServer.addFixedExposedPort(OAUTH_SERVER_PORT, OAUTH_SERVER_PORT);
        oauthServer.withEnv("SERVER_PORT", OAUTH_SERVER_PORT + "");
        oauthServer.withEnv("LOG_LEVEL", "DEBUG"); // required to for the startup message to be logged.
        oauthServer.start();
    }

    @AfterAll
    static void afterAll() {
        if (oauthServer != null) {
            oauthServer.close();
        }
    }

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
        var cacheField = OAuthBearerValidatorCallbackHandler.class.getDeclaredField("VERIFICATION_KEY_RESOLVER_CACHE");
        cacheField.setAccessible(true);
        ((Map<?, ?>) cacheField.get(null)).clear();
    }

    @Test
    void successfulAuthWithValidToken(Topic topic) {
        var config = getClientConfig(TOKEN_ENDPOINT_URL);

        try (var tester = kroxyliciousTester(getConfiguredProxyBuilder());
                var admin = tester.admin(config);
                var ahc = tester.getManagementClient()) {
            assertThat(performClusterOperation(admin))
                    .succeedsWithin(10, TimeUnit.SECONDS)
                    .isNotNull();

            var allMetrics = ahc.scrapeMetrics();

            var saslHandshakeRequestsGoingUpCount = findFirstMetricMatching(allMetrics, UPSTREAM_SASL_HANDSHAKE_LABELS_PREDICATE);
            var saslAuthenticationResponsesComingDown = findFirstMetricMatching(allMetrics, DOWNSTREAM_SASL_AUTHENTICATE_PREDICATE);

            assertThat(saslHandshakeRequestsGoingUpCount)
                    .isPresent()
                    .get(DOUBLE)
                    .withFailMessage("Expecting proxy to have seen at least two handshake requests (one for metadata, one for broker) from downstream")
                    .isGreaterThanOrEqualTo(2);

            assertThat(saslAuthenticationResponsesComingDown)
                    .isPresent()
                    .get(DOUBLE)
                    .withFailMessage("Expecting proxy to have seen at the same number of authentication responses from the broker as were handshake requests")
                    .isEqualTo(saslHandshakeRequestsGoingUpCount.get());
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
                    .hasValueEqualTo("clientIdIgnore");
        }
    }

    @Test
    void authWithBadToken(@TempDir Path tempdir) throws Exception {
        var badTokenFile = Files.createTempFile(tempdir, "badtoken", "b64");
        Files.writeString(badTokenFile, BAD_TOKEN);
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

            var saslHandshakeRequestsGoingUpCount = findFirstMetricMatching(allMetrics, UPSTREAM_SASL_HANDSHAKE_LABELS_PREDICATE);
            var saslAuthenticationResponsesComingDown = findFirstMetricMatching(allMetrics, DOWNSTREAM_SASL_AUTHENTICATE_PREDICATE);

            assertThat(saslHandshakeRequestsGoingUpCount)
                    .isPresent()
                    .get(DOUBLE)
                    .withFailMessage("Expecting proxy to have seen at least one handshake requests from downstream")
                    .isPositive();

            assertThat(saslAuthenticationResponsesComingDown).isNotPresent();
        }

    }

    private Optional<Double> findFirstMetricMatching(List<SimpleMetric> all, Predicate<SimpleMetric> predicate) {
        return all.stream()
                .filter(predicate)
                .findFirst()
                .map(SimpleMetric::value);
    }

    private ConfigurationBuilder getConfiguredProxyBuilder() {
        NamedFilterDefinition oauthValidationFilter = new NamedFilterDefinitionBuilder(
                "oauth",
                OauthBearerValidation.class.getName())
                .withConfig("jwksEndpointUrl", JWKS_ENDPOINT_URL,
                        "expectedAudience", EXPECTED_AUDIENCE)
                .build();
        NamedFilterDefinition lawyer = new NamedFilterDefinitionBuilder(
                ClientTlsAwareLawyer.class.getName(),
                ClientTlsAwareLawyer.class.getName())
                .build();

        // recordHeaders.singleHeaderWithKey(ClientAuthAwareLawyerFilter.HEADER_KEY_CLIENT_SASL_AUTHORIZATION_ID)
        // .hasValueEqualTo("alice");
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

    @NonNull
    private Map<String, Object> getProducerConfig() {
        Map<String, Object> clientConfig = getClientConfig(OauthBearerValidationIT.TOKEN_ENDPOINT_URL);
        clientConfig.put(CommonClientConfigs.CLIENT_ID_CONFIG, "producer");
        return clientConfig;
    }

    @NonNull
    private Map<String, Object> getConsumerConfig() {
        Map<String, Object> clientConfig = getClientConfig(OauthBearerValidationIT.TOKEN_ENDPOINT_URL);
        clientConfig.put(CommonClientConfigs.CLIENT_ID_CONFIG, "consumer");
        clientConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "my-consumer-group");
        clientConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return clientConfig;
    }

    private Map<String, Object> getClientConfig(URI tokenEndpointUrl) {
        var oauthClientConfig = new HashMap<String, Object>();
        oauthClientConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name());
        oauthClientConfig.put(SaslConfigs.SASL_MECHANISM, OAuthBearerLoginModule.OAUTHBEARER_MECHANISM);
        oauthClientConfig.put(SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, tokenEndpointUrl.toString());
        oauthClientConfig.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, OAuthBearerLoginCallbackHandler.class.getCanonicalName());
        oauthClientConfig.put(SaslConfigs.SASL_JAAS_CONFIG,
                "%s required clientId=\"clientIdIgnore\" clientSecret=\"clientSecretIgnore\";".formatted(OAuthBearerLoginModule.class.getName()));
        return oauthClientConfig;
    }

    /**
     * Pings the cluster in order to assert connectivity. We don't care about the result.
     * @param admin admin
     */
    private KafkaFuture<?> performClusterOperation(Admin admin) {
        return admin.describeCluster().nodes();
    }

    private static class OauthServerContainer extends GenericContainer<OauthServerContainer> {
        private OauthServerContainer(DockerImageName dockerImageName) {
            super(dockerImageName);
        }

        @Override
        protected void addFixedExposedPort(int hostPort, int containerPort) {
            super.addFixedExposedPort(hostPort, containerPort);
        }
    }

    static boolean isDockerAvailable() {
        return DockerClientFactory.instance().isDockerAvailable();
    }

}
