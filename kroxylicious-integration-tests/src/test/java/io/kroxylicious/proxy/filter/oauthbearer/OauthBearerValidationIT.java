/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter.oauthbearer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.FilterDefinitionBuilder;
import io.kroxylicious.test.tester.AdminHttpClient;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerConfig;
import io.kroxylicious.testing.kafka.common.SaslMechanism;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;

/**
 * Integration test for OauthBearerValidation filter that uses wires the filter and Kafka Cluster to the
 * same test OAuth server.  It uses metric observations taken at the proxy to verify that the filter
 * is correctly protecting the broker from bad tokens.
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class OauthBearerValidationIT {

    private static final DockerImageName DOCKER_IMAGE_NAME = DockerImageName.parse("ghcr.io/navikt/mock-oauth2-server").withTag("2.1.8");
    private static final String JWKS_ENDPOINT_URL = "http://localhost:28089/default/jwks";
    private static final URI OAUTH_ENDPOINT_URL = URI.create(JWKS_ENDPOINT_URL).resolve("/");
    private static final URI TOKEN_ENDPOINT_URL = OAUTH_ENDPOINT_URL.resolve("default/token");
    private static final String EXPECTED_AUDIENCE = "default";
    private static final String BAD_TOKEN = "eyJraWQiOiJkZWZhdWx0IiwidHlwIjoiSldUIiwiYWxnIjoiUlMyNTYifQ."
            + "eyJzdWIiOiJjbGllbnRJZElnbm9yZSIsImF1ZCI6ImRlZmF1bHQiLCJuYmYiOjE3MjA3MjIyOTAsImF6cCI6ImNsaWVudElkSWdub3JlIiwiaXNzIjoiaHR0cDovL2xvY2FsaG9zdDoyODA4OS9kZWZhdWx0IiwiZXhwIjoxNzIwNzI1ODkwLCJpYXQiOjE3MjA3MjIyOTAsImp0aSI6IjE0MGZjMmFhLWRjZmQtNDE1Mi05MWJmLWQyMDZiM2M1MzAxZiIsInRpZCI6ImRlZmF1bHQifQ."
            + "I4LUI4Lp6XwUzD-UN2LDRfiNPCvhHDQJVH_LCE4gkuY5UxrRMJ8H3C9408zmjGPRChWmKU4aRIy8rtSPbfRmDLenoM91dr1mDy8B01TZohEOACnAxBSvsN73-cNUUvaRzZmMeUkGbmgEtGhqZ2d3MELe7bm0fEyuNRyM9fv-AahGm551hchMe3bzeYjbcBKuatKYoiHuPTX_HuNF4AAwI_vz_lYzHliKDmPRJwpsaMUaCtsfUKbSzpRPe6X2FWWkkgOLtCD-W14sp3r8z2KrHryH_ILz2MtPvIlvmkJE8U0CRaVyQ-6L2sL-iUdXoUGTHmV384X2R-cy5gKFhN3Ibg";
    private static final String KROXYLICIOUS_PAYLOAD_SIZE_BYTES_COUNT_METRIC = "kroxylicious_payload_size_bytes_count"; // notsecret
    private static final Predicate<SimpleMetric> UPSTREAM_SASL_HANDSHAKE_LABELS_PREDICATE = m -> m.name().equals(KROXYLICIOUS_PAYLOAD_SIZE_BYTES_COUNT_METRIC)
            && m.labels().entrySet().containsAll(
                    Map.of("ApiKey", "SASL_HANDSHAKE", "flowing", "upstream").entrySet());
    private static final Predicate<SimpleMetric> DOWNSTREAM_SASL_AUTHENTICATE_PREDICATE = m -> m.name().equals(KROXYLICIOUS_PAYLOAD_SIZE_BYTES_COUNT_METRIC)
            && m.labels().entrySet().containsAll(
                    Map.of("ApiKey", "SASL_AUTHENTICATE", "flowing", "downstream").entrySet());
    private static final String METRICS = "metrics";
    @SaslMechanism(value = OAuthBearerLoginModule.OAUTHBEARER_MECHANISM)
    @BrokerConfig(name = "listener.name.external.sasl.oauthbearer.jwks.endpoint.url", value = JWKS_ENDPOINT_URL)
    @BrokerConfig(name = "listener.name.external.sasl.oauthbearer.expected.audience", value = EXPECTED_AUDIENCE)
    @BrokerConfig(name = "listener.name.external.oauthbearer.sasl.server.callback.handler.class", value = "org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler")
    KafkaCluster cluster;
    private static OauthServerContainer oauthServer;

    @BeforeAll
    public static void beforeAll() {
        assumeThat(DockerClientFactory.instance().isDockerAvailable()).withFailMessage("docker unavailable").isTrue();

        int port = OAUTH_ENDPOINT_URL.getPort();
        oauthServer = new OauthServerContainer(OauthBearerValidationIT.DOCKER_IMAGE_NAME);
        oauthServer.setWaitStrategy(new LogMessageWaitStrategy().withRegEx(".*started server on address.*"));
        oauthServer.addFixedExposedPort(port, port);
        oauthServer.withEnv("SERVER_PORT", port + "");
        oauthServer.start();
    }

    @AfterAll
    public static void afterAll() {
        if (oauthServer != null) {
            oauthServer.close();
        }
    }

    @AfterEach
    public void afterEach() throws Exception {
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
    void successfulAuthWithValidToken() {
        var config = getClientConfig(TOKEN_ENDPOINT_URL);

        try (var tester = kroxyliciousTester(getConfiguredProxyBuilder());
                var admin = tester.admin(config)) {
            performClusterOperation(admin);

            var text = AdminHttpClient.INSTANCE.getFromAdminEndpoint(METRICS).body();
            var allMetrics = SimpleMetric.parse(text);

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
    void authWithBadToken() throws Exception {
        var badTokenFile = Files.createTempFile("badtoken", "b64");
        Files.writeString(badTokenFile, BAD_TOKEN);
        var config = getClientConfig(badTokenFile.toUri());

        try (var tester = kroxyliciousTester(getConfiguredProxyBuilder());
                var admin = tester.admin(config)) {
            assertThatThrownBy(() -> performClusterOperation(admin))
                    .hasCauseInstanceOf(SaslAuthenticationException.class)
                    .hasMessageContaining("invalid_token");

            var text = AdminHttpClient.INSTANCE.getFromAdminEndpoint(METRICS).body();
            var allMetrics = SimpleMetric.parse(text);

            var saslHandshakeRequestsGoingUpCount = findFirstMetricMatching(allMetrics, UPSTREAM_SASL_HANDSHAKE_LABELS_PREDICATE);
            var saslAuthenticationResponsesComingDown = findFirstMetricMatching(allMetrics, DOWNSTREAM_SASL_AUTHENTICATE_PREDICATE);

            assertThat(saslHandshakeRequestsGoingUpCount)
                    .isPresent()
                    .get(DOUBLE)
                    .withFailMessage("Expecting proxy to have seen at least one handshake requests from downstream")
                    .isPositive();

            assertThat(saslAuthenticationResponsesComingDown).isNotPresent();
        }
        finally {
            @SuppressWarnings("java:S1481")
            var ignored = badTokenFile.toFile().delete();
        }

    }

    private Optional<Double> findFirstMetricMatching(List<SimpleMetric> all, Predicate<SimpleMetric> predicate) {
        return all.stream()
                .filter(predicate)
                .findFirst()
                .map(SimpleMetric::value);
    }

    private ConfigurationBuilder getConfiguredProxyBuilder() {
        return proxy(cluster)
                .withNewAdminHttp()
                .withNewEndpoints()
                .withNewPrometheus()
                .endPrometheus()
                .endEndpoints()
                .endAdminHttp()
                .addToFilters(new FilterDefinitionBuilder(
                        OauthBearerValidation.class.getName())
                        .withConfig("jwksEndpointUrl", JWKS_ENDPOINT_URL,
                                "expectedAudience", EXPECTED_AUDIENCE)
                        .build());
    }

    @NonNull
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
    @SuppressWarnings("java:S1481") // making clear the intent that the result of the operation is unneeded.
    private void performClusterOperation(Admin admin) {
        var unused = admin.describeCluster().nodes().toCompletionStage().toCompletableFuture().join();
        assertThat(unused).isNotNull();
    }

    private static class OauthServerContainer extends GenericContainer<OauthServerContainer> {
        private OauthServerContainer(DockerImageName dockerImageName) {
            super(dockerImageName);
        }

        protected void addFixedExposedPort(int hostPort, int containerPort) {
            super.addFixedExposedPort(hostPort, containerPort);
        }
    }

    private record SimpleMetric(String name, Map<String, String> labels, double value) {

        // https://github.com/prometheus/docs/blob/main/content/docs/instrumenting/exposition_formats.md
        // note: RE doesn't handle escaping within label values
        static Pattern PROM_TEXT_EXPOSITION_PATTERN = Pattern
                .compile("^(?<metric>[a-zA-Z_:][a-zA-Z0-9_:]*]*)(\\{(?<labels>.*)})?[\\t ]*(?<value>[0-9E.]*)[\\t ]*(?<timestamp>[0-9]+)?$");
        static Pattern NAME_WITH_QUOTED_VALUE = Pattern.compile("^(?<name>[a-zA-Z_:][a-zA-Z0-9_:]*)=\"(?<value>.*)\"$");
        static List<SimpleMetric> parse(String output) {
            var all = new ArrayList<SimpleMetric>();
            try (var reader = new BufferedReader(new StringReader(output))) {
                var line = reader.readLine();
                while (line != null) {
                    if (!(line.startsWith("#") || line.isEmpty())) {
                        var matched = PROM_TEXT_EXPOSITION_PATTERN.matcher(line);
                        if (!matched.matches()) {
                            throw new IllegalArgumentException("Failed to parse metric %s".formatted(line));
                        }
                        var metricName = matched.group("metric");
                        var metricValue = Double.parseDouble(matched.group("value"));
                        var metricLabels = matched.group("labels");
                        var labels = labelsToMap(metricLabels);

                        all.add(new SimpleMetric(metricName, labels, metricValue));
                    }
                    line = reader.readLine();
                }
                return all;
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to parse metrics", e);
            }
        }

        @NonNull
        private static Map<String, String> labelsToMap(String metricLabels) {
            var splitLabels = metricLabels.split(",");
            return Arrays.stream(splitLabels)
                    .map(nv -> NAME_WITH_QUOTED_VALUE.matcher(nv))
                    .filter(Matcher::matches)
                    .collect(Collectors.toMap(nv -> nv.group("name"), nv -> nv.group("value")));
        }
    }
}
