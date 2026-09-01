/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import io.kroxylicious.kafka.common.message.MetadataRequestData;
import io.kroxylicious.kafka.common.message.MetadataResponseData;
import io.kroxylicious.kafka.common.message.SaslAuthenticateRequestData;
import io.kroxylicious.kafka.common.message.SaslAuthenticateResponseData;
import io.kroxylicious.kafka.common.message.SaslHandshakeRequestData;
import io.kroxylicious.kafka.common.message.SaslHandshakeResponseData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import io.kroxylicious.kafka.common.protocol.Errors;
import org.apache.kafka.common.security.oauthbearer.internals.secured.VerificationKeyResolverFactory;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.jose4j.jwk.PublicJsonWebKey;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwt.JwtClaims;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.filter.sasl.termination.SaslTermination;
import io.kroxylicious.it.testplugins.ClientAuthAwareLawyer;
import io.kroxylicious.it.testplugins.ClientAuthAwareLawyerFilter;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.scram.credentialstore.file.ScramCredentialFileManager;
import io.kroxylicious.scram.credentialstore.file.ScramCredentialFileService;
import io.kroxylicious.testing.filter.assertj.KafkaAssertions;
import io.kroxylicious.testing.filter.jws.JwsTestUtils;
import io.kroxylicious.testing.integration.Request;
import io.kroxylicious.testing.integration.client.KafkaClient;
import io.kroxylicious.testing.integration.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.testing.integration.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for SASL termination filter with OAUTHBEARER mechanism.
 * <p>
 * Tests authenticate against a real OAuth server (mock-oauth2-server) and verify
 * that JWT validation is correctly enforced: audience, issuer, expiry, key ID,
 * and signature verification.
 * </p>
 */
@EnabledIf(value = "isDockerAvailable", disabledReason = "docker unavailable")
class SaslTerminationOauthBearerIT extends BaseOauthBearerIT {

    @BeforeAll
    static void removeJwksUrlsToVerifyProductionCodeAddsThem() {
        // The base class pre-populates JWKS URLs for the in-VM test broker (see BaseOauthBearerIT).
        // Strip them here so these tests verify that the SaslTermination filter's production code
        // adds JWKS URLs to the allowed list itself. Token endpoint URLs are kept for the Kafka client.
        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG,
                TOKEN_ENDPOINT_URL + "," + TOKEN_ENDPOINT_URL_OTHER_ISSUER);
    }

    private static final String TEST_USERNAME = "alice";
    private static final String TEST_PASSWORD = "alice-secret-password-123";
    private static final String KEYSTORE_PASSWORD = "keystore-password-secret-456";

    KafkaCluster cluster;

    @AfterEach
    @SuppressWarnings("java:S3011")
    void afterEach() throws Exception {
        // https://issues.apache.org/jira/browse/KAFKA-17134
        var cacheField = VerificationKeyResolverFactory.class.getDeclaredField("CACHE");
        cacheField.setAccessible(true);
        ((Map<?, ?>) cacheField.get(null)).clear();
    }

    @Test
    void shouldAuthenticateWithValidToken() {
        // Given
        var config = proxy(cluster)
                .addToFilterDefinitions(createOauthTerminationFilter())
                .addToDefaultFilters(SaslTermination.class.getSimpleName());

        // When
        try (var tester = kroxyliciousTester(config);
                var admin = tester.admin(getClientConfig(TOKEN_ENDPOINT_URL))) {

            // Then
            assertThat(admin.describeCluster().nodes())
                    .succeedsWithin(10, TimeUnit.SECONDS)
                    .isNotNull();
        }
    }

    @Test
    void shouldProduceAndConsumeWithValidToken(Topic topic) {
        // Given
        var lawyer = new NamedFilterDefinitionBuilder(
                ClientAuthAwareLawyer.class.getName(),
                ClientAuthAwareLawyer.class.getName())
                .build();

        var config = proxy(cluster)
                .addToFilterDefinitions(createOauthTerminationFilter(), lawyer)
                .addToDefaultFilters(SaslTermination.class.getSimpleName(), lawyer.name());

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(getProducerConfig());
                var consumer = tester.consumer(getConsumerConfig())) {

            // When
            assertThat(producer.send(new ProducerRecord<>(topic.name(), "my-key", "my-value")))
                    .succeedsWithin(Duration.ofSeconds(5));

            consumer.subscribe(Set.of(topic.name()));
            var records = consumer.poll(Duration.ofSeconds(10));

            // Then
            assertThat(records).hasSize(1);
            var recordHeaders = assertThat(records.records(topic.name()))
                    .as("topic %s records", topic.name())
                    .singleElement()
                    .asInstanceOf(new InstanceOfAssertFactory<>(ConsumerRecord.class, KafkaAssertions::assertThat))
                    .headers();

            recordHeaders.singleHeaderWithKey(ClientAuthAwareLawyerFilter.HEADER_KEY_CLIENT_SASL_CONTEXT_PRESENT)
                    .hasByteValueSatisfying(val -> assertThat(val).isEqualTo(ClientAuthAwareLawyerFilter.trueValue()));

            recordHeaders.singleHeaderWithKey(ClientAuthAwareLawyerFilter.HEADER_KEY_CLIENT_SASL_AUTHORIZATION_ID)
                    .hasValueEqualTo(CLIENT_ID);
        }
    }

    @Test
    void shouldRejectTokenWithWrongAudience() {
        // Given
        var config = proxy(cluster)
                .addToFilterDefinitions(createOauthTerminationFilter())
                .addToDefaultFilters(SaslTermination.class.getSimpleName());

        // Token from /other-issuer has aud:"other-issuer", but filter expects aud:"default"
        var clientConfig = getClientConfig(TOKEN_ENDPOINT_URL_OTHER_ISSUER);

        // When/Then
        try (var tester = kroxyliciousTester(config);
                var admin = tester.admin(clientConfig)) {
            assertThat(admin.describeCluster().nodes())
                    .failsWithin(10, TimeUnit.SECONDS)
                    .withThrowableOfType(ExecutionException.class)
                    .withCauseInstanceOf(SaslAuthenticationException.class);
        }
    }

    @Test
    void shouldRejectTokenWithWrongIssuer() {
        // Given — filter validates against /other-issuer JWKS (so signature is valid)
        // but expects issuer "http://localhost:<port>/default"
        var filter = createOauthTerminationFilterWithConfig(
                JWKS_ENDPOINT_URL_OTHER_ISSUER,
                "other-issuer",
                EXPECTED_ISSUER);

        var config = proxy(cluster)
                .addToFilterDefinitions(filter)
                .addToDefaultFilters(SaslTermination.class.getSimpleName());

        // Token from /other-issuer has iss:"http://localhost:<port>/other-issuer"
        var clientConfig = getClientConfig(TOKEN_ENDPOINT_URL_OTHER_ISSUER);

        // When/Then
        try (var tester = kroxyliciousTester(config);
                var admin = tester.admin(clientConfig)) {
            assertThat(admin.describeCluster().nodes())
                    .failsWithin(10, TimeUnit.SECONDS)
                    .withThrowableOfType(ExecutionException.class)
                    .withCauseInstanceOf(SaslAuthenticationException.class);
        }
    }

    @Test
    void shouldRejectExpiredToken(@TempDir Path tempDir) throws Exception {
        // Given
        var badTokenFile = Files.createTempFile(tempDir, "expiredtoken", "b64");
        Files.writeString(badTokenFile, LONG_SINCE_EXPIRED_TOKEN);

        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG,
                System.getProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG) + "," + badTokenFile.toUri());

        var config = proxy(cluster)
                .addToFilterDefinitions(createOauthTerminationFilter())
                .addToDefaultFilters(SaslTermination.class.getSimpleName());

        var clientConfig = getClientConfig(badTokenFile.toUri());

        // When/Then
        try (var tester = kroxyliciousTester(config);
                var admin = tester.admin(clientConfig)) {
            assertThat(admin.describeCluster().nodes())
                    .failsWithin(10, TimeUnit.SECONDS)
                    .withThrowableOfType(ExecutionException.class)
                    .withCauseInstanceOf(SaslAuthenticationException.class);
        }
    }

    @Test
    void shouldRejectTokenWithNoKeyId(@TempDir Path tempDir) throws Exception {
        // Given
        var badTokenFile = Files.createTempFile(tempDir, "nokidtoken", "b64");
        Files.writeString(badTokenFile, NO_KEYID_TOKEN);

        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG,
                System.getProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG) + "," + badTokenFile.toUri());

        var config = proxy(cluster)
                .addToFilterDefinitions(createOauthTerminationFilter())
                .addToDefaultFilters(SaslTermination.class.getSimpleName());

        var clientConfig = getClientConfig(badTokenFile.toUri());

        // When/Then
        try (var tester = kroxyliciousTester(config);
                var admin = tester.admin(clientConfig)) {
            assertThat(admin.describeCluster().nodes())
                    .failsWithin(10, TimeUnit.SECONDS)
                    .withThrowableOfType(ExecutionException.class)
                    .withCauseInstanceOf(SaslAuthenticationException.class);
        }
    }

    @Test
    void shouldRejectTokenSignedByUnknownKey(@TempDir Path tempDir) throws Exception {
        // Given — craft a JWT with valid claims but signed by a key not in the mock server's JWKS
        PublicJsonWebKey rsaKey = (PublicJsonWebKey) JwsTestUtils.RSA_SIGN_JWKS.getJsonWebKeys().get(0);

        JwtClaims claims = new JwtClaims();
        claims.setSubject(CLIENT_ID);
        claims.setAudience(EXPECTED_AUDIENCE);
        claims.setIssuer(EXPECTED_ISSUER);
        claims.setExpirationTimeMinutesInTheFuture(60);
        claims.setIssuedAtToNow();

        JsonWebSignature jws = new JsonWebSignature();
        jws.setKeyIdHeaderValue(rsaKey.getKeyId());
        jws.setAlgorithmHeaderValue(AlgorithmIdentifiers.RSA_USING_SHA256);
        jws.setKey(rsaKey.getPrivateKey());
        jws.setPayload(claims.toJson());

        var badTokenFile = Files.createTempFile(tempDir, "unknownkey", "b64");
        Files.writeString(badTokenFile, jws.getCompactSerialization());

        System.setProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG,
                System.getProperty(ALLOWED_SASL_OAUTHBEARER_URLS_CONFIG) + "," + badTokenFile.toUri());

        var config = proxy(cluster)
                .addToFilterDefinitions(createOauthTerminationFilter())
                .addToDefaultFilters(SaslTermination.class.getSimpleName());

        var clientConfig = getClientConfig(badTokenFile.toUri());

        // When/Then
        try (var tester = kroxyliciousTester(config);
                var admin = tester.admin(clientConfig)) {
            assertThat(admin.describeCluster().nodes())
                    .failsWithin(10, TimeUnit.SECONDS)
                    .withThrowableOfType(ExecutionException.class)
                    .withCauseInstanceOf(SaslAuthenticationException.class);
        }
    }

    @SuppressWarnings("java:S2925") // Can't test Kafka re-auth (KIP-368) without Thread#sleep
    @Test
    void shouldRejectExpiredSessionWithoutReauthentication() throws Exception {
        // Given
        Duration maxTimeBeforeReauth = Duration.ofSeconds(2);
        var config = proxy(cluster)
                .addToFilterDefinitions(createOauthTerminationFilterWithReauth(maxTimeBeforeReauth))
                .addToDefaultFilters(SaslTermination.class.getSimpleName());

        String accessToken = getAccessToken();

        try (var tester = kroxyliciousTester(config)) {
            String bootstrapAddress = tester.getBootstrapAddress();
            String[] hostPort = bootstrapAddress.split(":"); // NOPMD
            try (var client = new KafkaClient(hostPort[0], Integer.parseInt(hostPort[1]))) {
                var handshakeResponse = (SaslHandshakeResponseData) client.getSync(getRequest(
                        ApiKeys.SASL_HANDSHAKE.latestVersion(),
                        new SaslHandshakeRequestData().setMechanism("OAUTHBEARER")))
                        .payload().message();
                assertThat(Errors.forCode(handshakeResponse.errorCode())).isEqualTo(Errors.NONE);

                byte[] authBytes = ("n,,auth=Bearer " + accessToken + "").getBytes(StandardCharsets.UTF_8);
                var authenticateResponse = (SaslAuthenticateResponseData) client.getSync(getRequest(
                        ApiKeys.SASL_AUTHENTICATE.latestVersion(),
                        new SaslAuthenticateRequestData().setAuthBytes(authBytes)))
                        .payload().message();
                assertThat(Errors.forCode(authenticateResponse.errorCode())).isEqualTo(Errors.NONE);

                // When
                Thread.sleep(maxTimeBeforeReauth.plusSeconds(1).toMillis());

                var metadataResponse = (MetadataResponseData) client.getSync(getRequest(
                        ApiKeys.METADATA.latestVersion(),
                        new MetadataRequestData()))
                        .payload().message();

                // Then
                assertThat(Errors.forCode(metadataResponse.errorCode()))
                        .isEqualTo(Errors.SASL_AUTHENTICATION_FAILED);
            }
        }
    }

    @SuppressWarnings("java:S2925") // Can't test Kafka re-auth (KIP-368) without Thread#sleep
    @Test
    void shouldReauthenticateWithOauthBearer(Topic topic) {
        // Given
        Duration maxTimeBeforeReauth = Duration.ofSeconds(2);
        var config = proxy(cluster)
                .addToFilterDefinitions(createOauthTerminationFilterWithReauth(maxTimeBeforeReauth))
                .addToDefaultFilters(SaslTermination.class.getSimpleName());

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(getProducerConfig())) {

            // When
            assertThat(producer.send(new ProducerRecord<>(topic.name(), "key1", "value1")))
                    .succeedsWithin(Duration.ofSeconds(5));

            try {
                Thread.sleep(maxTimeBeforeReauth.plusSeconds(1).toMillis());
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }

            assertThat(producer.send(new ProducerRecord<>(topic.name(), "key2", "value2")))
                    .succeedsWithin(Duration.ofSeconds(10));

            // Then
            try (var consumer = tester.consumer(getConsumerConfig())) {
                consumer.subscribe(Set.of(topic.name()));
                var records = consumer.poll(Duration.ofSeconds(10));
                assertThat(records).hasSize(2);
            }
        }
    }

    @Test
    void shouldAuthenticateWithBothScramAndOauthMechanisms(
                                                           @TempDir Path tempDir)
            throws Exception {

        // Given
        Path keystorePath = tempDir.resolve("credentials.jks");
        var credentialManager = new ScramCredentialFileManager();
        credentialManager.createKeyStore(keystorePath, KEYSTORE_PASSWORD);
        credentialManager.addUser(keystorePath, KEYSTORE_PASSWORD, TEST_USERNAME, TEST_PASSWORD, ScramMechanism.SCRAM_SHA_256);

        var saslTermination = createMultiMechanismFilter(keystorePath, KEYSTORE_PASSWORD);
        var config = proxy(cluster)
                .addToFilterDefinitions(saslTermination)
                .addToDefaultFilters(saslTermination.name());

        try (var tester = kroxyliciousTester(config)) {
            // When/Then — SCRAM client
            try (var scramAdmin = tester.admin(createScramClientConfigs(TEST_USERNAME, TEST_PASSWORD))) {
                assertThat(scramAdmin.describeCluster().nodes())
                        .succeedsWithin(10, TimeUnit.SECONDS)
                        .isNotNull();
            }

            // When/Then — OAuth client
            try (var oauthAdmin = tester.admin(getClientConfig(TOKEN_ENDPOINT_URL))) {
                assertThat(oauthAdmin.describeCluster().nodes())
                        .succeedsWithin(10, TimeUnit.SECONDS)
                        .isNotNull();
            }
        }
    }

    private NamedFilterDefinition createOauthTerminationFilter() {
        return createOauthTerminationFilterWithConfig(
                JWKS_ENDPOINT_URL,
                EXPECTED_AUDIENCE,
                EXPECTED_ISSUER);
    }

    private NamedFilterDefinition createOauthTerminationFilterWithConfig(
                                                                         String jwksUrl,
                                                                         String audience,
                                                                         String issuer) {
        return new NamedFilterDefinitionBuilder(
                SaslTermination.class.getSimpleName(),
                SaslTermination.class.getName())
                .withConfig("mechanisms", List.of(
                        Map.of(
                                "mechanism", "OAUTHBEARER",
                                "jwksEndpointUrl", jwksUrl,
                                "expectedAudience", audience,
                                "expectedIssuer", issuer)))
                .build();
    }

    private NamedFilterDefinition createOauthTerminationFilterWithReauth(Duration maxTimeBeforeReauth) {
        return new NamedFilterDefinitionBuilder(
                SaslTermination.class.getSimpleName(),
                SaslTermination.class.getName())
                .withConfig("mechanisms", List.of(
                        Map.of("mechanism", "OAUTHBEARER",
                                "jwksEndpointUrl", JWKS_ENDPOINT_URL,
                                "expectedAudience", EXPECTED_AUDIENCE,
                                "expectedIssuer", EXPECTED_ISSUER)),
                        "maxTimeBeforeReauth", maxTimeBeforeReauth.toSeconds() + "s")
                .build();
    }

    private NamedFilterDefinition createMultiMechanismFilter(Path keystorePath, String keystorePassword) {
        return new NamedFilterDefinitionBuilder(
                SaslTermination.class.getSimpleName(),
                SaslTermination.class.getName())
                .withConfig("mechanisms", List.of(
                        Map.of("mechanism", "SCRAM-SHA-256",
                                "credentialStore", ScramCredentialFileService.class.getName(),
                                "credentialStoreConfig", Map.of(
                                        "file", keystorePath.toString(),
                                        "filePassword", Map.of("password", keystorePassword))),
                        Map.of("mechanism", "OAUTHBEARER",
                                "jwksEndpointUrl", JWKS_ENDPOINT_URL,
                                "expectedAudience", EXPECTED_AUDIENCE,
                                "expectedIssuer", EXPECTED_ISSUER)))
                .build();
    }

    private Map<String, Object> createScramClientConfigs(String username, String password) {
        String jaasConfig = String.format(
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
                username, password);
        return new HashMap<>(Map.of(
                CommonClientConfigs.CLIENT_ID_CONFIG, "scram-test-client",
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT",
                SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256",
                SaslConfigs.SASL_JAAS_CONFIG, jaasConfig));
    }

    private String getAccessToken() throws IOException, InterruptedException {
        try (var httpClient = HttpClient.newHttpClient()) {
            var tokenRequest = HttpRequest.newBuilder()
                    .uri(TOKEN_ENDPOINT_URL)
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .POST(HttpRequest.BodyPublishers.ofString(
                            "grant_type=client_credentials&client_id=" + CLIENT_ID + "&client_secret=" + CLIENT_SECRET))
                    .build();
            var tokenResponse = httpClient.send(tokenRequest, HttpResponse.BodyHandlers.ofString());
            JsonNode json = new ObjectMapper().readTree(tokenResponse.body());
            return json.get("access_token").asText();
        }
    }

    private static Request getRequest(short apiVersion, ApiMessage request) {
        return new Request(
                ApiKeys.forId(request.apiKey()),
                apiVersion,
                "test",
                request);
    }
}
