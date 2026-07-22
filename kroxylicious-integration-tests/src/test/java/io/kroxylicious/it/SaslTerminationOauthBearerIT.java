/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.security.oauthbearer.internals.secured.VerificationKeyResolverFactory;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.jose4j.jwk.PublicJsonWebKey;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwt.JwtClaims;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.io.TempDir;

import io.kroxylicious.filter.sasl.termination.SaslTermination;
import io.kroxylicious.it.testplugins.ClientAuthAwareLawyer;
import io.kroxylicious.it.testplugins.ClientAuthAwareLawyerFilter;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.testing.filter.assertj.KafkaAssertions;
import io.kroxylicious.testing.filter.jws.JwsTestUtils;
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

    @AfterEach
    @SuppressWarnings("java:S3011")
    void afterEach() throws Exception {
        // https://issues.apache.org/jira/browse/KAFKA-17134
        var cacheField = VerificationKeyResolverFactory.class.getDeclaredField("CACHE");
        cacheField.setAccessible(true);
        ((Map<?, ?>) cacheField.get(null)).clear();
    }

    @Test
    void shouldAuthenticateWithValidToken(KafkaCluster cluster) {
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
    void shouldProduceAndConsumeWithValidToken(KafkaCluster cluster, Topic topic) {
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
    void shouldRejectTokenWithWrongAudience(KafkaCluster cluster, @TempDir Path tempDir) {
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
    void shouldRejectTokenWithWrongIssuer(KafkaCluster cluster) {
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
    void shouldRejectExpiredToken(KafkaCluster cluster, @TempDir Path tempDir) throws Exception {
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
    void shouldRejectTokenWithNoKeyId(KafkaCluster cluster, @TempDir Path tempDir) throws Exception {
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
    void shouldRejectTokenSignedByUnknownKey(KafkaCluster cluster, @TempDir Path tempDir) throws Exception {
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
                .withConfig("mechanisms", Map.of(
                        "OAUTHBEARER", Map.of(
                                "jwksEndpointUrl", jwksUrl,
                                "expectedAudience", audience,
                                "expectedIssuer", issuer)))
                .build();
    }
}
