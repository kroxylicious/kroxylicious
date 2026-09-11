/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it;

import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.ScramCredentialInfo;
import org.apache.kafka.clients.admin.UserScramCredentialUpsertion;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.UnsupportedSaslMechanismException;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.filter.sasl.termination.SaslTermination;
import io.kroxylicious.it.testplugins.ClientAuthAwareLawyer;
import io.kroxylicious.it.testplugins.ClientAuthAwareLawyerFilter;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.scram.credentialstore.file.ScramCredentialFileManager;
import io.kroxylicious.scram.credentialstore.file.ScramCredentialFileService;
import io.kroxylicious.testing.filter.assertj.KafkaAssertions;
import io.kroxylicious.testing.integration.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.testing.integration.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for SASL termination filter with SCRAM-SHA-256 and SCRAM-SHA-512.
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class SaslTerminationScramIT extends BaseIT {

    private static final String TEST_USERNAME = "alice";
    private static final String TEST_PASSWORD = "alice-secret-password-123";
    private static final String KEYSTORE_PASSWORD = "keystore-password-secret-456";

    KafkaCluster cluster;

    @Test
    void shouldAuthenticateClientWithValidCredentials(
                                                      Topic topic,
                                                      @TempDir Path tempDir)
            throws Exception {

        // Generate KeyStore with test credentials
        Path keystorePath = tempDir.resolve("credentials.jks");
        var credentialManager = new ScramCredentialFileManager();
        credentialManager.createKeyStore(keystorePath, KEYSTORE_PASSWORD);
        credentialManager.addUser(keystorePath, KEYSTORE_PASSWORD, TEST_USERNAME, TEST_PASSWORD, ScramMechanism.SCRAM_SHA_256);

        // Configure SASL termination filter
        var saslTermination = createSaslTerminationFilter(keystorePath);
        var lawyer = createLawyerFilter();

        var config = proxy(cluster)
                .addToFilterDefinitions(saslTermination, lawyer)
                .addToDefaultFilters(saslTermination.name(), lawyer.name());

        // Create SCRAM client configs
        var clientConfigs = createScramClientConfigs(TEST_USERNAME, TEST_PASSWORD);

        try (var tester = kroxyliciousTester(config)) {
            // Test successful produce
            try (var producer = tester.producer(clientConfigs)) {
                assertThat(producer.send(new ProducerRecord<>(topic.name(), "my-key", "my-value")))
                        .succeedsWithin(Duration.ofSeconds(5));
            }

            // Test successful consume and verify Subject propagated
            var consumerConfigs = new HashMap<>(clientConfigs);
            consumerConfigs.put(GROUP_ID_CONFIG, "test-group");
            consumerConfigs.put(AUTO_OFFSET_RESET_CONFIG, "earliest");

            try (var consumer = tester.consumer(consumerConfigs)) {
                consumer.subscribe(Set.of(topic.name()));
                var records = consumer.poll(Duration.ofSeconds(10));

                assertThat(records).hasSize(1);
                var recordHeaders = assertThat(records.records(topic.name()))
                        .as("topic %s records", topic.name())
                        .singleElement()
                        .asInstanceOf(new InstanceOfAssertFactory<>(ConsumerRecord.class, KafkaAssertions::assertThat))
                        .headers();

                // Verify authenticated Subject propagated to downstream filter
                recordHeaders.firstHeaderWithKey(ClientAuthAwareLawyerFilter.HEADER_KEY_CLIENT_SASL_AUTHORIZATION_ID)
                        .hasValueEqualTo(TEST_USERNAME);
            }
        }
    }

    @Test
    void shouldRejectClientWithWrongPassword(
                                             Topic topic,
                                             @TempDir Path tempDir)
            throws Exception {

        // Generate KeyStore with test credentials
        Path keystorePath = tempDir.resolve("credentials.jks");
        var credentialManager = new ScramCredentialFileManager();
        credentialManager.createKeyStore(keystorePath, KEYSTORE_PASSWORD);
        credentialManager.addUser(keystorePath, KEYSTORE_PASSWORD, TEST_USERNAME, TEST_PASSWORD, ScramMechanism.SCRAM_SHA_256);

        var saslTermination = createSaslTerminationFilter(keystorePath);
        var config = proxy(cluster)
                .addToFilterDefinitions(saslTermination)
                .addToDefaultFilters(saslTermination.name());

        // Create client with wrong password
        var clientConfigs = createScramClientConfigs(TEST_USERNAME, "wrong-password");

        try (var tester = kroxyliciousTester(config)) {
            try (var producer = tester.producer(clientConfigs)) {
                assertThat(producer.send(new ProducerRecord<>(topic.name(), "my-key", "my-value")))
                        .failsWithin(5, TimeUnit.SECONDS)
                        .withThrowableOfType(ExecutionException.class)
                        .withCauseExactlyInstanceOf(SaslAuthenticationException.class);
            }
        }
    }

    @Test
    void shouldRejectClientWithUnknownUsername(
                                               Topic topic,
                                               @TempDir Path tempDir)
            throws Exception {

        // Generate KeyStore with test credentials
        Path keystorePath = tempDir.resolve("credentials.jks");
        var credentialManager = new ScramCredentialFileManager();
        credentialManager.createKeyStore(keystorePath, KEYSTORE_PASSWORD);
        credentialManager.addUser(keystorePath, KEYSTORE_PASSWORD, TEST_USERNAME, TEST_PASSWORD, ScramMechanism.SCRAM_SHA_256);

        var saslTermination = createSaslTerminationFilter(keystorePath);
        var config = proxy(cluster)
                .addToFilterDefinitions(saslTermination)
                .addToDefaultFilters(saslTermination.name());

        // Create client with unknown username
        var clientConfigs = createScramClientConfigs("unknown-user", TEST_PASSWORD);

        try (var tester = kroxyliciousTester(config)) {
            try (var producer = tester.producer(clientConfigs)) {
                assertThat(producer.send(new ProducerRecord<>(topic.name(), "my-key", "my-value")))
                        .failsWithin(5, TimeUnit.SECONDS)
                        .withThrowableOfType(ExecutionException.class)
                        .withCauseExactlyInstanceOf(SaslAuthenticationException.class);
            }
        }
    }

    @Test
    void shouldEnforceSecurityBarrier(
                                      Topic topic,
                                      @TempDir Path tempDir)
            throws Exception {

        // Generate KeyStore with test credentials
        Path keystorePath = tempDir.resolve("credentials.jks");
        var credentialManager = new ScramCredentialFileManager();
        credentialManager.createKeyStore(keystorePath, KEYSTORE_PASSWORD);
        credentialManager.addUser(keystorePath, KEYSTORE_PASSWORD, TEST_USERNAME, TEST_PASSWORD, ScramMechanism.SCRAM_SHA_256);

        var saslTermination = createSaslTerminationFilter(keystorePath);
        var config = proxy(cluster)
                .addToFilterDefinitions(saslTermination)
                .addToDefaultFilters(saslTermination.name());

        // Create client without SASL configuration
        Map<String, Object> plainClientConfigs = Map.of(
                CLIENT_ID_CONFIG, "plain-client");

        try (var tester = kroxyliciousTester(config)) {
            try (var producer = tester.producer(plainClientConfigs)) {
                // Should fail - security barrier blocks unauthenticated requests
                assertThat(producer.send(new ProducerRecord<>(topic.name(), "my-key", "my-value")))
                        .failsWithin(5, TimeUnit.SECONDS)
                        .withThrowableOfType(ExecutionException.class);
            }
        }
    }

    @Test
    void shouldReauthenticateTransparently(
                                           Topic topic,
                                           @TempDir Path tempDir)
            throws Exception {

        // Given
        Path keystorePath = tempDir.resolve("credentials.jks");
        var credentialManager = new ScramCredentialFileManager();
        credentialManager.createKeyStore(keystorePath, KEYSTORE_PASSWORD);
        credentialManager.addUser(keystorePath, KEYSTORE_PASSWORD, TEST_USERNAME, TEST_PASSWORD, ScramMechanism.SCRAM_SHA_256);

        Duration maxTimeBeforeReauth = Duration.ofSeconds(2);
        var saslTermination = createSaslTerminationFilterWithReauth(keystorePath, maxTimeBeforeReauth);
        var config = proxy(cluster)
                .addToFilterDefinitions(saslTermination)
                .addToDefaultFilters(saslTermination.name());

        var clientConfigs = createScramClientConfigs(TEST_USERNAME, TEST_PASSWORD);

        try (var tester = kroxyliciousTester(config)) {
            try (var producer = tester.producer(clientConfigs)) {
                // When - initial auth
                assertThat(producer.send(new ProducerRecord<>(topic.name(), "key1", "value1")))
                        .succeedsWithin(Duration.ofSeconds(5));

                Thread.sleep(maxTimeBeforeReauth.plusSeconds(1).toMillis());

                // When - triggers reauthentication
                assertThat(producer.send(new ProducerRecord<>(topic.name(), "key2", "value2")))
                        .succeedsWithin(Duration.ofSeconds(10));
            }

            // Then - verify both records were produced
            var consumerConfigs = new HashMap<>(clientConfigs);
            consumerConfigs.put(GROUP_ID_CONFIG, "test-group-reauth");
            consumerConfigs.put(AUTO_OFFSET_RESET_CONFIG, "earliest");

            try (var consumer = tester.consumer(consumerConfigs)) {
                consumer.subscribe(Set.of(topic.name()));
                var records = consumer.poll(Duration.ofSeconds(10));
                assertThat(records).hasSize(2);
            }
        }
    }

    private NamedFilterDefinition createSaslTerminationFilterWithReauth(
                                                                        Path keystorePath,
                                                                        Duration maxTimeBeforeReauth) {
        return new NamedFilterDefinitionBuilder(
                SaslTermination.class.getSimpleName(),
                SaslTermination.class.getName())
                .withConfig("mechanisms", List.of(
                        Map.of(
                                "mechanism", "SCRAM-SHA-256",
                                "credentialStore", ScramCredentialFileService.class.getName(),
                                "credentialStoreConfig", Map.of(
                                        "file", keystorePath.toString(),
                                        "filePassword", Map.of("password", KEYSTORE_PASSWORD)))),
                        "maxTimeBeforeReauth", maxTimeBeforeReauth.toSeconds() + "s")
                .build();
    }

    private NamedFilterDefinition createSaslTerminationFilter(Path keystorePath) {
        return createSaslTerminationFilter(keystorePath, "SCRAM-SHA-256");
    }

    private NamedFilterDefinition createLawyerFilter() {
        return new NamedFilterDefinitionBuilder(
                ClientAuthAwareLawyer.class.getName(),
                ClientAuthAwareLawyer.class.getName())
                .build();
    }

    @Test
    void shouldAuthenticateClientWithScramSha512(
                                                 Topic topic,
                                                 @TempDir Path tempDir)
            throws Exception {

        // Generate KeyStore with test credentials for SHA-512
        Path keystorePath = tempDir.resolve("credentials-512.jks");
        var credentialManager = new ScramCredentialFileManager();
        credentialManager.createKeyStore(keystorePath, KEYSTORE_PASSWORD);
        credentialManager.addUser(keystorePath, KEYSTORE_PASSWORD, TEST_USERNAME, TEST_PASSWORD, ScramMechanism.SCRAM_SHA_512);

        // Configure SASL termination filter with SHA-512
        var saslTermination = createSaslTerminationFilter(keystorePath, "SCRAM-SHA-512");

        var config = proxy(cluster)
                .addToFilterDefinitions(saslTermination)
                .addToDefaultFilters(saslTermination.name());

        // Create SCRAM-SHA-512 client configs
        var clientConfigs = createScramClientConfigs(TEST_USERNAME, TEST_PASSWORD, ScramMechanism.SCRAM_SHA_512);

        try (var tester = kroxyliciousTester(config)) {
            // Test successful produce
            try (var producer = tester.producer(clientConfigs)) {
                assertThat(producer.send(new ProducerRecord<>(topic.name(), "my-key", "my-value")))
                        .succeedsWithin(Duration.ofSeconds(5));
            }

            // Test successful consume
            var consumerConfigs = new HashMap<>(clientConfigs);
            consumerConfigs.put(GROUP_ID_CONFIG, "test-group-512");
            consumerConfigs.put(AUTO_OFFSET_RESET_CONFIG, "earliest");

            try (var consumer = tester.consumer(consumerConfigs)) {
                consumer.subscribe(Set.of(topic.name()));
                var records = consumer.poll(Duration.ofSeconds(10));

                assertThat(records).hasSize(1);
            }
        }
    }

    @Test
    void shouldRejectClientUsingWrongScramMechanism(
                                                    Topic topic,
                                                    @TempDir Path tempDir)
            throws Exception {

        // Given — credential stored as SHA-256, client uses SHA-512
        Path keystorePath = tempDir.resolve("credentials.jks");
        var credentialManager = new ScramCredentialFileManager();
        credentialManager.createKeyStore(keystorePath, KEYSTORE_PASSWORD);
        credentialManager.addUser(keystorePath, KEYSTORE_PASSWORD, TEST_USERNAME, TEST_PASSWORD, ScramMechanism.SCRAM_SHA_256);

        var saslTermination = createSaslTerminationFilter(keystorePath);
        var config = proxy(cluster)
                .addToFilterDefinitions(saslTermination)
                .addToDefaultFilters(saslTermination.name());

        var clientConfigs = createScramClientConfigs(TEST_USERNAME, TEST_PASSWORD, ScramMechanism.SCRAM_SHA_512);

        // When/Then
        try (var tester = kroxyliciousTester(config)) {
            try (var producer = tester.producer(clientConfigs)) {
                assertThat(producer.send(new ProducerRecord<>(topic.name(), "my-key", "my-value")))
                        .failsWithin(5, TimeUnit.SECONDS)
                        .withThrowableOfType(ExecutionException.class)
                        .withCauseExactlyInstanceOf(UnsupportedSaslMechanismException.class);
            }
        }
    }

    @Test
    void shouldRejectClientRequestingUnsupportedMechanism(
                                                          Topic topic,
                                                          @TempDir Path tempDir)
            throws Exception {

        // Given — proxy only supports SCRAM-SHA-256, client requests PLAIN
        Path keystorePath = tempDir.resolve("credentials.jks");
        var credentialManager = new ScramCredentialFileManager();
        credentialManager.createKeyStore(keystorePath, KEYSTORE_PASSWORD);
        credentialManager.addUser(keystorePath, KEYSTORE_PASSWORD, TEST_USERNAME, TEST_PASSWORD, ScramMechanism.SCRAM_SHA_256);

        var saslTermination = createSaslTerminationFilter(keystorePath);
        var config = proxy(cluster)
                .addToFilterDefinitions(saslTermination)
                .addToDefaultFilters(saslTermination.name());

        String jaasConfig = String.format(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                TEST_USERNAME, TEST_PASSWORD);

        var clientConfigs = new HashMap<String, Object>(Map.of(
                CLIENT_ID_CONFIG, "plain-client",
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT",
                SaslConfigs.SASL_MECHANISM, "PLAIN",
                SaslConfigs.SASL_JAAS_CONFIG, jaasConfig));

        // When/Then
        try (var tester = kroxyliciousTester(config)) {
            try (var producer = tester.producer(clientConfigs)) {
                assertThat(producer.send(new ProducerRecord<>(topic.name(), "my-key", "my-value")))
                        .failsWithin(5, TimeUnit.SECONDS)
                        .withThrowableOfType(ExecutionException.class);
            }
        }
    }

    @Test
    void shouldRejectDescribeUserScramCredentials(
                                                  @TempDir Path tempDir)
            throws Exception {

        // Given
        Path keystorePath = tempDir.resolve("credentials.jks");
        var credentialManager = new ScramCredentialFileManager();
        credentialManager.createKeyStore(keystorePath, KEYSTORE_PASSWORD);
        credentialManager.addUser(keystorePath, KEYSTORE_PASSWORD, TEST_USERNAME, TEST_PASSWORD, ScramMechanism.SCRAM_SHA_256);

        var saslTermination = createSaslTerminationFilter(keystorePath);
        var config = proxy(cluster)
                .addToFilterDefinitions(saslTermination)
                .addToDefaultFilters(saslTermination.name());

        var clientConfigs = createScramClientConfigs(TEST_USERNAME, TEST_PASSWORD);

        try (var tester = kroxyliciousTester(config);
                var admin = tester.admin(clientConfigs)) {

            // When/Then
            assertThat(admin.describeUserScramCredentials(List.of(TEST_USERNAME)).all())
                    .failsWithin(10, TimeUnit.SECONDS)
                    .withThrowableOfType(ExecutionException.class);
        }
    }

    @Test
    void shouldRejectAlterUserScramCredentials(
                                               @TempDir Path tempDir)
            throws Exception {

        // Given
        Path keystorePath = tempDir.resolve("credentials.jks");
        var credentialManager = new ScramCredentialFileManager();
        credentialManager.createKeyStore(keystorePath, KEYSTORE_PASSWORD);
        credentialManager.addUser(keystorePath, KEYSTORE_PASSWORD, TEST_USERNAME, TEST_PASSWORD, ScramMechanism.SCRAM_SHA_256);

        var saslTermination = createSaslTerminationFilter(keystorePath);
        var config = proxy(cluster)
                .addToFilterDefinitions(saslTermination)
                .addToDefaultFilters(saslTermination.name());

        var clientConfigs = createScramClientConfigs(TEST_USERNAME, TEST_PASSWORD);

        try (var tester = kroxyliciousTester(config);
                var admin = tester.admin(clientConfigs)) {

            // When
            var upsertion = new UserScramCredentialUpsertion("bob",
                    new ScramCredentialInfo(
                            org.apache.kafka.clients.admin.ScramMechanism.SCRAM_SHA_256, 10000),
                    "bobs-password-123");

            // Then
            assertThat(admin.alterUserScramCredentials(List.of(upsertion)).all())
                    .failsWithin(10, TimeUnit.SECONDS)
                    .withThrowableOfType(ExecutionException.class);
        }
    }

    @Test
    void shouldRejectCreateDelegationToken(
                                           @TempDir Path tempDir)
            throws Exception {

        // Given
        Path keystorePath = tempDir.resolve("credentials.jks");
        var credentialManager = new ScramCredentialFileManager();
        credentialManager.createKeyStore(keystorePath, KEYSTORE_PASSWORD);
        credentialManager.addUser(keystorePath, KEYSTORE_PASSWORD, TEST_USERNAME, TEST_PASSWORD, ScramMechanism.SCRAM_SHA_256);

        var saslTermination = createSaslTerminationFilter(keystorePath);
        var config = proxy(cluster)
                .addToFilterDefinitions(saslTermination)
                .addToDefaultFilters(saslTermination.name());

        var clientConfigs = createScramClientConfigs(TEST_USERNAME, TEST_PASSWORD);

        try (var tester = kroxyliciousTester(config);
                var admin = tester.admin(clientConfigs)) {

            // When/Then
            assertThat(admin.createDelegationToken().delegationToken())
                    .failsWithin(10, TimeUnit.SECONDS)
                    .withThrowableOfType(ExecutionException.class);
        }
    }

    @Test
    void shouldRejectDescribeDelegationToken(
                                             @TempDir Path tempDir)
            throws Exception {

        // Given
        Path keystorePath = tempDir.resolve("credentials.jks");
        var credentialManager = new ScramCredentialFileManager();
        credentialManager.createKeyStore(keystorePath, KEYSTORE_PASSWORD);
        credentialManager.addUser(keystorePath, KEYSTORE_PASSWORD, TEST_USERNAME, TEST_PASSWORD, ScramMechanism.SCRAM_SHA_256);

        var saslTermination = createSaslTerminationFilter(keystorePath);
        var config = proxy(cluster)
                .addToFilterDefinitions(saslTermination)
                .addToDefaultFilters(saslTermination.name());

        var clientConfigs = createScramClientConfigs(TEST_USERNAME, TEST_PASSWORD);

        try (var tester = kroxyliciousTester(config);
                var admin = tester.admin(clientConfigs)) {

            // When/Then
            assertThat(admin.describeDelegationToken().delegationTokens())
                    .failsWithin(10, TimeUnit.SECONDS)
                    .withThrowableOfType(ExecutionException.class);
        }
    }

    private NamedFilterDefinition createSaslTerminationFilter(
                                                              Path keystorePath,
                                                              String mechanism) {
        return new NamedFilterDefinitionBuilder(
                SaslTermination.class.getSimpleName(),
                SaslTermination.class.getName())
                .withConfig("mechanisms", List.of(
                        Map.of(
                                "mechanism", mechanism,
                                "credentialStore", ScramCredentialFileService.class.getName(),
                                "credentialStoreConfig", Map.of(
                                        "file", keystorePath.toString(),
                                        "filePassword", Map.of("password", KEYSTORE_PASSWORD)))))
                .build();
    }

    private Map<String, Object> createScramClientConfigs(
                                                         String username,
                                                         String password,
                                                         ScramMechanism mechanism) {
        String jaasConfig = String.format(
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
                username, password);

        return new HashMap<>(Map.of(
                CLIENT_ID_CONFIG, "scram-test-client",
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT",
                SaslConfigs.SASL_MECHANISM, mechanism.mechanismName(),
                SaslConfigs.SASL_JAAS_CONFIG, jaasConfig));
    }

    private Map<String, Object> createScramClientConfigs(
                                                         String username,
                                                         String password) {
        return createScramClientConfigs(username, password, ScramMechanism.SCRAM_SHA_256);
    }
}
