/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it;

import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.SaslAuthenticationException;
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
import io.kroxylicious.proxy.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.sasl.credentialstore.keystore.KeystoreScramCredentialStoreService;
import io.kroxylicious.sasl.credentialstore.keystore.TestCredentialGenerator;
import io.kroxylicious.test.assertj.KafkaAssertions;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for SASL termination filter with SCRAM-SHA-256.
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class SaslTerminationIT extends BaseIT {

    private static final String TEST_USERNAME = "alice";
    private static final String TEST_PASSWORD = "alice-secret";
    private static final String KEYSTORE_PASSWORD = "keystore-password";

    @Test
    void shouldAuthenticateClientWithValidCredentials(
                                                      KafkaCluster cluster,
                                                      Topic topic,
                                                      @TempDir Path tempDir)
            throws Exception {

        // Generate KeyStore with test credentials
        Path keystorePath = tempDir.resolve("credentials.jks");
        var generator = new TestCredentialGenerator();
        generator.generateKeyStore(
                keystorePath,
                KEYSTORE_PASSWORD,
                TEST_USERNAME, TEST_PASSWORD);

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
                                             KafkaCluster cluster,
                                             Topic topic,
                                             @TempDir Path tempDir)
            throws Exception {

        // Generate KeyStore with test credentials
        Path keystorePath = tempDir.resolve("credentials.jks");
        var generator = new TestCredentialGenerator();
        generator.generateKeyStore(
                keystorePath,
                KEYSTORE_PASSWORD,
                TEST_USERNAME, TEST_PASSWORD);

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
                                               KafkaCluster cluster,
                                               Topic topic,
                                               @TempDir Path tempDir)
            throws Exception {

        // Generate KeyStore with test credentials
        Path keystorePath = tempDir.resolve("credentials.jks");
        var generator = new TestCredentialGenerator();
        generator.generateKeyStore(
                keystorePath,
                KEYSTORE_PASSWORD,
                TEST_USERNAME, TEST_PASSWORD);

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
                                      KafkaCluster cluster,
                                      Topic topic,
                                      @TempDir Path tempDir)
            throws Exception {

        // Generate KeyStore with test credentials
        Path keystorePath = tempDir.resolve("credentials.jks");
        var generator = new TestCredentialGenerator();
        generator.generateKeyStore(
                keystorePath,
                KEYSTORE_PASSWORD,
                TEST_USERNAME, TEST_PASSWORD);

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

    private NamedFilterDefinition createSaslTerminationFilter(Path keystorePath) {
        return new NamedFilterDefinitionBuilder(
                SaslTermination.class.getSimpleName(),
                SaslTermination.class.getName())
                .withConfig("mechanisms", Map.of(
                        "SCRAM-SHA-256", Map.of(
                                "credentialStore", KeystoreScramCredentialStoreService.class.getName(),
                                "credentialStoreConfig", Map.of(
                                        "file", keystorePath.toString(),
                                        "storePassword", Map.of("password", KEYSTORE_PASSWORD),
                                        "storeType", "PKCS12"))))
                .build();
    }

    private NamedFilterDefinition createLawyerFilter() {
        return new NamedFilterDefinitionBuilder(
                ClientAuthAwareLawyer.class.getName(),
                ClientAuthAwareLawyer.class.getName())
                .build();
    }

    private Map<String, Object> createScramClientConfigs(
                                                         String username,
                                                         String password) {
        String jaasConfig = String.format(
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
                username, password);

        return new HashMap<>(Map.of(
                CLIENT_ID_CONFIG, "scram-test-client",
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT",
                SaslConfigs.SASL_MECHANISM, ScramMechanism.SCRAM_SHA_256.mechanismName(),
                SaslConfigs.SASL_JAAS_CONFIG, jaasConfig));
    }
}
