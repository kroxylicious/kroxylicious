/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.SslAuthenticationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.assertj.core.condition.AllOf;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.authorizer.provider.acl.AclAuthorizerService;
import io.kroxylicious.filter.authorization.Authorization;
import io.kroxylicious.it.AuditLoggingTestSupport.LogCaptor;
import io.kroxylicious.it.testplugins.SaslPlainTermination;
import io.kroxylicious.proxy.authentication.UserFactory;
import io.kroxylicious.proxy.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.proxy.config.TransportSubjectBuilderDefinitionBuilder;
import io.kroxylicious.proxy.config.tls.TlsClientAuth;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.KeytoolCertificateGenerator;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import static io.kroxylicious.it.AuditLoggingTestSupport.addAuditLogging;
import static io.kroxylicious.it.AuditLoggingTestSupport.auditAction;
import static io.kroxylicious.it.AuditLoggingTestSupport.hasActorPrincipal;
import static io.kroxylicious.it.AuditLoggingTestSupport.hasClientActorType;
import static io.kroxylicious.it.AuditLoggingTestSupport.hasNoPrincipals;
import static io.kroxylicious.it.AuditLoggingTestSupport.hasObjectRef;
import static io.kroxylicious.it.AuditLoggingTestSupport.hasPrincipals;
import static io.kroxylicious.it.AuditLoggingTestSupport.hasSessionId;
import static io.kroxylicious.it.AuditLoggingTestSupport.isFailure;
import static io.kroxylicious.it.AuditLoggingTestSupport.isSuccess;
import static io.kroxylicious.proxy.internal.subject.DefaultTransportSubjectBuilderService.CLIENT_TLS_SUBJECT;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.baseConfigurationBuilder;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.baseVirtualClusterBuilder;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for audit logging functionality.
 * <p>
 * This test class validates that audit events are correctly logged via the Slf4j emitter.
 * Tests focus on audit logging in isolation, without metrics emitters.
 * </p>
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class AuditLoggingIT extends AbstractTlsIT {

    /**
     * Tests that proxy lifecycle events (start and stop) are logged to Slf4j.
     * This test validates pure audit logging without metrics emitters.
     */
    @Test
    void shouldLogProxyStartAndStopToSlf4j(KafkaCluster cluster) {
        try (var captor = new LogCaptor()) {
            var builder = proxy(cluster);
            addAuditLogging(builder);

            // Proxy starts and stops within this try-with-resources block
            try (var tester = kroxyliciousTester(builder)) {
                // Proxy is now running
            }

            // Verify ProxyStart event was logged
            assertThat(captor.capturedEvents())
                    .haveExactly(1, AllOf.allOf(
                            auditAction("ProxyStart"),
                            isSuccess()));

            // Verify ProxyStop event was logged
            assertThat(captor.capturedEvents())
                    .haveExactly(1, AllOf.allOf(
                            auditAction("ProxyStop"),
                            isSuccess()));
        }
    }

    @Test
    void shouldAuditAuthorizationDecisionsDuringProduce(KafkaCluster cluster) throws Exception {
        // Create temp ACL rules file
        var rulesFile = Files.createTempFile(getClass().getName(), ".aclRules");
        try {
            Files.writeString(rulesFile, """
                    from io.kroxylicious.filter.authorization import TopicResource as Topic;
                    allow User with name = "alice" to DESCRIBE Topic with name in {"allowed-topic", "denied-topic"};
                    allow User with name = "alice" to WRITE Topic with name = "allowed-topic";
                    otherwise deny;
                    """);

            // Create topics on backing cluster before starting proxy
            try (var admin = Admin.create(cluster.getKafkaClientConfiguration())) {
                admin.createTopics(List.of(
                        new NewTopic("allowed-topic", 1, (short) 1),
                        new NewTopic("denied-topic", 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
            }

            // Configure passwords for SASL authentication
            var passwords = Map.of("alice", "alice-secret");

            // Configure SASL termination filter
            var saslTermination = new NamedFilterDefinitionBuilder(
                    "authn",
                    SaslPlainTermination.class.getName())
                    .withConfig("userNameToPassword", passwords)
                    .build();

            // Configure Authorization filter with ACL rules
            var authorization = new NamedFilterDefinitionBuilder(
                    "authz",
                    Authorization.class.getName())
                    .withConfig("authorizer", AclAuthorizerService.class.getName(),
                            "authorizerConfig", Map.of("aclFile", rulesFile.toFile().getAbsolutePath()))
                    .build();

            // Build proxy configuration with both filters and audit logging
            var builder = proxy(cluster)
                    .addToFilterDefinitions(saslTermination, authorization)
                    .addToDefaultFilters(saslTermination.name(), authorization.name());
            addAuditLogging(builder);

            // Configure SASL client credentials
            var clientSaslConfigs = Map.of(
                    CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT",
                    SaslConfigs.SASL_MECHANISM, "PLAIN",
                    SaslConfigs.SASL_JAAS_CONFIG,
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"alice\" password=\"alice-secret\";");
            Map<String, Object> producerConfig = new HashMap<>();
            producerConfig.putAll(clientSaslConfigs);

            // Start LogCaptor and proxy
            try (var captor = new LogCaptor();
                    var tester = kroxyliciousTester(builder)) {

                try (var producer = tester.producer(producerConfig)) {
                    // Send to allowed topic - should succeed
                    producer.send(new ProducerRecord<>("allowed-topic", "key1", "value1"))
                            .get(5, TimeUnit.SECONDS);

                    // Send to denied topic - should fail with authorization error
                    var future = producer.send(new ProducerRecord<>("denied-topic", "key2", "value2"));
                    assertThatThrownBy(() -> future.get(5, TimeUnit.SECONDS))
                            .hasCauseInstanceOf(TopicAuthorizationException.class);
                }

                // Verify exactly one successful Write event for allowed-topic with alice as actor
                assertThat(captor.capturedEvents())
                        .haveExactly(1, AllOf.allOf(
                                auditAction("Write"),
                                isSuccess(),
                                hasActorPrincipal("alice"),
                                hasObjectRef("io.kroxylicious.filter.authorization.TopicResource", "allowed-topic")));

                // Verify exactly one denied Write event for denied-topic with alice as actor
                assertThat(captor.capturedEvents())
                        .haveExactly(1, AllOf.allOf(
                                auditAction("Write"),
                                isFailure(),
                                hasActorPrincipal("alice"),
                                hasObjectRef("io.kroxylicious.filter.authorization.TopicResource", "denied-topic")));
            }
        }
        finally {
            Files.deleteIfExists(rulesFile);
        }
    }

    /**
     * Tests that ClientConnect events are logged for plain TCP connections.
     * Kafka clients make multiple connections (bootstrap + brokers), so we expect
     * multiple ClientConnect events. Without authentication, there should be no
     * ClientAuthenticate events.
     */
    @Test
    void shouldAuditClientConnectForPlainTcpConnection(KafkaCluster cluster) throws Exception {
        try (var captor = new LogCaptor()) {
            var builder = proxy(cluster);
            addAuditLogging(builder);

            try (var tester = kroxyliciousTester(builder);
                    var producer = tester.producer()) {
                // Send a record to ensure connection is fully established
                producer.send(new ProducerRecord<>("topic", "key", "value"))
                        .get(5, TimeUnit.SECONDS);
            }

            var events = captor.capturedEvents();

            // Verify we have at least one ClientConnect event
            assertThat(events)
                    .filteredOn(json -> "ClientConnect".equals(json.get("action").asText()))
                    .isNotEmpty()
                    .allMatch(json -> {
                        // Each ClientConnect should have these properties
                        return hasClientActorType().matches(json) &&
                                hasNoPrincipals().matches(json) &&
                                isSuccess().matches(json) &&
                                hasObjectRef("vc", "demo").matches(json);
                    });

            // Verify no ClientAuthenticate events (no auth configured)
            assertThat(events)
                    .filteredOn(json -> "ClientAuthenticate".equals(json.get("action").asText()))
                    .isEmpty();
        }
    }

    /**
     * Tests that both ClientConnect and ClientAuthenticate events are logged for
     * successful SASL PLAIN authentication. Each connection should have both events
     * with the same session ID.
     */
    @Test
    void shouldAuditClientConnectAndAuthenticateForSaslAuth(KafkaCluster cluster) throws Exception {
        // Configure SASL authentication
        var passwords = Map.of("alice", "alice-secret");
        var saslTermination = new NamedFilterDefinitionBuilder(
                "authn",
                SaslPlainTermination.class.getName())
                .withConfig("userNameToPassword", passwords)
                .build();

        var builder = proxy(cluster)
                .addToFilterDefinitions(saslTermination)
                .addToDefaultFilters(saslTermination.name());
        addAuditLogging(builder);

        // Client config with SASL credentials
        Map<String, Object> clientSaslConfigs = new HashMap<>();
        clientSaslConfigs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        clientSaslConfigs.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        clientSaslConfigs.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"alice\" password=\"alice-secret\";");

        try (var captor = new LogCaptor();
                var tester = kroxyliciousTester(builder);
                var producer = tester.producer(clientSaslConfigs)) {
            // Send a record to ensure full handshake
            producer.send(new ProducerRecord<>("topic", "key", "value"))
                    .get(5, TimeUnit.SECONDS);

            var events = captor.capturedEvents();

            // Verify we have ClientConnect events with no principals
            assertThat(events)
                    .filteredOn(json -> "ClientConnect".equals(json.get("action").asText()))
                    .isNotEmpty()
                    .allMatch(json -> hasClientActorType().matches(json) &&
                            hasNoPrincipals().matches(json) &&
                            isSuccess().matches(json));

            // Verify we have ClientAuthenticate events with principals
            assertThat(events)
                    .filteredOn(json -> "ClientAuthenticate".equals(json.get("action").asText()))
                    .isNotEmpty()
                    .allMatch(json -> hasClientActorType().matches(json) &&
                            hasPrincipals().matches(json) &&
                            hasActorPrincipal("alice").matches(json) &&
                            isSuccess().matches(json));

            // Verify session matching: for each ClientConnect, there should be a corresponding ClientAuthenticate
            var connectEvents = events.stream()
                    .filter(json -> "ClientConnect".equals(json.get("action").asText()))
                    .toList();

            for (var connectEvent : connectEvents) {
                String sessionId = connectEvent.get("actor").get("session").asText();

                // Verify there's a ClientAuthenticate with the same session ID
                assertThat(events)
                        .filteredOn(json -> "ClientAuthenticate".equals(json.get("action").asText()))
                        .anyMatch(json -> hasSessionId(sessionId).matches(json));
            }
        }
    }

    /**
     * Tests that ClientConnect events are logged for TLS transport without client authentication.
     * With server-side TLS only (no client cert extraction), we expect ClientConnect events
     * but no ClientAuthenticate events.
     */
    @Test
    void shouldAuditClientConnectForTlsTransport(KafkaCluster cluster) throws Exception {
        var builder = baseConfigurationBuilder()
                .addToVirtualClusters(baseVirtualClusterBuilder(cluster, "demo")
                        .addToGateways(
                                defaultPortIdentifiesNodeGatewayBuilder(PROXY_ADDRESS)
                                        .withNewTls()
                                        .withNewKeyStoreKey()
                                        .withStoreFile(downstreamCertificateGenerator.getKeyStoreLocation())
                                        .withNewInlinePasswordStoreProvider(downstreamCertificateGenerator.getPassword())
                                        .endKeyStoreKey()
                                        .endTls()
                                        .build())
                        .build());
        addAuditLogging(builder);

        Map<String, Object> clientConfig = new HashMap<>();
        clientConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name);
        clientConfig.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, clientTrustStore.toAbsolutePath().toString());
        clientConfig.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, downstreamCertificateGenerator.getPassword());

        try (var captor = new LogCaptor();
                var tester = kroxyliciousTester(builder);
                var admin = tester.admin("demo", clientConfig)) {
            // Perform operation to ensure connection
            admin.describeCluster().clusterId().get(10, TimeUnit.SECONDS);

            var events = captor.capturedEvents();

            // Verify we have ClientConnect events
            assertThat(events)
                    .filteredOn(json -> "ClientConnect".equals(json.get("action").asText()))
                    .isNotEmpty()
                    .allMatch(json -> hasClientActorType().matches(json) &&
                            hasNoPrincipals().matches(json) &&
                            isSuccess().matches(json));

            // Verify no ClientAuthenticate events (TLS without client cert extraction doesn't authenticate)
            assertThat(events)
                    .filteredOn(json -> "ClientAuthenticate".equals(json.get("action").asText()))
                    .isEmpty();
        }
    }

    /**
     * Tests that ClientConnect events are logged when SASL authentication fails.
     * With invalid credentials, the connection is established (ClientConnect), but
     * authentication fails. The current implementation logs a ClientAuthenticate event
     * with empty principals before the failure is detected.
     */
    @Test
    void shouldAuditClientAuthenticateFailureForInvalidSaslCredentials(KafkaCluster cluster) throws Exception {
        // Configure SASL authentication with valid credentials
        var passwords = Map.of("alice", "alice-secret");
        var saslTermination = new NamedFilterDefinitionBuilder(
                "authn",
                SaslPlainTermination.class.getName())
                .withConfig("userNameToPassword", passwords)
                .build();

        var builder = proxy(cluster)
                .addToFilterDefinitions(saslTermination)
                .addToDefaultFilters(saslTermination.name());
        addAuditLogging(builder);

        // Client config with WRONG password
        Map<String, Object> clientSaslConfigs = new HashMap<>();
        clientSaslConfigs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        clientSaslConfigs.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        clientSaslConfigs.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"alice\" password=\"wrong-password\";");

        try (var captor = new LogCaptor();
                var tester = kroxyliciousTester(builder)) {

            // Attempt to create producer - should fail with auth exception
            assertThatThrownBy(() -> {
                try (var producer = tester.producer(clientSaslConfigs)) {
                    producer.send(new ProducerRecord<>("topic", "key", "value")).get(5, TimeUnit.SECONDS);
                }
            }).hasCauseInstanceOf(SaslAuthenticationException.class);

            var events = captor.capturedEvents();

            // Verify we have ClientConnect events
            assertThat(events)
                    .filteredOn(json -> "ClientConnect".equals(json.get("action").asText()))
                    .isNotEmpty()
                    .allMatch(json -> hasClientActorType().matches(json) &&
                            hasNoPrincipals().matches(json) &&
                            isSuccess().matches(json));

            // Verify we have ClientAuthenticate events (authentication was attempted)
            assertThat(events)
                    .filteredOn(json -> "ClientAuthenticate".equals(json.get("action").asText()))
                    .isNotEmpty()
                    .allMatch(json -> hasClientActorType().matches(json));
        }
    }

    /**
     * Tests that both ClientConnect and ClientAuthenticate events are logged for
     * successful mTLS authentication with TransportSubjectBuilder extracting principals
     * from the client certificate.
     */
    @Test
    void shouldAuditClientConnectAndAuthenticateForMutualTls(KafkaCluster cluster) throws Exception {
        // Configure transport subject builder to extract identity from client cert
        var transportSubjectBuilderConfig = new TransportSubjectBuilderDefinitionBuilder(
                "DefaultTransportSubjectBuilderService")
                .withConfig("addPrincipals", List.of(
                        Map.of("from", CLIENT_TLS_SUBJECT,
                                "map", List.of(Map.of("replaceMatch", "/CN=(.*),OU=.*/$1/L")),
                                "principalFactory", UserFactory.class.getName())))
                .build();

        var builder = baseConfigurationBuilder()
                .addToVirtualClusters(baseVirtualClusterBuilder(cluster, "demo")
                        .withSubjectBuilder(transportSubjectBuilderConfig)
                        .addToGateways(
                                defaultPortIdentifiesNodeGatewayBuilder(PROXY_ADDRESS)
                                        .withNewTls()
                                        .withNewKeyStoreKey()
                                        .withStoreFile(downstreamCertificateGenerator.getKeyStoreLocation())
                                        .withNewInlinePasswordStoreProvider(downstreamCertificateGenerator.getPassword())
                                        .endKeyStoreKey()
                                        .withNewTrustStoreTrust()
                                        .withNewServerOptionsTrust()
                                        .withClientAuth(TlsClientAuth.REQUIRED)
                                        .endServerOptionsTrust()
                                        .withStoreFile(proxyTrustStore.toAbsolutePath().toString())
                                        .withNewInlinePasswordStoreProvider(clientCertGenerator.getPassword())
                                        .endTrustStoreTrust()
                                        .endTls()
                                        .build())
                        .build());
        addAuditLogging(builder);

        Map<String, Object> clientConfig = new HashMap<>();
        clientConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name);
        clientConfig.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, clientCertGenerator.getKeyStoreLocation());
        clientConfig.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, clientCertGenerator.getPassword());
        clientConfig.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, clientTrustStore.toAbsolutePath().toString());
        clientConfig.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, downstreamCertificateGenerator.getPassword());

        try (var captor = new LogCaptor();
                var tester = kroxyliciousTester(builder);
                var admin = tester.admin("demo", clientConfig)) {
            // Perform operation to ensure connection
            admin.describeCluster().clusterId().get(10, TimeUnit.SECONDS);

            var events = captor.capturedEvents();

            // Verify we have ClientConnect events with no principals (before auth)
            assertThat(events)
                    .filteredOn(json -> "ClientConnect".equals(json.get("action").asText()))
                    .isNotEmpty()
                    .allMatch(json -> hasClientActorType().matches(json) &&
                            hasNoPrincipals().matches(json) &&
                            isSuccess().matches(json));

            // Verify we have ClientAuthenticate events with principals extracted from cert
            assertThat(events)
                    .filteredOn(json -> "ClientAuthenticate".equals(json.get("action").asText()))
                    .isNotEmpty()
                    .allMatch(json -> hasClientActorType().matches(json) &&
                            hasPrincipals().matches(json) &&
                            hasActorPrincipal(CLIENT_CERT_DOMAIN).matches(json) &&
                            isSuccess().matches(json));

            // Verify session matching: for each ClientConnect, there should be a corresponding ClientAuthenticate
            var connectEvents = events.stream()
                    .filter(json -> "ClientConnect".equals(json.get("action").asText()))
                    .toList();

            for (var connectEvent : connectEvents) {
                String sessionId = connectEvent.get("actor").get("session").asText();

                // Verify there's a ClientAuthenticate with the same session ID
                assertThat(events)
                        .filteredOn(json -> "ClientAuthenticate".equals(json.get("action").asText()))
                        .anyMatch(json -> hasSessionId(sessionId).matches(json));
            }
        }
    }

    /**
     * Tests that ClientConnect events are logged when mTLS authentication fails.
     * With an untrusted certificate, the connection is established (ClientConnect),
     * but TLS handshake fails. The current implementation logs a ClientAuthenticate
     * event with empty principals before the failure is detected.
     */
    @Test
    void shouldAuditClientAuthenticateFailureForInvalidClientCertificate(KafkaCluster cluster) throws Exception {
        // Create separate untrusted client cert not in proxy's trust store
        var untrustedClientCertGen = new KeytoolCertificateGenerator();
        untrustedClientCertGen.generateSelfSignedCertificateEntry(
                "untrusted@example.com", "untrusted", "Malicious", "example.com",
                null, null, "US");

        // Configure proxy with mTLS (client auth required)
        var builder = baseConfigurationBuilder()
                .addToVirtualClusters(baseVirtualClusterBuilder(cluster, "demo")
                        .addToGateways(
                                defaultPortIdentifiesNodeGatewayBuilder(PROXY_ADDRESS)
                                        .withNewTls()
                                        .withNewKeyStoreKey()
                                        .withStoreFile(downstreamCertificateGenerator.getKeyStoreLocation())
                                        .withNewInlinePasswordStoreProvider(downstreamCertificateGenerator.getPassword())
                                        .endKeyStoreKey()
                                        .withNewTrustStoreTrust()
                                        .withNewServerOptionsTrust()
                                        .withClientAuth(TlsClientAuth.REQUIRED)
                                        .endServerOptionsTrust()
                                        .withStoreFile(proxyTrustStore.toAbsolutePath().toString())
                                        .withNewInlinePasswordStoreProvider(clientCertGenerator.getPassword())
                                        .endTrustStoreTrust()
                                        .endTls()
                                        .build())
                        .build());
        addAuditLogging(builder);

        // Client config with untrusted certificate
        Map<String, Object> clientConfig = new HashMap<>();
        clientConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name);
        clientConfig.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, untrustedClientCertGen.getKeyStoreLocation());
        clientConfig.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, untrustedClientCertGen.getPassword());
        clientConfig.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, clientTrustStore.toAbsolutePath().toString());
        clientConfig.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, downstreamCertificateGenerator.getPassword());

        try (var captor = new LogCaptor();
                var tester = kroxyliciousTester(builder)) {

            // Attempt to create admin client - should fail during TLS handshake
            assertThatThrownBy(() -> {
                try (var admin = tester.admin("demo", clientConfig)) {
                    admin.describeCluster().clusterId().get(10, TimeUnit.SECONDS);
                }
            }).hasCauseInstanceOf(SslAuthenticationException.class);

            var events = captor.capturedEvents();

            // Verify we have ClientConnect events (connection starts before handshake fails)
            assertThat(events)
                    .filteredOn(json -> "ClientConnect".equals(json.get("action").asText()))
                    .isNotEmpty()
                    .allMatch(json -> hasClientActorType().matches(json) &&
                            isSuccess().matches(json));

            // Verify we have ClientAuthenticate events (authentication was attempted)
            assertThat(events)
                    .filteredOn(json -> "ClientAuthenticate".equals(json.get("action").asText()))
                    .isNotEmpty()
                    .allMatch(json -> hasClientActorType().matches(json));
        }
    }
}
