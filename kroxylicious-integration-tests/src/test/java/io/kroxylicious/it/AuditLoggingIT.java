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
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.assertj.core.condition.AllOf;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.authorizer.provider.acl.AclAuthorizerService;
import io.kroxylicious.filter.authorization.Authorization;
import io.kroxylicious.it.AuditLoggingTestSupport.LogCaptor;
import io.kroxylicious.it.testplugins.SaslPlainTermination;
import io.kroxylicious.proxy.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import static io.kroxylicious.it.AuditLoggingTestSupport.addAuditLogging;
import static io.kroxylicious.it.AuditLoggingTestSupport.auditAction;
import static io.kroxylicious.it.AuditLoggingTestSupport.hasActorPrincipal;
import static io.kroxylicious.it.AuditLoggingTestSupport.hasObjectRef;
import static io.kroxylicious.it.AuditLoggingTestSupport.isFailure;
import static io.kroxylicious.it.AuditLoggingTestSupport.isSuccess;
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
class AuditLoggingIT {

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
}
