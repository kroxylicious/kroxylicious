/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnsupportedSaslMechanismException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.protocol.ApiKeys;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.assertj.core.api.ThrowingConsumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.filters.sasl.inspection.SaslInspection;
import io.kroxylicious.proxy.authentication.UserFactory;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.proxy.internal.subject.DefaultSaslSubjectBuilderService;
import io.kroxylicious.proxy.internal.subject.PrincipalAdderConf;
import io.kroxylicious.proxy.testplugins.ClientAuthAwareLawyer;
import io.kroxylicious.proxy.testplugins.ClientAuthAwareLawyerFilter;
import io.kroxylicious.proxy.testplugins.ProtocolCounter;
import io.kroxylicious.proxy.testplugins.ProtocolCounterFilter;
import io.kroxylicious.test.assertj.HeadersAssert;
import io.kroxylicious.test.assertj.KafkaAssertions;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerConfig;
import io.kroxylicious.testing.kafka.common.SaslMechanism;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for the SASL Inspection Filter. These IT tests cover PLAIN and SCRAM mechanism.
 *
 * @see SaslInspectionOauthBearerIT
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
@SuppressWarnings("java:S5976") // Ignoring 'replace these n tests with a single parameterized one' - we are using the annotated parameters that a parameterized test wouldn't handle nicely.
class SaslInspectionIT extends BaseIT {

    /**
     * client handshakes with PLAIN
     * proxy has PLAIN inspection enabled
     * broker has PLAIN enabled
     * client authenticated with the correct password
     * => client should be able to produce and consume
     */
    @Test
    void shouldAuthenticateWhenSameMechanism_PLAIN(@SaslMechanism(value = "PLAIN", principals = {
            @SaslMechanism.Principal(user = "alice", password = "alice-secret") }) KafkaCluster cluster,
                                                   Topic topic) {

        String mechanism = "PLAIN";
        String clientLoginModule = "org.apache.kafka.common.security.plain.PlainLoginModule";
        String username = "alice";
        String password = "alice-secret";

        assertClientsCanAccessCluster(cluster, topic, mechanism, 1, clientLoginModule, username, password);
    }

    /**
     * client handshakes with SCRAM-SHA-256
     * proxy has SCRAM-SHA-256 explicitly enabled
     * broker has SCRAM-SHA-256 enabled
     * client authenticated with the correct password
     * => client should be able to produce and consume
     */
    @Test
    void shouldAuthenticateWhenSameMechanism_SCRAM_SHA_256(@SaslMechanism(value = "SCRAM-SHA-256", principals = {
            @SaslMechanism.Principal(user = "alice", password = "alice-secret") }) KafkaCluster cluster,
                                                           Topic topic) {

        String mechanism = "SCRAM-SHA-256";
        String clientLoginModule = "org.apache.kafka.common.security.scram.ScramLoginModule";
        String username = "alice";
        String password = "alice-secret";

        assertClientsCanAccessCluster(cluster, topic, mechanism, 2, clientLoginModule, username, password);
    }

    /**
     * client handshakes with SCRAM-SHA-256
     * proxy has SCRAM-SHA-256 implicitly enabled
     * broker has SCRAM-SHA-256 enabled
     * client authenticated with the correct password
     * => client should be able to produce and consume
     */
    @Test
    void shouldAuthenticateWhenSameMechanism_implict_SCRAM_SHA_256(@SaslMechanism(value = "SCRAM-SHA-256", principals = {
            @SaslMechanism.Principal(user = "alice", password = "alice-secret") }) KafkaCluster cluster,
                                                                   Topic topic) {

        String mechanism = "SCRAM-SHA-256";
        String clientLoginModule = "org.apache.kafka.common.security.scram.ScramLoginModule";
        String username = "alice";
        String password = "alice-secret";

        assertClientsCanAccessCluster(cluster, null, null, topic, mechanism,
                clientLoginModule, username, password,
                1, 2, Duration.ofMillis(0), headers -> {
                    HeadersAssert.assertThat(headers)
                            .singleHeaderWithKey(ClientAuthAwareLawyerFilter.HEADER_KEY_CLIENT_SASL_AUTHORIZATION_ID)
                            .hasValueEqualTo("alice");
                    HeadersAssert.assertThat(headers).singleHeaderWithKey(ClientAuthAwareLawyerFilter.HEADER_KEY_CLIENT_SASL_MECH_NAME)
                            .hasValueEqualTo(mechanism);
                });
    }

    /**
     * Same as {@link #shouldProvideSubject(KafkaCluster, Topic)} but with
     * SCRAM-SHA-512 rather than SCRAM-SHA-256.
     */
    @Test
    void shouldAuthenticateWhenSameMechanism_SCRAM_SHA_512(@SaslMechanism(value = "SCRAM-SHA-512", principals = {
            @SaslMechanism.Principal(user = "alice", password = "alice-secret") }) KafkaCluster cluster,
                                                           Topic topic) {

        String mechanism = "SCRAM-SHA-512";
        String clientLoginModule = "org.apache.kafka.common.security.scram.ScramLoginModule";
        String username = "alice";
        String password = "alice-secret";

        assertClientsCanAccessCluster(cluster, topic, mechanism, 2, clientLoginModule, username, password);
    }

    /**
     * client handshakes with PLAIN
     * proxy has PLAIN inspection enabled
     * broker has PLAIN enabled
     * client authenticated with incorrect password
     * => client should not be able to produce and consume
     */
    @Test
    void shouldNotAuthenticateWhenSameMechanismButWrongPassword_PLAIN(@SaslMechanism(value = "PLAIN", principals = {
            @SaslMechanism.Principal(user = "alice", password = "alice-secret") }) KafkaCluster cluster,
                                                                      Topic topic) {

        String mechanism = "PLAIN";
        String clientLoginModule = "org.apache.kafka.common.security.plain.PlainLoginModule";
        String username = "alice";
        String password = "alice-oops";

        assertClientsGetSaslAuthenticationException(cluster, topic, mechanism, clientLoginModule, username, password);
    }

    /**
     * As {@link #shouldAuthenticateWhenSameMechanism_PLAIN(KafkaCluster, Topic)}
     * but exercising the reauth case.
     */
    @Test
    void shouldAuthenticateWhenSameMechanism_PLAIN_withReauth(@SaslMechanism(value = "PLAIN", principals = {
            @SaslMechanism.Principal(user = "alice", password = "alice-secret")
    }) @BrokerConfig(name = "connections.max.reauth.ms", value = "5000") KafkaCluster cluster,
                                                              Topic topic) {

        String mechanism = "PLAIN";
        String clientLoginModule = "org.apache.kafka.common.security.plain.PlainLoginModule";
        String username = "alice";
        String password = "alice-secret";

        assertClientsCanAccessCluster(cluster, Set.of(mechanism), null, topic, mechanism, clientLoginModule, username, password,
                2, 1,
                Duration.ofMillis(10_000), headers -> {
                    HeadersAssert.assertThat(headers)
                            .singleHeaderWithKey(ClientAuthAwareLawyerFilter.HEADER_KEY_CLIENT_SASL_AUTHORIZATION_ID)
                            .hasValueEqualTo("alice");
                    HeadersAssert.assertThat(headers).singleHeaderWithKey(ClientAuthAwareLawyerFilter.HEADER_KEY_CLIENT_SASL_MECH_NAME)
                            .hasValueEqualTo(mechanism);
                });
    }

    /**
     * As {@link #shouldAuthenticateWhenSameMechanism_SCRAM_SHA_512(KafkaCluster, Topic)}
     * but exercising the reauth case.
     */
    @Test
    void shouldAuthenticateWhenSameMechanism_SCRAM_SHA_512_withReauth(@SaslMechanism(value = "SCRAM-SHA-256", principals = {
            @SaslMechanism.Principal(user = "alice", password = "alice-secret")
    }) @BrokerConfig(name = "connections.max.reauth.ms", value = "5000") KafkaCluster cluster,
                                                                      Topic topic) {

        String mechanism = "SCRAM-SHA-256";
        String clientLoginModule = "org.apache.kafka.common.security.scram.ScramLoginModule";
        String username = "alice";
        String password = "alice-secret";

        assertClientsCanAccessCluster(cluster, Set.of(mechanism), null, topic, mechanism, clientLoginModule, username, password,
                2, 2,
                Duration.ofMillis(10_000), headers -> {
                    HeadersAssert.assertThat(headers)
                            .singleHeaderWithKey(ClientAuthAwareLawyerFilter.HEADER_KEY_CLIENT_SASL_AUTHORIZATION_ID)
                            .hasValueEqualTo("alice");
                    HeadersAssert.assertThat(headers).singleHeaderWithKey(ClientAuthAwareLawyerFilter.HEADER_KEY_CLIENT_SASL_MECH_NAME)
                            .hasValueEqualTo(mechanism);
                });
    }

    /**
     * broker has PLAIN enabled
     * proxy does not support PLAIN inspection
     * client handshakes with PLAIN
     * => client should not complete authentication
     */
    @Test
    void shouldNotAuthenticateWhenNoCommonMechanism(@SaslMechanism(value = "PLAIN", principals = {
            @SaslMechanism.Principal(user = "alice", password = "alice-secret") }) KafkaCluster cluster,
                                                    Topic topic) {
        var config = buildProxyConfig(cluster, null, null);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(Map.of(
                        CommonClientConfigs.CLIENT_ID_CONFIG, "PLAIN-producer",
                        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT",
                        SaslConfigs.SASL_MECHANISM, "PLAIN",
                        SaslConfigs.SASL_JAAS_CONFIG, """
                                        org.apache.kafka.common.security.plain.PlainLoginModule required
                                            username="alice"
                                            password="alice-secret";
                                """))) {
            assertThat(producer.send(new ProducerRecord<>(topic.name(), "my-key", "my-value")))
                    .failsWithin(5, TimeUnit.SECONDS)
                    .withThrowableOfType(ExecutionException.class)
                    .withCauseExactlyInstanceOf(UnsupportedSaslMechanismException.class);
        }
    }

    /**
     * broker has PLAIN enabled
     * proxy supports PLAIN inspection
     * client not configured for SASL
     * => client should not complete authentication
     */
    @Test
    void shouldNotConnectWhenClientNotConfiguredForSasl(@SaslMechanism(value = "PLAIN", principals = {
            @SaslMechanism.Principal(user = "alice", password = "alice-secret") }) KafkaCluster cluster) {
        var config = buildProxyConfig(cluster, Set.of("PLAIN"), null);

        try (var tester = kroxyliciousTester(config);
                var admin = tester.admin(Map.of(
                        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers(),
                        CommonClientConfigs.CLIENT_ID_CONFIG, "PLAIN-producer",
                        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT"))) {
            assertThat(admin.describeCluster(new DescribeClusterOptions().timeoutMs(1000)).clusterId())
                    .failsWithin(5, TimeUnit.SECONDS)
                    .withThrowableOfType(ExecutionException.class)
                    .withCauseExactlyInstanceOf(TimeoutException.class);
        }
    }

    @Test
    void shouldProvideSubject(@SaslMechanism(value = "SCRAM-SHA-256", principals = {
            @SaslMechanism.Principal(user = "alice", password = "alice-secret") }) KafkaCluster cluster,
                              Topic topic) {

        String mechanism = "SCRAM-SHA-256";
        String clientLoginModule = "org.apache.kafka.common.security.scram.ScramLoginModule";
        String username = "alice";
        String password = "alice-secret";

        assertClientsCanAccessCluster(cluster, topic, mechanism, null, 2, clientLoginModule, username, password, headers -> {
            HeadersAssert.assertThat(headers)
                    .singleHeaderWithKey(ClientAuthAwareLawyerFilter.HEADER_KEY_AUTHENTICATED_SUBJECT)
                    .hasValueEqualTo("Subject[principals=[User[name=%s]]]".formatted(username));
        });
    }

    @Test
    void shouldProvideSubjectContainingMappedPrincipal(@SaslMechanism(value = "SCRAM-SHA-256", principals = {
            @SaslMechanism.Principal(user = "alice", password = "alice-secret") }) KafkaCluster cluster,
                                                       Topic topic) {

        String mechanism = "SCRAM-SHA-256";
        String clientLoginModule = "org.apache.kafka.common.security.scram.ScramLoginModule";
        String username = "alice";
        String password = "alice-secret";

        var subjectBuilderConfig = new DefaultSaslSubjectBuilderService.Config(List.of(
                new PrincipalAdderConf(DefaultSaslSubjectBuilderService.SASL_AUTHORIZED_ID,
                        List.of(new io.kroxylicious.proxy.internal.subject.Map("/(.*)/$1/U", null)), UserFactory.class.getName())));

        var expectedUpperCasedUserName = username.toUpperCase(Locale.ROOT);
        assertClientsCanAccessCluster(cluster, topic, mechanism, subjectBuilderConfig, 2, clientLoginModule, username, password, headers -> {
            HeadersAssert.assertThat(headers)
                    .singleHeaderWithKey(ClientAuthAwareLawyerFilter.HEADER_KEY_AUTHENTICATED_SUBJECT)
                    .hasValueEqualTo("Subject[principals=[User[name=%s]]]".formatted(expectedUpperCasedUserName));
        });
    }

    private static void assertClientsGetSaslAuthenticationException(KafkaCluster cluster, Topic topic, String mechanism, String clientLoginModule, String username,
                                                                    String password) {
        var config = buildProxyConfig(cluster, Set.of(mechanism), null);

        String jaasConfig = "%s required%n  username=\"%s\"%n   password=\"%s\";".formatted(clientLoginModule, username, password);
        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(Map.of(
                        CommonClientConfigs.CLIENT_ID_CONFIG, mechanism + "-producer",
                        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT",
                        SaslConfigs.SASL_MECHANISM, mechanism,
                        SaslConfigs.SASL_JAAS_CONFIG, jaasConfig))) {
            assertThat(producer.send(new ProducerRecord<>(topic.name(), "my-key", "my-value")))
                    .failsWithin(5, TimeUnit.SECONDS)
                    .withThrowableOfType(ExecutionException.class)
                    .withCauseExactlyInstanceOf(SaslAuthenticationException.class);
        }
    }

    private static void assertClientsCanAccessCluster(KafkaCluster cluster,
                                                      Topic topic,
                                                      String mechanism,
                                                      final int numAuthReqPerAuth,
                                                      String clientLoginModule,
                                                      String username,
                                                      String password) {
        assertClientsCanAccessCluster(cluster, topic, mechanism, null, numAuthReqPerAuth, clientLoginModule, username, password, headers -> {
            HeadersAssert.assertThat(headers)
                    .singleHeaderWithKey(ClientAuthAwareLawyerFilter.HEADER_KEY_CLIENT_SASL_AUTHORIZATION_ID)
                    .hasValueEqualTo(username);
            HeadersAssert.assertThat(headers).singleHeaderWithKey(ClientAuthAwareLawyerFilter.HEADER_KEY_CLIENT_SASL_MECH_NAME)
                    .hasValueEqualTo(mechanism);
        });
    }

    private static void assertClientsCanAccessCluster(KafkaCluster cluster,
                                                      Topic topic,
                                                      String mechanism,
                                                      @Nullable DefaultSaslSubjectBuilderService.Config subjectBuilderConfig,
                                                      final int numAuthReqPerAuth,
                                                      String clientLoginModule,
                                                      String username,
                                                      String password,
                                                      ThrowingConsumer<Headers> headersThrowingConsumer) {
        assertClientsCanAccessCluster(cluster, Set.of(mechanism), subjectBuilderConfig, topic, mechanism,
                clientLoginModule, username, password,
                1, numAuthReqPerAuth, Duration.ofMillis(0), headersThrowingConsumer);
    }

    @SuppressWarnings("java:S2925") // Impossible to integration test reauth without Thread.sleep
    private static void assertClientsCanAccessCluster(KafkaCluster cluster,
                                                      @Nullable Set<String> proxyEnabledSaslMechanisms,
                                                      @Nullable DefaultSaslSubjectBuilderService.Config subjectBuilderConfig,
                                                      Topic topic,
                                                      String clientSaslMechanism,
                                                      String clientLoginModule,
                                                      String username,
                                                      String password,
                                                      final int numBatches,
                                                      final int numAuthReqPerAuth,
                                                      Duration pauseTime,
                                                      ThrowingConsumer<Headers> headersAssertion) {
        var config = buildProxyConfig(cluster, proxyEnabledSaslMechanisms, subjectBuilderConfig);

        String jaasConfig = "%s required%n  username=\"%s\"%n   password=\"%s\";".formatted(clientLoginModule, username, password);
        try (var tester = kroxyliciousTester(config)) {

            var producerConfig = Map.<String, Object> of(
                    CommonClientConfigs.CLIENT_ID_CONFIG, clientSaslMechanism + "-producer",
                    CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT",
                    SaslConfigs.SASL_MECHANISM, clientSaslMechanism,
                    SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);
            var consumerConfig = Map.<String, Object> of(
                    CommonClientConfigs.CLIENT_ID_CONFIG, clientSaslMechanism + "-consumer",
                    CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT",
                    SaslConfigs.SASL_MECHANISM, clientSaslMechanism,
                    SaslConfigs.SASL_JAAS_CONFIG, jaasConfig,
                    ConsumerConfig.GROUP_ID_CONFIG, "my-group-id",
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                    ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "1");

            sendReceiveBatches(tester, topic, producerConfig, consumerConfig, numBatches, (batchNum, records) -> {
                var headers = assertThat(records.records(topic.name()))
                        .as("topic %s records", topic.name())
                        .singleElement()
                        .asInstanceOf(new InstanceOfAssertFactory<>(ConsumerRecord.class, KafkaAssertions::assertThat))
                        .headers()
                        .satisfies(headersAssertion);

                int newCount = ProtocolCounterFilter.fromBytes(
                        headers.singleHeaderWithKey(ProtocolCounterFilter.requestCountHeaderKey(ApiKeys.SASL_AUTHENTICATE)).value().actual());

                assertThat(newCount)
                        .as("Observed number of %s requests @ batch #%s", ApiKeys.SASL_AUTHENTICATE, batchNum)
                        .isEqualTo(numAuthReqPerAuth * batchNum);

                if (batchNum < 2 && pauseTime.isPositive()) {
                    try {
                        Thread.sleep(pauseTime);
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                }

            });
        }
    }

    private static ConfigurationBuilder buildProxyConfig(KafkaCluster cluster, @Nullable Set<String> enableMechanisms,
                                                         @Nullable DefaultSaslSubjectBuilderService.Config subjectBuilderConfig) {
        var saslInspection = buildSaslInspector(enableMechanisms, subjectBuilderConfig);
        var counter = new NamedFilterDefinitionBuilder(
                ProtocolCounter.class.getName(),
                ProtocolCounter.class.getName())
                .withConfig(
                        "countRequests", Set.of(ApiKeys.SASL_AUTHENTICATE),
                        "countResponses", Set.of(ApiKeys.SASL_AUTHENTICATE))
                .build();
        var lawyer = new NamedFilterDefinitionBuilder(
                ClientAuthAwareLawyer.class.getName(),
                ClientAuthAwareLawyer.class.getName())
                .build();
        return proxy(cluster)
                .addToFilterDefinitions(saslInspection, counter, lawyer)
                .addToDefaultFilters(saslInspection.name(), counter.name(), lawyer.name());
    }

    private static NamedFilterDefinition buildSaslInspector(@Nullable Set<String> enableMechanisms,
                                                            @Nullable DefaultSaslSubjectBuilderService.Config subjectBuilderConfig) {
        var saslInspectorBuilder = new NamedFilterDefinitionBuilder(
                SaslInspection.class.getName(),
                SaslInspection.class.getName());

        var saslInspectorConfig = new HashMap<String, Object>();
        Optional.ofNullable(enableMechanisms).ifPresent(value -> {
            saslInspectorConfig.put("enabledMechanisms", value);
        });
        Optional.ofNullable(subjectBuilderConfig).ifPresent(value -> {
            saslInspectorConfig.put("subjectBuilder", DefaultSaslSubjectBuilderService.class.getName());
            saslInspectorConfig.put("subjectBuilderConfig", subjectBuilderConfig);
        });
        saslInspectorBuilder.withConfig(saslInspectorConfig);

        return saslInspectorBuilder.build();
    }
}
