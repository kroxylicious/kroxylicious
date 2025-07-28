/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.UnsupportedSaslMechanismException;
import org.apache.kafka.common.serialization.Serdes;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.proxy.testplugins.ClientAuthAwareLawyerFilter;
import io.kroxylicious.proxy.testplugins.ClientTlsAwareLawyer;
import io.kroxylicious.proxy.testplugins.SaslInspection;
import io.kroxylicious.test.assertj.KafkaAssertions;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerConfig;
import io.kroxylicious.testing.kafka.common.SaslMechanism;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class SaslInspectionIT {
    // client handshakes with PLAIN
    // proxy and broker have PLAIN enabled
    // client authenticated with the correct password
    // => client should be able to produce and consume
    @Test
    void shouldAuthenticateWhenSameMechanism_PLAIN_withReauth(
                                                              @SaslMechanism(value = "PLAIN", principals = {
                                                                      @SaslMechanism.Principal(user = "alice", password = "alice-secret") }) @BrokerConfig(name = "connections.max.reauth.ms", value = "5000") KafkaCluster cluster,
                                                              Topic topic)
            throws Exception {

        String mechanism = "PLAIN";
        String clientLoginModule = "org.apache.kafka.common.security.plain.PlainLoginModule";
        String testName = "shouldInspect";
        String username = "alice";
        String password = "alice-secret";

        assertClientsCanAccessCluster(cluster, topic, mechanism, clientLoginModule, username, password, testName,
                2,
                10_000);
    }

    // client handshakes with PLAIN
    // proxy and broker have PLAIN enabled
    // client authenticated with the correct password
    // => client should be able to produce and consume
    @Test
    void shouldAuthenticateWhenSameMechanism_PLAIN(
                                                   @SaslMechanism(value = "PLAIN", principals = {
                                                           @SaslMechanism.Principal(user = "alice", password = "alice-secret") }) KafkaCluster cluster,
                                                   Topic topic)
            throws Exception {

        String mechanism = "PLAIN";
        String clientLoginModule = "org.apache.kafka.common.security.plain.PlainLoginModule";
        String testName = "shouldInspect";
        String username = "alice";
        String password = "alice-secret";

        assertClientsCanAccessCluster(cluster, topic, mechanism, clientLoginModule, username, password, testName);
    }

    // client handshakes with SCRAM-SHA-256
    // proxy and broker have SCRAM-SHA-256 enabled
    // client authenticated with the correct password
    // => client should be able to produce and consume
    @Test
    void shouldAuthenticateWhenSameMechanism_SCRAM_SHA_256(
                                                           @SaslMechanism(value = "SCRAM-SHA-256", principals = {
                                                                   @SaslMechanism.Principal(user = "alice", password = "alice-secret") }) KafkaCluster cluster,
                                                           Topic topic)
            throws Exception {

        String mechanism = "SCRAM-SHA-256";
        String clientLoginModule = "org.apache.kafka.common.security.scram.ScramLoginModule";
        String testName = "shouldInspect";
        String username = "alice";
        String password = "alice-secret";

        assertClientsCanAccessCluster(cluster, topic, mechanism, clientLoginModule, username, password, testName);
    }

    // client handshakes with SCRAM-SHA-256
    // proxy and broker have SCRAM-SHA-256 enabled
    // client authenticated with the correct password
    // => client should be able to produce and consume
    @Test
    void shouldAuthenticateWhenSameMechanism_SCRAM_SHA_512(
                                                           @SaslMechanism(value = "SCRAM-SHA-512", principals = {
                                                                   @SaslMechanism.Principal(user = "alice", password = "alice-secret") }) KafkaCluster cluster,
                                                           Topic topic)
            throws Exception {

        String mechanism = "SCRAM-SHA-512";
        String clientLoginModule = "org.apache.kafka.common.security.scram.ScramLoginModule";
        String testName = "shouldInspect";
        String username = "alice";
        String password = "alice-secret";

        assertClientsCanAccessCluster(cluster, topic, mechanism, clientLoginModule, username, password, testName);
    }

    // client handshakes with PLAIN
    // proxy and broker have PLAIN enabled
    // client authenticated with the correct password
    // => client should be able to produce and consume
    @Test
    void shouldNotAuthenticateWhenSameMechanismButWrongPassword_PLAIN(
                                                                      @SaslMechanism(value = "PLAIN", principals = {
                                                                              @SaslMechanism.Principal(user = "alice", password = "alice-secret") }) KafkaCluster cluster,
                                                                      Topic topic) {

        String mechanism = "PLAIN";
        String clientLoginModule = "org.apache.kafka.common.security.plain.PlainLoginModule";
        String testName = "shouldInspect";
        String username = "alice";
        String password = "alice-oops";

        assertClientsGetSaslAuthenticationException(cluster, topic, mechanism, clientLoginModule, username, password, testName);
    }


    // TODO assert fails if no handshake done at all
    // TODO assert fails if client not configured for SASL
    // TODO assert that filters don't get invoked even if a client sends a metadata after getting an error after authenticate

    // TODO client tries one mech (not supported by proxy), then reattempts with the commonly supported mech
    // TODO client tries one mech (supported by proxy, but not by broker), then reattempts with the commonly supported mech
    // TODO all these things with older api versions

    // TODO reauth:
    // reauth for scram mechs
    // reauth without account => that plugins get an empty principal
    // reauth attempt by client which didn't use >= v1 Autn req

    private static void assertClientsGetSaslAuthenticationException(KafkaCluster cluster, Topic topic, String mechanism, String clientLoginModule, String username,
                                                                    String password, String testName) {
        var config = buildProxyConfig(mechanism, cluster);

        String jaasConfig = "%s required%n  username=\"%s\"%n   password=\"%s\";".formatted(clientLoginModule, username, password);
        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(Map.of(
                        CommonClientConfigs.CLIENT_ID_CONFIG, testName + "-producer",
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
                                                      String clientLoginModule,
                                                      String username,
                                                      String password,
                                                      String testName)
            throws InterruptedException, ExecutionException {
        assertClientsCanAccessCluster(cluster, topic, mechanism,
                clientLoginModule, username, password,
                testName, 1, 0);
    }

    @SuppressWarnings("java:S2925") // Impossible to integration test reauth without Thread.sleep
    private static void assertClientsCanAccessCluster(KafkaCluster cluster,
                                                      Topic topic,
                                                      String mechanism,
                                                      String clientLoginModule,
                                                      String username,
                                                      String password,
                                                      String testName,
                                                      int numBatches,
                                                      long pauseMs)
            throws InterruptedException, ExecutionException {
        var config = buildProxyConfig(mechanism, cluster);

        String jaasConfig = "%s required%n  username=\"%s\"%n   password=\"%s\";".formatted(clientLoginModule, username, password);
        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(Map.of(
                        CommonClientConfigs.CLIENT_ID_CONFIG, testName + "-producer",
                        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT",
                        SaslConfigs.SASL_MECHANISM, mechanism,
                        SaslConfigs.SASL_JAAS_CONFIG, jaasConfig));
                var consumer = tester
                        .consumer(Serdes.String(), Serdes.ByteArray(), Map.of(
                                CommonClientConfigs.CLIENT_ID_CONFIG, testName + "-consumer",
                                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT",
                                SaslConfigs.SASL_MECHANISM, mechanism,
                                SaslConfigs.SASL_JAAS_CONFIG, jaasConfig,
                                ConsumerConfig.GROUP_ID_CONFIG, "my-group-id",
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                                ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "1"))) {
            while (numBatches > 0) {
                assertThat(producer.send(new ProducerRecord<>(topic.name(), "my-key", "my-value")))
                        .succeedsWithin(Duration.ofSeconds(5));

                producer.flush();

                consumer.subscribe(Set.of(topic.name()));
                var records = consumer.poll(Duration.ofSeconds(10));

                assertThat(records).hasSize(1);
                var recordHeaders = assertThat(records.records(topic.name()))
                        .as("topic %s records", topic.name())
                        .singleElement()
                        .asInstanceOf(new InstanceOfAssertFactory<>(ConsumerRecord.class, KafkaAssertions::assertThat))
                        .headers();
                recordHeaders.singleHeaderWithKey(ClientAuthAwareLawyerFilter.HEADER_KEY_CLIENT_SASL_CLIENT_SASLPRINCIPAL_NAME)
                        .hasValueEqualTo("alice");
                recordHeaders.singleHeaderWithKey(ClientAuthAwareLawyerFilter.HEADER_KEY_CLIENT_SASL_MECH_NAME)
                        .hasValueEqualTo(mechanism);

                numBatches -= 1;
                if (numBatches > 0) {
                    Thread.sleep(pauseMs);
                }
            }
        }
    }

    // broker has PLAIN enabled
    // proxy has SCRAM enabled
    // client handshakes with PLAIN
    // => client should not complete authentication
    @Test
    void shouldNotAuthenticateWhenNoCommonMechanism(@SaslMechanism(value = "PLAIN", principals = {
            @SaslMechanism.Principal(user = "alice", password = "alice-secret") }) KafkaCluster cluster,
                                                    Topic topic) {
        String testName = "shouldInspect";

        var config = buildProxyConfig("SCRAM-SHA-256", cluster);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(Map.of(
                        CommonClientConfigs.CLIENT_ID_CONFIG, testName + "-producer",
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

    private static ConfigurationBuilder buildProxyConfig(String e1, KafkaCluster cluster) {
        NamedFilterDefinition saslInspection = new NamedFilterDefinitionBuilder(
                SaslInspection.class.getName(),
                SaslInspection.class.getName())
                .withConfig("enabledMechanisms", Set.of(e1))
                .build();
        NamedFilterDefinition lawyer = new NamedFilterDefinitionBuilder(
                ClientTlsAwareLawyer.class.getName(),
                ClientTlsAwareLawyer.class.getName())
                .build();
        return proxy(cluster)
                .addToFilterDefinitions(saslInspection, lawyer)
                .addToDefaultFilters(saslInspection.name(), lawyer.name());
    }
}
