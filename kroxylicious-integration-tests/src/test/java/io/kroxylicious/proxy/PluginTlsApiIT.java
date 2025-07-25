/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.proxy.config.secret.InlinePassword;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.config.tls.TlsBuilder;
import io.kroxylicious.proxy.config.tls.TlsClientAuth;
import io.kroxylicious.proxy.testplugins.ClientAuthAwareLawyerFilter;
import io.kroxylicious.proxy.testplugins.ClientTlsAwareLawyer;
import io.kroxylicious.test.assertj.KafkaAssertions;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

public class PluginTlsApiIT extends AbstractTlsIT {

    @Test
    void clientTlsContextPlainTcp(KafkaCluster cluster,
                                  Topic topic) {
        assertClientTlsContext(
                buildGatewayTls(TlsClientAuth.NONE, null),
                Map.of(
                        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.PLAINTEXT.name),
                cluster,
                topic,
                false,
                null,
                null);
    }

    @Test
    void clientTlsContextMutualTls(KafkaCluster cluster,
                                   Topic topic) {
        var proxyKeystorePassword = downstreamCertificateGenerator.getPassword();

        assertClientTlsContext(
                buildGatewayTls(TlsClientAuth.REQUIRED, proxyKeystorePassword),
                Map.of(
                        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name,
                        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, clientTrustStore.toAbsolutePath().toString(),
                        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, proxyKeystorePassword,
                        SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, clientCertGenerator.getKeyStoreLocation(),
                        SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, clientCertGenerator.getPassword()),
                cluster,
                topic,
                true,
                "CN=client, OU=Dev, O=kroxylicious.io, L=null, ST=null, C=US, emailAddress=clientTest@kroxylicious.io",
                "CN=localhost, OU=KI, O=kroxylicious.io, L=null, ST=null, C=US, emailAddress=test@kroxylicious.io");
    }

    @Test
    void clientTlsContextUnilateralTls(KafkaCluster cluster,
                                       Topic topic) {
        var proxyKeystorePassword = downstreamCertificateGenerator.getPassword();

        assertClientTlsContext(
                buildGatewayTls(TlsClientAuth.REQUESTED, proxyKeystorePassword),
                Map.of(
                        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name,
                        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, clientTrustStore.toAbsolutePath().toString(),
                        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, proxyKeystorePassword),
                cluster,
                topic,
                true,
                null,
                "CN=localhost, OU=KI, O=kroxylicious.io, L=null, ST=null, C=US, emailAddress=test@kroxylicious.io");
    }

    @NonNull
    private Optional<Tls> buildGatewayTls(@NonNull TlsClientAuth required,
                                          @Nullable String proxyKeystorePassword) {
        if (required == TlsClientAuth.NONE) {
            return Optional.empty();
        }
        Objects.requireNonNull(proxyKeystorePassword, "proxyKeystorePassword is null");
        var proxyKeystoreLocation = downstreamCertificateGenerator.getKeyStoreLocation();
        var proxyKeystorePasswordProvider = constructPasswordProvider(InlinePassword.class, proxyKeystorePassword);

        // @formatter:off
        return Optional.of(new TlsBuilder()
                .withNewKeyStoreKey()
                    .withStoreFile(proxyKeystoreLocation)
                    .withStorePasswordProvider(proxyKeystorePasswordProvider)
                .endKeyStoreKey()
                .withNewTrustStoreTrust()
                    .withNewServerOptionsTrust()
                        .withClientAuth(required)
                    .endServerOptionsTrust()
                    .withStoreFile(proxyTrustStore.toAbsolutePath().toString())
                    .withNewInlinePasswordStoreProvider(clientCertGenerator.getPassword())
                .endTrustStoreTrust()
            .build());
        // @formatter:on
    }

    private void assertClientTlsContext(Optional<Tls> gatewayTls,
                                        Map<String, Object> clientConfigs,
                                        KafkaCluster cluster,
                                        Topic topic,
                                        boolean expectHeaderKeyClientTlsPresent,
                                        @Nullable String expectedClientPrincipalName,
                                        @Nullable String expectedProxyPrincipalName) {
        var bootstrapServers = cluster.getBootstrapServers();

        // @formatter:off
        String demoCluster = "demo";
        var builder = new ConfigurationBuilder()
                .addNewFilterDefinition("clientConnection", ClientTlsAwareLawyer.class.getName(), null)
                .addToVirtualClusters(new VirtualClusterBuilder()
                        .withName(demoCluster)
                        .addToFilters("clientConnection")
                            .withNewTargetCluster()
                                .withBootstrapServers(bootstrapServers)
                            .endTargetCluster()
                        .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(PROXY_ADDRESS)
                                .withTls(gatewayTls)
                                .build())
                        .build());
        // @formatter:on

        try (var tester = kroxyliciousTester(builder)) {

            try (Producer<String, String> producer = tester.producer(demoCluster, clientConfigs)) {
                producer.send(new ProducerRecord<>(topic.name(), "hello", "world"));
                producer.flush();
            }

            List<ConsumerRecord<String, String>> records;
            try (var consumer = tester.consumer(demoCluster, clientConfigs)) {
                TopicPartition tp = new TopicPartition(topic.name(), 0);
                consumer.assign(Set.of(tp));
                consumer.seekToBeginning(Set.of(tp));
                do {
                    ConsumerRecords<String, String> poll = consumer.poll(Duration.ofMillis(100));
                    records = poll.records(tp);
                } while (records.isEmpty());
            }
            var recordHeaders = assertThat(records).singleElement().asInstanceOf(new InstanceOfAssertFactory<>(ConsumerRecord.class, KafkaAssertions::assertThat))
                    .headers();
            recordHeaders.singleHeaderWithKey(ClientAuthAwareLawyerFilter.HEADER_KEY_CLIENT_TLS_IS_PRESENT).value()
                    .containsExactly(expectHeaderKeyClientTlsPresent ? (byte) 1 : (byte) 0);
            recordHeaders.singleHeaderWithKey(ClientAuthAwareLawyerFilter.HEADER_KEY_CLIENT_TLS_CLIENT_X500PRINCIPAL_NAME)
                    .hasValueEqualTo(expectedClientPrincipalName);
            recordHeaders.singleHeaderWithKey(ClientAuthAwareLawyerFilter.HEADER_KEY_CLIENT_TLS_PROXY_X500PRINCIPAL_NAME)
                    .hasValueEqualTo(expectedProxyPrincipalName);
        }
    }
}
