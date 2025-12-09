/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.assertj.core.api.ThrowingConsumer;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.proxy.authentication.UserFactory;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.proxy.config.TransportSubjectBuilderConfig;
import io.kroxylicious.proxy.config.TransportSubjectBuilderDefinitionBuilder;
import io.kroxylicious.proxy.config.tls.TlsClientAuth;
import io.kroxylicious.proxy.testplugins.ClientAuthAwareLawyer;
import io.kroxylicious.proxy.testplugins.ClientAuthAwareLawyerFilter;
import io.kroxylicious.test.assertj.KafkaAssertions;
import io.kroxylicious.test.tester.KroxyliciousConfigUtils;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.proxy.internal.subject.DefaultTransportSubjectBuilderService.CLIENT_TLS_SAN_DNS_NAME;
import static io.kroxylicious.proxy.internal.subject.DefaultTransportSubjectBuilderService.CLIENT_TLS_SUBJECT;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.baseVirtualClusterBuilder;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

/**
 * Tests the ability of the virtual cluster to config and use a subject builder
 * to build a subject from a client cert presented by the Kafka client.
 */
@ExtendWith(KafkaClusterExtension.class)
class VirtualClusterSubjectBuilderIT extends AbstractTlsIT {

    static Stream<Arguments> tlsComponents() {
        var builder = new TransportSubjectBuilderDefinitionBuilder("DefaultTransportSubjectBuilderService");
        return Stream.of(
                argumentSet("unmappedClientTlsSubject",
                        builder.withConfig(Map.of("addPrincipals", List.of(
                                Map.of("from", CLIENT_TLS_SUBJECT,
                                        "principalFactory", UserFactory.class.getName())))).build(),
                        (ThrowingConsumer<String>) input -> assertThat(input).contains("CN=client,OU=Dev,O=kroxylicious.io,L=null,ST=null,C=US")),
                argumentSet("mappedClientTlsSubject",
                        builder.withConfig(Map.of("addPrincipals", List.of(
                                Map.of("from", CLIENT_TLS_SUBJECT,
                                        "map", List.of(Map.of("replaceMatch", "/CN=(.*),OU=Dev.*/$1/L")),
                                        "principalFactory", UserFactory.class.getName())))).build(),
                        (ThrowingConsumer<String>) input -> assertThat(input).isEqualTo("Subject[principals=[User[name=client]]]")),
                argumentSet("elseAnonymous",
                        builder.withConfig(Map.of("addPrincipals", List.of(
                                Map.of("from", CLIENT_TLS_SUBJECT,
                                        "map", List.of(Map.of("replaceMatch", "/willNotMatch/$1/L"), Map.of("else", "anonymous")),
                                        "principalFactory", UserFactory.class.getName())))).build(),
                        (ThrowingConsumer<String>) input -> assertThat(input).isEqualTo("Subject[principals=[]]")),
                argumentSet("elseIdentity",
                        builder.withConfig(Map.of("addPrincipals", List.of(
                                Map.of("from", CLIENT_TLS_SUBJECT,
                                        "map", List.of(Map.of("replaceMatch", "/willNotMatch/$1/L"), Map.of("else", "identity")),
                                        "principalFactory", UserFactory.class.getName())))).build(),
                        (ThrowingConsumer<String>) input -> assertThat(input).contains("CN=client,OU=Dev,O=kroxylicious.io,L=null,ST=null,C=US")),
                argumentSet("clientTlsSanDnsName",
                        builder.withConfig(Map.of("addPrincipals", List.of(
                                Map.of("from", CLIENT_TLS_SAN_DNS_NAME,
                                        "principalFactory", UserFactory.class.getName())))).build(),
                        (ThrowingConsumer<String>) input -> assertThat(input).isEqualTo("Subject[principals=[User[name=client]]]"))
        );
    }

    @ParameterizedTest
    @MethodSource("tlsComponents")
    void shouldBuildSubject(TransportSubjectBuilderConfig transportSubjectBuilderConfig, ThrowingConsumer<String> expected, KafkaCluster cluster, Topic topic) {

        Map<String, Object> clientConfig = Map.of(
                CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name,
                SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, clientCertGenerator.getKeyStoreLocation(),
                SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, clientCertGenerator.getPassword(),
                SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, clientTrustStore.toAbsolutePath().toString(),
                SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, downstreamCertificateGenerator.getPassword());

        var producerConfig = new HashMap<>(clientConfig);

        var consumerConfig = new HashMap<>(clientConfig);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (var tester = kroxyliciousTester(constructMutualTlsBuilder(cluster,
                transportSubjectBuilderConfig))) {

            sendReceiveBatches(tester, topic, producerConfig, consumerConfig, 1, (batchNum, records) -> {
                assertThat(records.records(topic.name()))
                        .as("topic %s records", topic.name())
                        .singleElement()
                        .asInstanceOf(new InstanceOfAssertFactory<>(ConsumerRecord.class, KafkaAssertions::assertThat))
                        .headers()
                        .singleHeaderWithKey(ClientAuthAwareLawyerFilter.HEADER_KEY_AUTHENTICATED_SUBJECT)
                        .hasStringValueSatisfying(expected);
            });
        }

    }

    private ConfigurationBuilder constructMutualTlsBuilder(KafkaCluster cluster, TransportSubjectBuilderConfig subjectBuilderConfig) {
        var clientAware = new NamedFilterDefinitionBuilder(
                "clientAuthAwareLawyer",
                ClientAuthAwareLawyer.class.getName())
                .build();
        // @formatter:off
        return KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToFilterDefinitions(clientAware)
                .addToDefaultFilters(clientAware.name())
                .addToVirtualClusters(baseVirtualClusterBuilder(cluster, "demo")
                        .withSubjectBuilder(subjectBuilderConfig)
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
        // @formatter:on
    }
}
