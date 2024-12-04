/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.model;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;

import javax.net.ssl.SSLEngine;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.netty.buffer.ByteBufAllocator;

import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.config.secret.InlinePassword;
import io.kroxylicious.proxy.config.tls.InsecureTls;
import io.kroxylicious.proxy.config.tls.KeyPair;
import io.kroxylicious.proxy.config.tls.ServerOptions;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.config.tls.TlsClientAuth;
import io.kroxylicious.proxy.config.tls.TlsTestConstants;
import io.kroxylicious.proxy.config.tls.TrustStore;
import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.PortPerBrokerClusterNetworkAddressConfigProvider;

import static io.kroxylicious.proxy.service.HostPort.parse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

class VirtualClusterTest {

    private String privateKeyFile;
    private String cert;
    private String client;

    @BeforeEach
    void setUp() {
        privateKeyFile = TlsTestConstants.getResourceLocationOnFilesystem("server.key");
        cert = TlsTestConstants.getResourceLocationOnFilesystem("server.crt");
        client = TlsTestConstants.getResourceLocationOnFilesystem("client.jks");
    }

    @Test
    void shouldBuildSslContext() {
        // Given
        final KeyPair keyPair = new KeyPair(privateKeyFile, cert, null);
        final Optional<Tls> tls = Optional.of(
                new Tls(keyPair,
                        new InsecureTls(false)));
        final PortPerBrokerClusterNetworkAddressConfigProvider.PortPerBrokerClusterNetworkAddressConfigProviderConfig clusterNetworkAddressConfigProviderConfig = new PortPerBrokerClusterNetworkAddressConfigProvider.PortPerBrokerClusterNetworkAddressConfigProviderConfig(
                parse("localhost:1235"),
                "localhost", 19092, 0, 1);
        final PortPerBrokerClusterNetworkAddressConfigProvider clusterNetworkAddressConfigProvider = new PortPerBrokerClusterNetworkAddressConfigProvider(
                clusterNetworkAddressConfigProviderConfig);

        // When

        final VirtualCluster virtualCluster = new VirtualCluster("wibble", new TargetCluster("bootstrap:9092", tls), clusterNetworkAddressConfigProvider, tls, false,
                false);

        // Then
        assertThat(virtualCluster).isNotNull().extracting("downstreamSslContext").isNotNull();
        assertThat(virtualCluster)
                .isNotNull()
                .satisfies(vc -> assertThat(vc.getUpstreamSslContext()).isPresent());
    }

    @ParameterizedTest
    @MethodSource("clientAuthSettings")
    void shouldRequireDownstreamClientAuth(TlsClientAuth clientAuth, Consumer<SSLEngine> sslEngineAssertions) {
        // Given
        final KeyPair keyPair = new KeyPair(privateKeyFile,
                cert,
                null);
        final Optional<Tls> tls = Optional.of(new Tls(keyPair, new TrustStore(client, new InlinePassword("storepass"), null, new ServerOptions(clientAuth))));
        final PortPerBrokerClusterNetworkAddressConfigProvider.PortPerBrokerClusterNetworkAddressConfigProviderConfig clusterNetworkAddressConfigProviderConfig = new PortPerBrokerClusterNetworkAddressConfigProvider.PortPerBrokerClusterNetworkAddressConfigProviderConfig(
                parse("localhost:1235"),
                "localhost", 19092, 0, 1);
        final PortPerBrokerClusterNetworkAddressConfigProvider clusterNetworkAddressConfigProvider = new PortPerBrokerClusterNetworkAddressConfigProvider(
                clusterNetworkAddressConfigProviderConfig);

        // When
        final VirtualCluster virtualCluster = new VirtualCluster("wibble", new TargetCluster("bootstrap:9092", tls), clusterNetworkAddressConfigProvider, tls, false,
                false);

        // Then
        assertThat(virtualCluster)
                .isNotNull()
                .satisfies(vc -> assertThat(vc.getDownstreamSslContext())
                        .isPresent()
                        .hasValueSatisfying(
                                sslContext -> {
                                    assertThat(sslContext.isClient()).isFalse();
                                    assertThat(sslContext.isServer()).isTrue();
                                    assertThat(sslContext.newEngine(ByteBufAllocator.DEFAULT)).satisfies(sslEngineAssertions);
                                }));
    }

    public static Stream<Arguments> clientAuthSettings() {
        return Stream.of(
                argumentSet("don't expect client side auth",
                        TlsClientAuth.NONE,
                        (Consumer<SSLEngine>) (SSLEngine sslEngine) -> {
                            assertThat(sslEngine.getWantClientAuth()).isFalse();
                            assertThat(sslEngine.getNeedClientAuth()).isFalse();
                        }),
                argumentSet("want client side auth",
                        TlsClientAuth.REQUESTED,
                        (Consumer<SSLEngine>) (SSLEngine sslEngine) -> {
                            assertThat(sslEngine.getWantClientAuth()).isTrue();
                            assertThat(sslEngine.getNeedClientAuth()).isFalse();
                        }),
                argumentSet("need client side auth",
                        TlsClientAuth.REQUIRED,
                        (Consumer<SSLEngine>) (SSLEngine sslEngine) -> {
                            assertThat(sslEngine.getWantClientAuth()).isFalse();
                            assertThat(sslEngine.getNeedClientAuth()).isTrue();
                        }));
    }
}
