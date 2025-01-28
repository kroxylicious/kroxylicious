/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.model;

import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import io.netty.buffer.ByteBufAllocator;

import io.kroxylicious.proxy.config.IllegalConfigurationException;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.config.secret.InlinePassword;
import io.kroxylicious.proxy.config.tls.AllowDeny;
import io.kroxylicious.proxy.config.tls.InsecureTls;
import io.kroxylicious.proxy.config.tls.KeyPair;
import io.kroxylicious.proxy.config.tls.ServerOptions;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.config.tls.TlsClientAuth;
import io.kroxylicious.proxy.config.tls.TlsTestConstants;
import io.kroxylicious.proxy.config.tls.TrustStore;
import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.PortPerBrokerClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.PortPerBrokerClusterNetworkAddressConfigProvider.PortPerBrokerClusterNetworkAddressConfigProviderConfig;
import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.proxy.service.HostPort.parse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;
import static org.mockito.Mockito.when;

class VirtualClusterTest {

    private static final InlinePassword PASSWORD_PROVIDER = new InlinePassword("storepass");

    private static final String KNOWN_CIPHER_SUITE;

    static {
        try {
            var defaultSSLParameters = SSLContext.getDefault().getDefaultSSLParameters();
            KNOWN_CIPHER_SUITE = defaultSSLParameters.getCipherSuites()[0];
            assertThat(KNOWN_CIPHER_SUITE).isNotNull();
            assertThat(defaultSSLParameters.getProtocols()).contains("TLSv1.2", "TLSv1.3");
        }
        catch (NoSuchAlgorithmException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private String client;
    private KeyPair keyPair;

    @BeforeEach
    void setUp() {
        String privateKeyFile = TlsTestConstants.getResourceLocationOnFilesystem("server.key");
        String cert = TlsTestConstants.getResourceLocationOnFilesystem("server.crt");
        client = TlsTestConstants.getResourceLocationOnFilesystem("client.jks");
        keyPair = new KeyPair(privateKeyFile, cert, null);
    }

    @Test
    void delegatesToProviderForAdvertisedPort() {
        ClusterNetworkAddressConfigProvider mock = Mockito.mock(ClusterNetworkAddressConfigProvider.class);
        VirtualCluster cluster = new VirtualCluster("cluster", new TargetCluster("bootstrap:9092", Optional.empty()), mock, Optional.empty(), false, false);
        HostPort advertisedHostPort = new HostPort("broker", 55);
        when(mock.getAdvertisedBrokerAddress(0)).thenReturn(advertisedHostPort);
        assertThat(cluster.getAdvertisedBrokerAddress(0)).isEqualTo(advertisedHostPort);
    }

    @Test
    void shouldBuildSslContext() {
        // Given
        final Optional<Tls> downstreamTls = Optional.of(
                new Tls(keyPair,
                        new InsecureTls(false), null, null));
        final ClusterNetworkAddressConfigProvider clusterNetworkAddressConfigProvider = createTestClusterNetworkAddressConfigProvider();

        // When

        final VirtualCluster virtualCluster = new VirtualCluster("wibble", new TargetCluster("bootstrap:9092", downstreamTls), clusterNetworkAddressConfigProvider,
                downstreamTls, false,
                false);

        // Then
        assertThat(virtualCluster).isNotNull().extracting("downstreamSslContext").isNotNull();
        assertThat(virtualCluster)
                .isNotNull()
                .satisfies(vc -> assertThat(vc.getUpstreamSslContext()).isPresent());
    }

    static Stream<Arguments> clientAuthSettings() {
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

    @ParameterizedTest
    @MethodSource("clientAuthSettings")
    void shouldRequireDownstreamClientAuth(TlsClientAuth clientAuth, Consumer<SSLEngine> sslEngineAssertions) {
        // Given
        final Optional<Tls> downstreamTls = Optional.of(new Tls(keyPair, new TrustStore(client, PASSWORD_PROVIDER, null, new ServerOptions(clientAuth)), null, null));
        final ClusterNetworkAddressConfigProvider clusterNetworkAddressConfigProvider = createTestClusterNetworkAddressConfigProvider();
        final TargetCluster targetCluster = new TargetCluster("bootstrap:9092", Optional.empty());

        // When
        final VirtualCluster virtualCluster = new VirtualCluster("wibble", targetCluster, clusterNetworkAddressConfigProvider, downstreamTls, false,
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

    @Test
    void shouldNotAllowUpstreamToProvideTlsServerOptions() {
        // Given
        final Optional<Tls> downstreamTls = Optional
                .of(new Tls(keyPair, new TrustStore(client, PASSWORD_PROVIDER, null, new ServerOptions(TlsClientAuth.REQUIRED)), null, null));
        final ClusterNetworkAddressConfigProvider clusterNetworkAddressConfigProvider = createTestClusterNetworkAddressConfigProvider();
        final TargetCluster targetCluster = new TargetCluster("bootstrap:9092", downstreamTls);

        // When/Then
        assertThatThrownBy(() -> new VirtualCluster("wibble", targetCluster, clusterNetworkAddressConfigProvider, Optional.empty(), false,
                false))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("Cannot apply trust options");
    }

    static Stream<Arguments> protocolRestrictions() {
        return Stream.of(
                argumentSet("allow single protocol",
                        new AllowDeny<>(List.of("TLSv1.3"), null),
                        (Consumer<SSLEngine>) (SSLEngine sslEngine) -> assertThat(sslEngine.getEnabledProtocols()).containsExactly("TLSv1.3")),
                argumentSet("deny single protocol",
                        new AllowDeny<>(null, Set.of("TLSv1.2")),
                        (Consumer<SSLEngine>) (SSLEngine sslEngine) -> assertThat(sslEngine.getEnabledProtocols()).doesNotContain("TLSv1.2")));
    }

    @ParameterizedTest
    @MethodSource("protocolRestrictions")
    void shouldApplyDownstreamProtocolRestriction(AllowDeny<String> protocolAllowDeny, Consumer<SSLEngine> sslEngineAssertions) {
        // Given
        final Optional<Tls> tls = Optional.of(new Tls(keyPair, new TrustStore(client, PASSWORD_PROVIDER, null, null), null, protocolAllowDeny));
        final ClusterNetworkAddressConfigProvider clusterNetworkAddressConfigProvider = createTestClusterNetworkAddressConfigProvider();
        final TargetCluster targetCluster = new TargetCluster("bootstrap:9092", Optional.empty());

        // When
        final VirtualCluster virtualCluster = new VirtualCluster("wibble", targetCluster, clusterNetworkAddressConfigProvider, tls, false,
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

    static Stream<Arguments> cipherSuiteRestrictions() {
        return Stream.of(
                argumentSet("allow single ciphersuite",
                        new AllowDeny<>(List.of(KNOWN_CIPHER_SUITE), null),
                        (Consumer<SSLEngine>) (SSLEngine sslEngine) -> assertThat(sslEngine.getEnabledCipherSuites()).containsExactly(KNOWN_CIPHER_SUITE)),
                argumentSet("deny single ciphersuite",
                        new AllowDeny<>(null, Set.of(KNOWN_CIPHER_SUITE)),
                        (Consumer<SSLEngine>) (SSLEngine sslEngine) -> assertThat(sslEngine.getEnabledProtocols()).doesNotContain(KNOWN_CIPHER_SUITE)));
    }

    @ParameterizedTest
    @MethodSource("cipherSuiteRestrictions")
    void shouldApplyDownstreamCipherSuiteRestriction(AllowDeny<String> cipherSuiteAllowDeny, Consumer<SSLEngine> sslEngineAssertions) {
        // Given
        final Optional<Tls> tls = Optional.of(new Tls(keyPair, new TrustStore(client, PASSWORD_PROVIDER, null, null), cipherSuiteAllowDeny, null));
        final ClusterNetworkAddressConfigProvider clusterNetworkAddressConfigProvider = createTestClusterNetworkAddressConfigProvider();
        final TargetCluster targetCluster = new TargetCluster("bootstrap:9092", Optional.empty());

        // When
        final VirtualCluster virtualCluster = new VirtualCluster("wibble", targetCluster, clusterNetworkAddressConfigProvider, tls, false,
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

    @NonNull
    private ClusterNetworkAddressConfigProvider createTestClusterNetworkAddressConfigProvider() {
        final PortPerBrokerClusterNetworkAddressConfigProviderConfig clusterNetworkAddressConfigProviderConfig = new PortPerBrokerClusterNetworkAddressConfigProviderConfig(
                parse("localhost:1235"),
                "localhost", 19092, 0, 1);
        return new PortPerBrokerClusterNetworkAddressConfigProvider().build(
                clusterNetworkAddressConfigProviderConfig);
    }
}
