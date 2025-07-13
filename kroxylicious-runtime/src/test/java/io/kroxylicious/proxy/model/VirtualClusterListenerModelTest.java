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

import io.kroxylicious.proxy.config.secret.InlinePassword;
import io.kroxylicious.proxy.config.tls.AllowDeny;
import io.kroxylicious.proxy.config.tls.InsecureTls;
import io.kroxylicious.proxy.config.tls.KeyPair;
import io.kroxylicious.proxy.config.tls.ServerOptions;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.config.tls.TlsClientAuth;
import io.kroxylicious.proxy.config.tls.TlsTestConstants;
import io.kroxylicious.proxy.config.tls.TrustStore;
import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.RangeAwarePortPerNodeClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.RangeAwarePortPerNodeClusterNetworkAddressConfigProvider.IntRangeSpec;
import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.RangeAwarePortPerNodeClusterNetworkAddressConfigProvider.NamedRangeSpec;
import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.RangeAwarePortPerNodeClusterNetworkAddressConfigProvider.RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig;
import io.kroxylicious.proxy.model.VirtualClusterModel.VirtualClusterGatewayModel;
import io.kroxylicious.proxy.service.ClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.proxy.service.HostPort.parse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class VirtualClusterListenerModelTest {

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
        var mock = mock(ClusterNetworkAddressConfigProvider.class);
        var listener = new VirtualClusterGatewayModel(mock(VirtualClusterModel.class), mock, Optional.empty(), "default");
        var advertisedHostPort = new HostPort("broker", 55);
        when(mock.getAdvertisedBrokerAddress(0)).thenReturn(advertisedHostPort);
        assertThat(listener.getAdvertisedBrokerAddress(0)).isEqualTo(advertisedHostPort);
    }

    @Test
    void delegatesToProviderForBrokerAddress() {
        var mock = mock(ClusterNetworkAddressConfigProvider.class);
        var listener = new VirtualClusterGatewayModel(mock(VirtualClusterModel.class), mock, Optional.empty(), "default");
        var brokerAddress = new HostPort("broker", 55);
        when(mock.getBrokerAddress(0)).thenReturn(brokerAddress);
        assertThat(listener.getBrokerAddress(0)).isEqualTo(brokerAddress);
    }

    @Test
    void delegatesToProviderForBrokerIdFromBrokerAddress() {
        var mock = mock(ClusterNetworkAddressConfigProvider.class);
        var listener = new VirtualClusterGatewayModel(mock(VirtualClusterModel.class), mock, Optional.empty(), "default");
        var brokerAddress = new HostPort("broker", 55);
        when(mock.getBrokerIdFromBrokerAddress(brokerAddress)).thenReturn(1);
        assertThat(listener.getBrokerIdFromBrokerAddress(brokerAddress)).isEqualTo(1);
    }

    @Test
    void delegatesToProviderForServerNameIndication() {
        var mock = mock(ClusterNetworkAddressConfigProvider.class);
        var listener = new VirtualClusterGatewayModel(mock(VirtualClusterModel.class), mock, Optional.empty(), "default");
        when(mock.requiresServerNameIndication()).thenReturn(true);
        assertThat(listener.requiresServerNameIndication()).isTrue();
    }

    @Test
    void shouldBuildDownstreamSslContext() {
        // Given
        var downstreamTls = Optional.of(
                new Tls(keyPair,
                        new InsecureTls(false), null, null));
        var clusterNetworkAddressConfigProvider = createTestClusterNetworkAddressConfigProvider();

        // When
        var listener = new VirtualClusterGatewayModel(mock(VirtualClusterModel.class), clusterNetworkAddressConfigProvider, downstreamTls, "default");

        // Then
        assertThat(listener).isNotNull().extracting("downstreamSslContext").isNotNull();
        assertThat(listener.getDownstreamSslContext())
                .isNotNull()
                .isPresent();
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
        var downstreamTls = Optional.of(new Tls(keyPair, new TrustStore(client, PASSWORD_PROVIDER, null, new ServerOptions(clientAuth)), null, null));
        var clusterNetworkAddressConfigProvider = createTestClusterNetworkAddressConfigProvider();

        // When
        var listener = new VirtualClusterGatewayModel(mock(VirtualClusterModel.class), clusterNetworkAddressConfigProvider, downstreamTls, "default");

        // Then
        assertThat(listener)
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
        var tls = Optional.of(new Tls(keyPair, new TrustStore(client, PASSWORD_PROVIDER, null, null), null, protocolAllowDeny));
        var clusterNetworkAddressConfigProvider = createTestClusterNetworkAddressConfigProvider();

        // When
        var listener = new VirtualClusterGatewayModel(mock(VirtualClusterModel.class), clusterNetworkAddressConfigProvider, tls, "default");

        // Then
        assertThat(listener)
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
        var tls = Optional.of(new Tls(keyPair, new TrustStore(client, PASSWORD_PROVIDER, null, null), cipherSuiteAllowDeny, null));
        var clusterNetworkAddressConfigProvider = createTestClusterNetworkAddressConfigProvider();

        // When
        var listener = new VirtualClusterGatewayModel(mock(VirtualClusterModel.class), clusterNetworkAddressConfigProvider, tls, "default");

        // Then
        assertThat(listener)
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

    static Stream<Arguments> downstreamListenerThatRequiresSniEnforcesTlsWithKey() {
        return Stream.of(
                argumentSet("no tls", Optional.empty()),
                argumentSet("no key", Optional.of(new Tls(null, null, null, null))));
    }

    @ParameterizedTest
    @MethodSource
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    void downstreamListenerThatRequiresSniEnforcesTlsWithKey(Optional<Tls> downstreamTls) {
        // Given
        var model = mock(VirtualClusterModel.class);
        var provider = mock(ClusterNetworkAddressConfigProvider.class);
        when(provider.requiresServerNameIndication()).thenReturn(true);

        // When/Then
        assertThatThrownBy(() -> new VirtualClusterGatewayModel(model, provider, downstreamTls, "mygateway"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Cluster endpoint provider requires ServerNameIndication, but virtual cluster gateway 'mygateway' does not configure TLS and provide a certificate for the server");
    }

    @Test
    void detectsBadPortConfiguration() {
        // Given
        var tls = Optional.<Tls> empty();
        var model = mock(VirtualClusterModel.class);
        var provider = mock(ClusterNetworkAddressConfigProvider.class);
        when(provider.getSharedPorts()).thenReturn(Set.of(9080, 9081));
        when(provider.getExclusivePorts()).thenReturn(Set.of(9080));

        // When/Then
        assertThatThrownBy(() -> new VirtualClusterGatewayModel(model, provider, tls, "mygateway"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "The set of exclusive ports described by the cluster endpoint provider must be distinct from those described as shared. Intersection: [9080]");
    }

    @NonNull
    @SuppressWarnings("removal")
    private ClusterNetworkAddressConfigProvider createTestClusterNetworkAddressConfigProvider() {
        final RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig clusterNetworkAddressConfigProviderConfig = new RangeAwarePortPerNodeClusterNetworkAddressConfigProviderConfig(
                parse("localhost:1235"),
                "localhost", 19092, List.of(new NamedRangeSpec("default", new IntRangeSpec(0, 1))));
        return new RangeAwarePortPerNodeClusterNetworkAddressConfigProvider().build(
                clusterNetworkAddressConfigProviderConfig, Mockito.mock(VirtualClusterModel.class));
    }
}
