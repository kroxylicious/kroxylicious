/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.model;

import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
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

import io.netty.buffer.ByteBufAllocator;

import io.kroxylicious.proxy.bootstrap.RouterChainFactory;
import io.kroxylicious.proxy.config.NamedRange;
import io.kroxylicious.proxy.config.PortIdentifiesNodeIdentificationStrategy;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.internal.routing.DynamicRouting;
import io.kroxylicious.proxy.internal.routing.RouteDescriptor;
import io.kroxylicious.proxy.config.secret.InlinePassword;
import io.kroxylicious.proxy.config.tls.AllowDeny;
import io.kroxylicious.proxy.config.tls.InsecureTls;
import io.kroxylicious.proxy.config.tls.KeyPair;
import io.kroxylicious.proxy.config.tls.ServerOptions;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.config.tls.TlsClientAuth;
import io.kroxylicious.proxy.config.tls.TrustStore;
import io.kroxylicious.proxy.internal.net.ProxyNodeId;
import io.kroxylicious.proxy.internal.tls.TlsTestConstants;
import io.kroxylicious.proxy.model.VirtualClusterModel.VirtualClusterGatewayModel;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.proxy.service.NodeIdentificationStrategy;

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
    void delegatesToStrategyForAdvertisedPort() {
        // Given - strategy is a plain NIS mock (does not implement AdvertisingSpec), no port resolver bound
        var mock = mock(NodeIdentificationStrategy.class);
        var listener = new VirtualClusterGatewayModel(mock(VirtualClusterModel.class), mock, Optional.empty(), "default");
        var advertisedHostPort = new HostPort("broker", 55);
        when(mock.getAdvertisedBrokerAddress(0)).thenReturn(advertisedHostPort);

        // When / Then - falls back to legacy delegation
        assertThat(listener.getAdvertisedBrokerAddress(0)).isEqualTo(advertisedHostPort);
    }

    @Test
    void shouldUseResolvedPortWhenResolverBound() {
        // Given
        var strategy = new PortIdentifiesNodeIdentificationStrategy(
                HostPort.parse("mybroker.example.com:9092"), null, null, null).buildStrategy("cluster");
        var listener = new VirtualClusterGatewayModel(mock(VirtualClusterModel.class), strategy, Optional.empty(), "default");

        int resolvedPort = 45678;
        listener.bindPortResolver(vn -> resolvedPort);

        // When
        var result = listener.getAdvertisedBrokerAddress(0);

        // Then
        assertThat(result.host()).isEqualTo("mybroker.example.com");
        assertThat(result.port()).isEqualTo(resolvedPort);
    }

    @Test
    void shouldPassCorrectProxyNodeIdToPortResolver() {
        // Given
        var strategy = new PortIdentifiesNodeIdentificationStrategy(
                HostPort.parse("broker:9092"), null, null, null).buildStrategy("cluster");
        var listener = new VirtualClusterGatewayModel(mock(VirtualClusterModel.class), strategy, Optional.empty(), "default");

        var capturedVn = new ProxyNodeId[1];
        listener.bindPortResolver(vn -> {
            capturedVn[0] = vn;
            return 9999;
        });

        // When
        listener.getAdvertisedBrokerAddress(0);

        // Then
        assertThat(capturedVn[0]).isInstanceOf(ProxyNodeId.Broker.class);
        assertThat(((ProxyNodeId.Broker) capturedVn[0]).nodeId()).isEqualTo(0);
        assertThat(((ProxyNodeId.Broker) capturedVn[0]).gateway()).isSameAs(listener);
    }

    @Test
    void shouldUseConfiguredPortWhenResolverNotBound() {
        // Given
        var strategy = new PortIdentifiesNodeIdentificationStrategy(
                HostPort.parse("broker:9092"), null, null, null).buildStrategy("cluster");
        var listener = new VirtualClusterGatewayModel(mock(VirtualClusterModel.class), strategy, Optional.empty(), "default");

        // When
        var result = listener.getAdvertisedBrokerAddress(0);

        // Then — pre-binding, strategy returns configured port
        assertThat(result).isEqualTo(new HostPort("broker", 9093));
    }

    @Test
    void shouldUseResolvedPortForBootstrapWhenResolverBound() {
        // Given
        var strategy = new PortIdentifiesNodeIdentificationStrategy(
                HostPort.parse("mybroker.example.com:9092"), null, null, null).buildStrategy("cluster");
        var listener = new VirtualClusterGatewayModel(mock(VirtualClusterModel.class), strategy, Optional.empty(), "default");

        int resolvedPort = 54321;
        listener.bindPortResolver(vn -> resolvedPort);

        // When
        var result = listener.getClusterBootstrapAddress();

        // Then
        assertThat(result.host()).isEqualTo("mybroker.example.com");
        assertThat(result.port()).isEqualTo(resolvedPort);
    }

    @Test
    void shouldUseConfiguredPortForBootstrapWhenResolverNotBound() {
        // Given
        var strategy = new PortIdentifiesNodeIdentificationStrategy(
                HostPort.parse("broker:9092"), null, null, null).buildStrategy("cluster");
        var listener = new VirtualClusterGatewayModel(mock(VirtualClusterModel.class), strategy, Optional.empty(), "default");

        // When
        var result = listener.getClusterBootstrapAddress();

        // Then
        assertThat(result).isEqualTo(new HostPort("broker", 9092));
    }

    @Test
    void delegatesToStrategyForBrokerAddress() {
        var mock = mock(NodeIdentificationStrategy.class);
        var listener = new VirtualClusterGatewayModel(mock(VirtualClusterModel.class), mock, Optional.empty(), "default");
        var brokerAddress = new HostPort("broker", 55);
        when(mock.getBrokerAddress(0)).thenReturn(brokerAddress);
        assertThat(listener.getBrokerAddress(0)).isEqualTo(brokerAddress);
    }

    @Test
    void delegatesToStrategyForServerNameIndication() {
        var mock = mock(NodeIdentificationStrategy.class);
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
        var nodeIdentificationStrategy = createTestNodeIdentificationStrategy();

        // When
        var listener = new VirtualClusterGatewayModel(mock(VirtualClusterModel.class), nodeIdentificationStrategy, downstreamTls, "default");

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
        var downstreamTls = Optional.of(new Tls(keyPair, new TrustStore(client, PASSWORD_PROVIDER, null, new ServerOptions(clientAuth)), null, null, null));
        var nodeIdentificationStrategy = createTestNodeIdentificationStrategy();

        // When
        var listener = new VirtualClusterGatewayModel(mock(VirtualClusterModel.class), nodeIdentificationStrategy, downstreamTls, "default");

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
        var nodeIdentificationStrategy = createTestNodeIdentificationStrategy();

        // When
        var listener = new VirtualClusterGatewayModel(mock(VirtualClusterModel.class), nodeIdentificationStrategy, tls, "default");

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
        var nodeIdentificationStrategy = createTestNodeIdentificationStrategy();

        // When
        var listener = new VirtualClusterGatewayModel(mock(VirtualClusterModel.class), nodeIdentificationStrategy, tls, "default");

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
        var nodeIdentificationStrategy = mock(NodeIdentificationStrategy.class);
        when(nodeIdentificationStrategy.requiresServerNameIndication()).thenReturn(true);

        // When/Then
        assertThatThrownBy(() -> new VirtualClusterGatewayModel(model, nodeIdentificationStrategy, downstreamTls, "mygateway"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "Node Identification Strategy requires ServerNameIndication, but virtual cluster gateway 'mygateway' does not configure TLS and provide a certificate for the server");
    }

    @Test
    void resolvePortForBrokerReturnsConfiguredPortWhenResolverNotBound() {
        // Given
        var strategy = new PortIdentifiesNodeIdentificationStrategy(
                HostPort.parse("broker:9092"), null, null, null).buildStrategy("cluster");
        var listener = new VirtualClusterGatewayModel(mock(VirtualClusterModel.class), strategy, Optional.empty(), "default");

        // When
        var port = listener.resolvePort(new ProxyNodeId.Broker(listener, 0));

        // Then
        assertThat(port).isEqualTo(9093);
    }

    @Test
    void resolvePortThrowsWhenPortIsZeroAndResolverNotBound() {
        // Given
        var strategy = new PortIdentifiesNodeIdentificationStrategy(
                HostPort.parse("broker:0"), null, null, null).buildStrategy("cluster");
        var listener = new VirtualClusterGatewayModel(mock(VirtualClusterModel.class), strategy, Optional.empty(), "default");
        ProxyNodeId.Bootstrap bootstrapNode = new ProxyNodeId.Bootstrap(listener);

        // When / Then
        assertThatThrownBy(() -> listener.resolvePort(bootstrapNode))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Port resolver not bound yet and configured port is 0");
    }

    @Test
    void targetClusterThrowsWhenNotDirectRouting() {
        // Given
        var model = mock(VirtualClusterModel.class);
        var strategy = new PortIdentifiesNodeIdentificationStrategy(
                HostPort.parse("broker:9092"), null, null, null).buildStrategy("cluster");
        var route = new RouteDescriptor("r1", 0, new TargetCluster("localhost:9092", Optional.empty()), null, List.of());
        when(model.routing()).thenReturn(new DynamicRouting("router", Map.of("r1", route), mock(RouterChainFactory.class)));
        when(model.getClusterName()).thenReturn("my-vc");
        var listener = new VirtualClusterGatewayModel(model, strategy, Optional.empty(), "default");

        // When / Then
        assertThatThrownBy(listener::targetCluster)
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("my-vc");
    }

    @Test
    void detectsBadPortConfiguration() {
        // Given
        var tls = Optional.<Tls> empty();
        var model = mock(VirtualClusterModel.class);
        var strategy = mock(NodeIdentificationStrategy.class);
        when(strategy.getSharedPorts()).thenReturn(Set.of(9080, 9081));
        when(strategy.getExclusivePorts()).thenReturn(Set.of(9080));

        // When/Then
        assertThatThrownBy(() -> new VirtualClusterGatewayModel(model, strategy, tls, "mygateway"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "The set of exclusive ports described by the Node Identification Strategy must be distinct from those described as shared. Intersection: [9080]");
    }

    private NodeIdentificationStrategy createTestNodeIdentificationStrategy() {
        return new PortIdentifiesNodeIdentificationStrategy(parse("localhost:1235"), "localhost", 19092,
                List.of(new NamedRange("default", 0, 0))).buildStrategy("cluster");
    }
}
