/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import io.kroxylicious.proxy.internal.net.AddressingSpec;
import io.kroxylicious.proxy.internal.net.AdvertisingSpec;
import io.kroxylicious.proxy.internal.net.BindingSpec;
import io.kroxylicious.proxy.internal.net.EndpointGateway;
import io.kroxylicious.proxy.internal.net.ProxyNodeId;
import io.kroxylicious.proxy.service.HostPort;

import static io.kroxylicious.proxy.service.HostPort.parse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SniHostIdentifiesNodeIdentificationStrategyTest {

    private static final HostPort GOOD_HOST_PORT = parse("boot.kafka:1234");

    @Test
    void canBuildStrategy() {
        var bootstrap = HostPort.parse("boot:1234");
        var config = new SniHostIdentifiesNodeIdentificationStrategy(bootstrap.toString(), "mybroker-$(nodeId)");
        var strategy = config.buildStrategy("my-cluster");
        assertThat(strategy.getClusterBootstrapAddress()).isEqualTo(bootstrap);
    }

    @Test
    void canBuildStrategyWithClusterNameReplacementToken() {
        String virtualClusterName = "my-cluster";
        var bootstrap = HostPort.parse("boot:1234");
        var config = new SniHostIdentifiesNodeIdentificationStrategy(bootstrap.toString(), "my-broker-$(virtualClusterName)-$(nodeId)");
        var strategy = config.buildStrategy(virtualClusterName);
        assertThat(strategy.getClusterBootstrapAddress()).isEqualTo(bootstrap);
        assertThat(strategy.getBrokerAddress(1)).isEqualTo(HostPort.parse("my-broker-my-cluster-1:1234"));
        assertThat(strategy.getAdvertisedBrokerAddress(1)).isEqualTo(HostPort.parse("my-broker-my-cluster-1:1234"));
    }

    @Test
    void valid() {
        var config = new SniHostIdentifiesNodeIdentificationStrategy(GOOD_HOST_PORT.toString(), "broker$(nodeId)-good");
        assertThat(config).isNotNull();
    }

    @Test
    void canBuildStrategyWithPortZero() {
        var bootstrap = HostPort.parse("boot:0");
        var config = new SniHostIdentifiesNodeIdentificationStrategy(bootstrap.toString(), "mybroker-$(nodeId)");
        assertThat(config.getBootstrapPort()).isZero();
        var strategy = config.buildStrategy("my-cluster");
        assertThat(strategy.getClusterBootstrapAddress()).isEqualTo(bootstrap);
    }

    @Test
    void missingBootstrap() {
        assertThatThrownBy(() -> new SniHostIdentifiesNodeIdentificationStrategy(null, "broker$(nodeId)-good"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void getBootstrapAddressFromConfig() {
        var config = new SniHostIdentifiesNodeIdentificationStrategy(GOOD_HOST_PORT.toString(), "broker$(nodeId)-good");
        assertThat(config.getBootstrapAddressPattern()).isEqualTo(GOOD_HOST_PORT.host());
        assertThat(config.getAdvertisedPort()).isEqualTo(GOOD_HOST_PORT.port());
    }

    @Test
    void mustSupplyAnAdvertisedBrokerAddressPattern() {
        assertThatThrownBy(() -> new SniHostIdentifiesNodeIdentificationStrategy("arbitrary:1235", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("advertisedBrokerAddressPattern cannot be null");
    }

    @ParameterizedTest
    @ValueSource(strings = { "nonodetoken", "recursive$(nodeId$(nodeId))", "capitalisedrejected$(NODEID)", "noportalloweed-in-pattern-part$(nodeId):1234:1234" })
    @NullAndEmptySource
    void invalidAdvertisedBrokerAddressPattern(String input) {
        assertThatThrownBy(() -> new SniHostIdentifiesNodeIdentificationStrategy("good:1235", input))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = { "broker$(nodeId)", "twice$(nodeId)allowed$(nodeId)too", "broker$(nodeId).kafka.com" })
    void validAdvertisedBrokerAddressPattern(String input) {
        var config = new SniHostIdentifiesNodeIdentificationStrategy("good:1235", input);
        assertThat(config).isNotNull();
    }

    @Test
    void getBrokerAddress() {
        var strategy = new SniHostIdentifiesNodeIdentificationStrategy(GOOD_HOST_PORT.toString(), "broker-$(nodeId).kafka").buildStrategy("cluster");
        assertThat(strategy.getBrokerAddress(0)).isEqualTo(HostPort.parse("broker-0.kafka:1234"));
    }

    @Test
    void getAdvertisedBrokerAddress() {
        var strategy = new SniHostIdentifiesNodeIdentificationStrategy(GOOD_HOST_PORT.toString(), "broker-$(nodeId).kafka").buildStrategy("cluster");
        assertThat(strategy.getAdvertisedBrokerAddress(0)).isEqualTo(HostPort.parse("broker-0.kafka:1234"));
    }

    @Test
    void getClusterBootstrapAddress() {
        var strategy = new SniHostIdentifiesNodeIdentificationStrategy(GOOD_HOST_PORT.toString(), "broker-$(nodeId).kafka").buildStrategy("cluster");
        assertThat(strategy.getClusterBootstrapAddress()).isEqualTo(GOOD_HOST_PORT);
    }

    @Test
    void getClusterBootstrapAddressReplacesClusterName() {
        var strategy = new SniHostIdentifiesNodeIdentificationStrategy("$(virtualClusterName)-boot.kafka:1234", "broker-$(nodeId).kafka").buildStrategy("cluster");
        assertThat(strategy.getClusterBootstrapAddress()).isEqualTo(HostPort.parse("cluster-boot.kafka:1234"));
    }

    public static Stream<Arguments> disallowedClusterNames() {
        return Stream.of(Arguments.argumentSet("disallowed-metacharacters", "broker$1"),
                Arguments.argumentSet("hyphen at start of dns label", "-broker"));
    }

    @ParameterizedTest
    @MethodSource("disallowedClusterNames")
    void buildFailsIfReplacedClusterNameInBootstrapAddressCreatesInvalidUri(String clusterName) {
        SniHostIdentifiesNodeIdentificationStrategy strategy = new SniHostIdentifiesNodeIdentificationStrategy("$(virtualClusterName).boot.kafka:1234",
                "broker-$(nodeId).kafka");
        assertThatThrownBy(() -> {
            strategy.buildStrategy(clusterName);
        }).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Bootstrap address is not a valid URI: " + clusterName + ".boot.kafka:1234");
    }

    @ParameterizedTest
    @MethodSource("disallowedClusterNames")
    void buildFailsIfReplacedClusterNameInBrokerAddressPatternCreatesInvalidUri(String clusterName) {
        SniHostIdentifiesNodeIdentificationStrategy strategy = new SniHostIdentifiesNodeIdentificationStrategy("boot.kafka:1234",
                "$(virtualClusterName).broker-$(nodeId).kafka");
        assertThatThrownBy(() -> {
            strategy.buildStrategy(clusterName);
        }).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Advertised broker address pattern did not produce a valid URI for test node 1: " + clusterName + ".broker-1.kafka:1234");
    }

    @Test
    void getSharedPorts() {
        var strategy = new SniHostIdentifiesNodeIdentificationStrategy(GOOD_HOST_PORT.toString(), "broker-$(nodeId).kafka").buildStrategy("cluster");
        assertThat(strategy.getSharedPorts()).isEqualTo(Set.of(GOOD_HOST_PORT.port()));
    }

    @Test
    void requiresServerNameIndication() {
        var strategy = new SniHostIdentifiesNodeIdentificationStrategy(GOOD_HOST_PORT.toString(), "broker-$(nodeId).kafka").buildStrategy("cluster");
        assertThat(strategy.requiresServerNameIndication()).isTrue();
    }

    @Test
    void getBrokerAddressUsesBootstrapPort() {
        var strategy = new SniHostIdentifiesNodeIdentificationStrategy(GOOD_HOST_PORT.toString(), "broker-$(nodeId).kafka:443").buildStrategy("cluster");

        assertThat(strategy.getBrokerAddress(0)).isEqualTo(HostPort.parse("broker-0.kafka:1234"));
    }

    @Test
    void getAdvertisedPortPrefersAdvertisedBrokerAddressIfSpecified() {
        var strategy = new SniHostIdentifiesNodeIdentificationStrategy(GOOD_HOST_PORT.toString(), "broker-$(nodeId).kafka:443").buildStrategy("cluster");

        assertThat(strategy.getAdvertisedBrokerAddress(0)).isEqualTo(HostPort.parse("broker-0.kafka:443"));
    }

    static Stream<Arguments> identifiesNodeIdFromSniHostnameUsingBrokerAddressPattern() {
        return Stream.of(
                argumentSet("broker 0", "broker-$(nodeId).kafka", "cluster", "broker-0.kafka", new AddressingSpec.Target.Node(0)),
                argumentSet("broker 99", "broker-$(nodeId).kafka", "cluster", "broker-99.kafka", new AddressingSpec.Target.Node(99)),
                argumentSet("nodeId replacement in last position", "broker-$(nodeId)", "cluster", "broker-99", new AddressingSpec.Target.Node(99)),
                argumentSet("RFC 4343 case insensitive", "broker-$(nodeId).kafka", "cluster", "BROKER-0.KAFKA", new AddressingSpec.Target.Node(0)),
                argumentSet("host mismatch", "broker-$(nodeId).kafka", "cluster", "broker-0.another", new AddressingSpec.Target.NotRecognised()),
                argumentSet("RE anchoring", "broker-$(nodeId).kafka", "cluster", "0.kafka", new AddressingSpec.Target.NotRecognised()),
                argumentSet("RE anchoring", "broker-$(nodeId).kafka", "cluster", "start.broker-0.kafka.end", new AddressingSpec.Target.NotRecognised()),
                argumentSet("RE metacharacters in brokerAddressPattern escaped", "broker-$(nodeId).kafka", "cluster", "broker-0xkafka",
                        new AddressingSpec.Target.NotRecognised()),
                argumentSet("cluster name replacement broker 0", "broker-$(virtualClusterName)-$(nodeId).kafka", "cluster", "broker-cluster-0.kafka",
                        new AddressingSpec.Target.Node(0)),
                argumentSet("cluster name replacement in last position", "broker-$(nodeId)-$(virtualClusterName)", "cluster", "broker-0-cluster",
                        new AddressingSpec.Target.Node(0)),
                argumentSet("cluster name replacement broker 99", "broker-$(virtualClusterName)-$(nodeId).kafka", "cluster", "broker-cluster-99.kafka",
                        new AddressingSpec.Target.Node(99)),
                argumentSet("cluster name replacement nodeId replacement in last position", "broker-$(virtualClusterName)-$(nodeId)", "cluster", "broker-cluster-99",
                        new AddressingSpec.Target.Node(99)),
                argumentSet("cluster name replacement RFC 4343 case insensitive", "broker-$(virtualClusterName)-$(nodeId).kafka", "cluster",
                        "BROKER-CLUSTER-0.KAFKA", new AddressingSpec.Target.Node(0)),
                argumentSet("cluster name replacement host mismatch", "broker-$(virtualClusterName)-$(nodeId).kafka", "cluster", "broker-cluster-0.another",
                        new AddressingSpec.Target.NotRecognised()),
                argumentSet("cluster name replacement RE anchoring", "broker-$(virtualClusterName)-$(nodeId).kafka", "cluster", "-cluster-0.kafka",
                        new AddressingSpec.Target.NotRecognised()),
                argumentSet("cluster name replacement RE anchoring", "broker-$(virtualClusterName)-$(nodeId).kafka", "cluster", "start.broker-cluster-0.kafka.end",
                        new AddressingSpec.Target.NotRecognised()),
                argumentSet("cluster name replacement RE metacharacters in brokerAddressPattern escaped", "broker-$(virtualClusterName)-$(nodeId).kafka", "cluster",
                        "broker-cluster-0xkafka", new AddressingSpec.Target.NotRecognised()));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("identifiesNodeIdFromSniHostnameUsingBrokerAddressPattern")
    void identifiesNodeIdFromSniHostnameUsingBrokerAddressPattern(String brokerAddressPattern, String clusterName, String sniHostname,
                                                                  AddressingSpec.Target expected) {
        var addressing = buildAddressingSpec(GOOD_HOST_PORT.toString(), brokerAddressPattern, clusterName);

        var result = addressing.identify(GOOD_HOST_PORT.port(), sniHostname);

        assertThat(result).isEqualTo(expected);
    }

    @Test
    void badNodeId() {
        var strategy = new SniHostIdentifiesNodeIdentificationStrategy(GOOD_HOST_PORT.toString(), "broker-$(nodeId).kafka").buildStrategy("cluster");

        assertThatThrownBy(() -> strategy.getBrokerAddress(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void containsOpenshiftRoutePlaceholderToken() {
        var strategy = new SniHostIdentifiesNodeIdentificationStrategy("one-bootstrap.$(unresolvedRouteHost):9291", "one-$(nodeId).$(unresolvedRouteHost):443");

        assertThatThrownBy(() -> strategy.buildStrategy("cluster"))
                .isInstanceOf(SniHostIdentifiesNodeIdentificationStrategy.UnresolvedHostException.class);
    }

    // AddressingSpec

    @Test
    void shouldIdentifyBrokerNodeIdFromSniHostname() {
        // Given
        var addressing = buildAddressingSpec("boot.kafka:1234", "broker-$(nodeId).kafka", "cluster");

        // When
        var result = addressing.identify(1234, "broker-5.kafka");

        // Then
        assertThat(result).isEqualTo(new AddressingSpec.Target.Node(5));
    }

    static Stream<Arguments> sniIdentifyCases() {
        return Stream.of(
                argumentSet("bootstrap SNI", "boot.kafka", new AddressingSpec.Target.Bootstrap()),
                argumentSet("null SNI treated as bootstrap", null, new AddressingSpec.Target.Bootstrap()),
                argumentSet("bootstrap SNI case-insensitive (RFC 4343)", "BOOT.KAFKA", new AddressingSpec.Target.Bootstrap()),
                argumentSet("broker SNI case-insensitive (RFC 4343)", "BROKER-7.KAFKA", new AddressingSpec.Target.Node(7)));
    }

    @ParameterizedTest
    @MethodSource("sniIdentifyCases")
    void shouldIdentifyTargetFromSni(String sniHostname, AddressingSpec.Target expected) {
        // Given
        var addressing = buildAddressingSpec("boot.kafka:1234", "broker-$(nodeId).kafka", "cluster");

        // When
        var result = addressing.identify(1234, sniHostname);

        // Then
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void shouldIdentifyNodeIdFromSniWhenPatternContainsClusterName() {
        // Given
        var addressing = buildAddressingSpec("boot.kafka:1234", "broker-$(virtualClusterName)-$(nodeId).kafka", "mycluster");

        // When
        var result = addressing.identify(1234, "broker-mycluster-3.kafka");

        // Then
        assertThat(result).isEqualTo(new AddressingSpec.Target.Node(3));
    }

    @Test
    void shouldReturnNotRecognisedForSniMatchingNeitherBootstrapNorBrokerPattern() {
        // Given - SNI for a hostname belonging to a different gateway sharing this channel
        var addressing = buildAddressingSpec("boot.kafka:1234", "broker-$(nodeId).kafka", "cluster");

        // When
        var result = addressing.identify(1234, "other-gateway.example.com");

        // Then
        assertThat(result).isEqualTo(new AddressingSpec.Target.NotRecognised());
    }

    // BindingSpec

    @Test
    void shouldExposeBootstrapAddressAsBindAddress() {
        // Given
        var binding = buildBindingSpec("boot.kafka:1234", "broker-$(nodeId).kafka");

        // When
        var result = binding.getBootstrapBindAddress();

        // Then
        assertThat(result).isEqualTo(HostPort.parse("boot.kafka:1234"));
    }

    @Test
    void shouldHaveNoPerNodeBindAddresses() {
        // Given - SNI shares one port; no per-node bind addresses needed
        var binding = buildBindingSpec("boot.kafka:1234", "broker-$(nodeId).kafka");

        // When
        var result = binding.nodeBindAddresses();

        // Then
        assertThat(result).isEmpty();
    }

    @Test
    void shouldHaveNoExclusivePorts() {
        // Given - the bootstrap port is shared, not exclusive
        var binding = buildBindingSpec("boot.kafka:1234", "broker-$(nodeId).kafka");

        // When
        var result = binding.getExclusivePorts();

        // Then
        assertThat(result).isEmpty();
    }

    @Test
    void shouldShareBootstrapPort() {
        // Given
        var binding = buildBindingSpec("boot.kafka:1234", "broker-$(nodeId).kafka");

        // When
        var result = binding.getSharedPorts();

        // Then
        assertThat(result).isEqualTo(Set.of(1234));
    }

    @Test
    void shouldRequireServerNameIndication() {
        // Given
        var binding = buildBindingSpec("boot.kafka:1234", "broker-$(nodeId).kafka");

        // When / Then
        assertThat(binding.requiresServerNameIndication()).isTrue();
    }

    @Test
    void shouldBindOnAllInterfaces() {
        // Given
        var binding = buildBindingSpec("boot.kafka:1234", "broker-$(nodeId).kafka");

        // When / Then
        assertThat(binding.getBindAddress()).isEmpty();
    }

    // AdvertisingSpec

    @Test
    void advertiseBootstrapUsesGatewayResolvedPort() {
        // Given
        var advertising = buildAdvertisingSpec("boot.kafka:1234", "broker-$(nodeId).kafka", "cluster");
        var gateway = mock(EndpointGateway.class);
        when(gateway.resolvePort(any())).thenReturn(1234);

        // When
        var result = advertising.advertiseBootstrap(new ProxyNodeId.Bootstrap(gateway));

        // Then — delegates port resolution to the gateway
        assertThat(result).isEqualTo(new HostPort("boot.kafka", 1234));
    }

    @Test
    void advertiseBootstrapUsesResolvedPort() {
        // Given
        var advertising = buildAdvertisingSpec("boot.kafka:1234", "broker-$(nodeId).kafka", "cluster");
        var gateway = mock(EndpointGateway.class);
        when(gateway.resolvePort(any())).thenReturn(55555);

        // When
        var result = advertising.advertiseBootstrap(new ProxyNodeId.Bootstrap(gateway));

        // Then
        assertThat(result).isEqualTo(new HostPort("boot.kafka", 55555));
    }

    @Test
    void advertiseBrokerSubstitutesNodeIdAndUsesGatewayResolvedPort() {
        // Given — no explicit port in broker pattern, delegates to gateway
        var advertising = buildAdvertisingSpec("boot.kafka:1234", "broker-$(nodeId).kafka", "cluster");
        var gateway = mock(EndpointGateway.class);
        when(gateway.resolvePort(any())).thenReturn(1234);

        // When
        var result = advertising.advertiseBroker(new ProxyNodeId.Broker(gateway, 5));

        // Then — delegates port resolution to the gateway
        assertThat(result).isEqualTo(new HostPort("broker-5.kafka", 1234));
    }

    @Test
    void advertiseBrokerUsesResolvedPortWhenNoExplicitPortInPattern() {
        // Given — no explicit port in broker pattern
        var advertising = buildAdvertisingSpec("boot.kafka:1234", "broker-$(nodeId).kafka", "cluster");
        var gateway = mock(EndpointGateway.class);
        when(gateway.resolvePort(any())).thenAnswer(inv -> {
            ProxyNodeId.Broker broker = inv.getArgument(0);
            return 45000 + broker.nodeId();
        });

        // When
        var result = advertising.advertiseBroker(new ProxyNodeId.Broker(gateway, 5));

        // Then — resolver provides the port, not the bootstrap port
        assertThat(result).isEqualTo(new HostPort("broker-5.kafka", 45005));
    }

    @Test
    void advertiseBrokerUsesExplicitPortFromPatternEvenWhenResolverAvailable() {
        // Given — explicit port 9999 in broker address pattern (e.g. passthrough proxy)
        var advertising = buildAdvertisingSpec("boot.kafka:1234", "broker-$(nodeId).kafka:9999", "cluster");
        var gateway = mock(EndpointGateway.class);
        when(gateway.resolvePort(any())).thenReturn(88888);

        // When
        var result = advertising.advertiseBroker(new ProxyNodeId.Broker(gateway, 5));

        // Then — explicit port from pattern takes precedence over resolver
        assertThat(result).isEqualTo(new HostPort("broker-5.kafka", 9999));
    }

    @Test
    void advertiseBrokerSubstitutesClusterName() {
        // Given
        var advertising = buildAdvertisingSpec("boot.kafka:1234", "broker-$(virtualClusterName)-$(nodeId).kafka", "mycluster");
        var gateway = mock(EndpointGateway.class);

        // When
        var result = advertising.advertiseBroker(new ProxyNodeId.Broker(gateway, 3));

        // Then
        assertThat(result.host()).isEqualTo("broker-mycluster-3.kafka");
    }

    private static AddressingSpec buildAddressingSpec(String bootstrap, String pattern, String cluster) {
        return (AddressingSpec) new SniHostIdentifiesNodeIdentificationStrategy(bootstrap, pattern).buildStrategy(cluster);
    }

    private static BindingSpec buildBindingSpec(String bootstrap, String pattern) {
        return (BindingSpec) new SniHostIdentifiesNodeIdentificationStrategy(bootstrap, pattern).buildStrategy("cluster");
    }

    private static AdvertisingSpec buildAdvertisingSpec(String bootstrap, String pattern, String cluster) {
        return (AdvertisingSpec) new SniHostIdentifiesNodeIdentificationStrategy(bootstrap, pattern).buildStrategy(cluster);
    }
}
