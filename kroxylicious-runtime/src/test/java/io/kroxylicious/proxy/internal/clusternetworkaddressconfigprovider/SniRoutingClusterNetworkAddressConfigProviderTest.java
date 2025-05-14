/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider;

import java.util.Set;
import java.util.stream.Stream;

import org.assertj.core.api.ThrowableAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.converter.ConvertWith;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import io.kroxylicious.proxy.HostPortConverter;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.service.HostPort;

import static io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.SniRoutingClusterNetworkAddressConfigProvider.SniRoutingClusterNetworkAddressConfigProviderConfig;
import static io.kroxylicious.proxy.service.HostPort.parse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;
import static org.mockito.Mockito.when;

@SuppressWarnings("removal")
class SniRoutingClusterNetworkAddressConfigProviderTest {

    private static final HostPort GOOD_HOST_PORT = parse("boot.kafka:1234");

    @Test
    void valid() {
        var config = new SniRoutingClusterNetworkAddressConfigProviderConfig(GOOD_HOST_PORT, "broker$(nodeId)-good", null);
        assertThat(config).isNotNull();
    }

    @Test
    void missingBootstrap() {
        assertThatThrownBy(() -> new SniRoutingClusterNetworkAddressConfigProviderConfig(null, "broker$(nodeId)-good", null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void getBootstrapAddressFromConfig() {
        var config = new SniRoutingClusterNetworkAddressConfigProviderConfig(GOOD_HOST_PORT, "broker$(nodeId)-good", null);
        assertThat(config.getBootstrapAddress()).isEqualTo(GOOD_HOST_PORT);
    }

    @Test
    void getBrokerAddressPatternFromConfig() {
        var brokerAddressPattern = "broker$(nodeId)-good";
        var config = new SniRoutingClusterNetworkAddressConfigProviderConfig(GOOD_HOST_PORT, brokerAddressPattern, null);
        assertThat(config.getBrokerAddressPattern()).isEqualTo(brokerAddressPattern);
    }

    static Stream<Arguments> mustSupplyABrokerAddressPatternOrAdvertisedBrokerAddressPattern() {
        return Stream.of(argumentSet("with brokerAddressPattern", "broker-$(nodeId)-good", null, true),
                argumentSet("with advertisedBrokerAddressPattern", null, "broker-$(nodeId)-good", true),
                argumentSet("both disallowed", "broker-$(nodeId)-good", "broker-$(nodeId)-good", false),
                argumentSet("neither disallowed", null, null, false));
    }

    @ParameterizedTest
    @MethodSource
    void mustSupplyABrokerAddressPatternOrAdvertisedBrokerAddressPattern(String brokerAddress, String advertisedBrokerAddress, boolean valid) {
        ThrowableAssert.ThrowingCallable test = () -> {
            new SniRoutingClusterNetworkAddressConfigProviderConfig(parse("arbitrary:1235"), brokerAddress, advertisedBrokerAddress);
        };
        if (valid) {
            assertThatCode(test).doesNotThrowAnyException();
        }
        else {
            assertThatThrownBy(test).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "nonodetoken", "recursive$(nodeId$(nodeId))", "capitalisedrejected$(NODEID)", "noportalloweed-in-pattern-part$(nodeId):1234:1234" })
    @NullAndEmptySource
    void invalidBrokerAddressPattern(String input) {
        var goodHostPort = parse("good:1235");
        assertThatThrownBy(() -> new SniRoutingClusterNetworkAddressConfigProviderConfig(goodHostPort, input, (String) null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = { "nonodetoken", "recursive$(nodeId$(nodeId))", "capitalisedrejected$(NODEID)", "noportalloweed-in-pattern-part$(nodeId):1234:1234" })
    @NullAndEmptySource
    void invalidAdvertisedBrokerAddressPattern(String input) {
        var goodHostPort = parse("good:1235");
        assertThatThrownBy(() -> new SniRoutingClusterNetworkAddressConfigProviderConfig(goodHostPort, (String) null, input))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = { "broker$(nodeId)", "twice$(nodeId)allowed$(nodeId)too", "broker$(nodeId).kafka.com" })
    void validBrokerAddressPatterns(String input) {
        var goodHostPort = parse("good:1235");
        var config = new SniRoutingClusterNetworkAddressConfigProviderConfig(goodHostPort, input, null);
        assertThat(config).isNotNull();
    }

    @Test
    void getBrokerAddress() {
        var provider = new SniRoutingClusterNetworkAddressConfigProvider().build(
                new SniRoutingClusterNetworkAddressConfigProviderConfig(GOOD_HOST_PORT, "broker-$(nodeId).kafka", null), mockCluster("cluster"));
        assertThat(provider.getBrokerAddress(0)).isEqualTo(HostPort.parse("broker-0.kafka:1234"));
    }

    @Test
    void getAdvertisedBrokerAddress() {
        var provider = new SniRoutingClusterNetworkAddressConfigProvider().build(
                new SniRoutingClusterNetworkAddressConfigProviderConfig(GOOD_HOST_PORT, null, "broker-$(nodeId).kafka"), mockCluster("cluster"));
        assertThat(provider.getAdvertisedBrokerAddress(0)).isEqualTo(HostPort.parse("broker-0.kafka:1234"));
    }

    @Test
    void getAdvertisedBrokerAddressWithClusterNameReplacement() {
        var provider = new SniRoutingClusterNetworkAddressConfigProvider().build(
                new SniRoutingClusterNetworkAddressConfigProviderConfig(GOOD_HOST_PORT, null, "broker-$(virtualClusterName)-$(nodeId).kafka"), mockCluster("cluster"));
        assertThat(provider.getAdvertisedBrokerAddress(0)).isEqualTo(HostPort.parse("broker-cluster-0.kafka:1234"));
    }

    @Test
    void getClusterBootstrapAddress() {
        var provider = new SniRoutingClusterNetworkAddressConfigProvider().build(
                new SniRoutingClusterNetworkAddressConfigProviderConfig(GOOD_HOST_PORT, "broker-$(nodeId).kafka", null), mockCluster("cluster"));
        assertThat(provider.getClusterBootstrapAddress()).isEqualTo(GOOD_HOST_PORT);
    }

    @Test
    void getSharedPorts() {
        var provider = new SniRoutingClusterNetworkAddressConfigProvider().build(
                new SniRoutingClusterNetworkAddressConfigProviderConfig(GOOD_HOST_PORT, "broker-$(nodeId).kafka", null), mockCluster("cluster"));
        assertThat(provider.getSharedPorts()).isEqualTo(Set.of(GOOD_HOST_PORT.port()));
    }

    @Test
    void requiresServerNameIndication() {
        var provider = new SniRoutingClusterNetworkAddressConfigProvider().build(
                new SniRoutingClusterNetworkAddressConfigProviderConfig(GOOD_HOST_PORT, "broker-$(nodeId).kafka", null), mockCluster("cluster"));
        assertThat(provider.requiresServerNameIndication()).isTrue();
    }

    @Test
    void getAdvertisedPortForDeprecatedBrokerAddressPatternIsBootstrapBrokerAddress() {
        var provider = new SniRoutingClusterNetworkAddressConfigProvider().build(
                new SniRoutingClusterNetworkAddressConfigProviderConfig(GOOD_HOST_PORT, "broker-$(nodeId).kafka", null), mockCluster("cluster"));
        assertThat(provider.getAdvertisedBrokerAddress(0)).isEqualTo(HostPort.parse("broker-0.kafka:1234"));
    }

    @Test
    void getBrokerAddressPrefersAdvertisedPortIfProvided() {
        var provider = new SniRoutingClusterNetworkAddressConfigProvider().build(
                new SniRoutingClusterNetworkAddressConfigProviderConfig(GOOD_HOST_PORT, (String) null, "broker-$(nodeId).kafka:443"),
                mockCluster("cluster"));
        assertThat(provider.getBrokerAddress(0)).isEqualTo(HostPort.parse("broker-0.kafka:1234"));
    }

    @Test
    void getAdvertisedPortPrefersAdvertisedBrokerAddressIfSpecified() {
        var provider = new SniRoutingClusterNetworkAddressConfigProvider().build(
                new SniRoutingClusterNetworkAddressConfigProviderConfig(GOOD_HOST_PORT, null, "broker-$(nodeId).kafka:443"), mockCluster("cluster"));
        assertThat(provider.getAdvertisedBrokerAddress(0)).isEqualTo(HostPort.parse("broker-0.kafka:443"));
    }

    @Test
    void getAdvertisedPortDefaultsToBootstrapBrokerAddressIfNotSpecified() {
        var provider = new SniRoutingClusterNetworkAddressConfigProvider().build(
                new SniRoutingClusterNetworkAddressConfigProviderConfig(GOOD_HOST_PORT, (String) null, "broker-$(nodeId).kafka"),
                mockCluster("cluster"));
        assertThat(provider.getAdvertisedBrokerAddress(0)).isEqualTo(HostPort.parse("broker-0.kafka:1234"));
    }

    static Stream<Arguments> getBrokerIdFromBrokerAddress() {
        return Stream.of(
                argumentSet("broker 0", "broker-$(nodeId).kafka", "cluster", "broker-0.kafka:1234", 0),
                argumentSet("broker 99", "broker-$(nodeId).kafka", "cluster", "broker-99.kafka:1234", 99),
                argumentSet("nodeId replacement in last position", "broker-$(nodeId)", "cluster", "broker-99:1234", 99),
                argumentSet("RFC 4343 case insensitive", "broker-$(nodeId).kafka", "cluster", "BROKER-0.KAFKA:1234", 0),
                argumentSet("port mismatch", "broker-$(nodeId).kafka", "cluster", "broker-0.kafka:1235", null),
                argumentSet("host mismatch", "broker-$(nodeId).kafka", "cluster", "broker-0.another:1234", null),
                argumentSet("RE anchoring", "broker-$(nodeId).kafka", "cluster", "0.kafka:1234", null),
                argumentSet("RE anchoring", "broker-$(nodeId).kafka", "cluster", "start.broker-0.kafka.end:1234", null),
                argumentSet("RE metacharacters in brokerAddressPattern escaped", "broker-$(nodeId).kafka", "cluster", "broker-0xkafka:1234", null),
                argumentSet("cluster name replacement broker 0", "broker-$(virtualClusterName)-$(nodeId).kafka", "cluster", "broker-cluster-0.kafka:1234", 0),
                argumentSet("cluster name replacement in last position", "broker-$(nodeId)-$(virtualClusterName)", "cluster", "broker-0-cluster:1234", 0),
                argumentSet("cluster name replacement broker 99", "broker-$(virtualClusterName)-$(nodeId).kafka", "cluster", "broker-cluster-99.kafka:1234", 99),
                argumentSet("cluster name replacement nodeId replacement in last position", "broker-$(virtualClusterName)-$(nodeId)", "cluster", "broker-cluster-99:1234",
                        99),
                argumentSet("cluster name replacement RFC 4343 case insensitive", "broker-$(virtualClusterName)-$(nodeId).kafka", "cluster",
                        "BROKER-CLUSTER-0.KAFKA:1234", 0),
                argumentSet("cluster name replacement port mismatch", "broker-$(virtualClusterName)-$(nodeId).kafka", "cluster", "broker-cluster-0.kafka:1235", null),
                argumentSet("cluster name replacement host mismatch", "broker-$(virtualClusterName)-$(nodeId).kafka", "cluster", "broker-cluster-0.another:1234", null),
                argumentSet("cluster name replacement RE anchoring", "broker-$(virtualClusterName)-$(nodeId).kafka", "cluster", "-cluster-0.kafka:1234", null),
                argumentSet("cluster name replacement RE anchoring", "broker-$(virtualClusterName)-$(nodeId).kafka", "cluster", "start.broker-cluster-0.kafka.end:1234",
                        null),
                argumentSet("cluster name replacement RE metacharacters in brokerAddressPattern escaped", "broker-$(virtualClusterName)-$(nodeId).kafka", "cluster",
                        "broker-cluster-0xkafka:1234", null));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void getBrokerIdFromBrokerAddress(String brokerAddressPattern, String clusterName, @ConvertWith(HostPortConverter.class) HostPort address, Integer expected) {
        var provider = new SniRoutingClusterNetworkAddressConfigProvider().build(
                new SniRoutingClusterNetworkAddressConfigProviderConfig(GOOD_HOST_PORT, brokerAddressPattern, null), mockCluster(clusterName));

        assertThat(provider.getBrokerIdFromBrokerAddress(address)).isEqualTo(expected);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("getBrokerIdFromBrokerAddress")
    void getBrokerIdFromAdvertisedBrokerAddress(String brokerAddressPattern, String clusterName, @ConvertWith(HostPortConverter.class) HostPort address,
                                                Integer expected) {
        var provider = new SniRoutingClusterNetworkAddressConfigProvider().build(
                new SniRoutingClusterNetworkAddressConfigProviderConfig(GOOD_HOST_PORT, null, brokerAddressPattern), mockCluster(clusterName));

        assertThat(provider.getBrokerIdFromBrokerAddress(address)).isEqualTo(expected);
    }

    private static VirtualClusterModel mockCluster(String clusterName) {
        VirtualClusterModel cluster = Mockito.mock(VirtualClusterModel.class);
        when(cluster.getClusterName()).thenReturn(clusterName);
        return cluster;
    }

    @Test
    void badNodeId() {
        var config = new SniRoutingClusterNetworkAddressConfigProviderConfig(GOOD_HOST_PORT, "broker-$(nodeId).kafka", null);
        var service = new SniRoutingClusterNetworkAddressConfigProvider();
        var provider = service.build(config, mockCluster("cluster"));

        assertThatThrownBy(() -> provider.getBrokerAddress(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
