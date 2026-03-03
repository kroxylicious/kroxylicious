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
import org.junit.jupiter.params.converter.ConvertWith;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import io.kroxylicious.proxy.HostPortConverter;
import io.kroxylicious.proxy.service.HostPort;

import static io.kroxylicious.proxy.service.HostPort.parse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

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
        assertThat(strategy.getBrokerIdFromBrokerAddress(HostPort.parse("my-broker-my-cluster-1:1234"))).isEqualTo(1);
        assertThat(strategy.getBrokerIdFromBrokerAddress(HostPort.parse("my-broker-another-cluster-1:1234"))).isNull();
        assertThat(strategy.getAdvertisedBrokerAddress(1)).isEqualTo(HostPort.parse("my-broker-my-cluster-1:1234"));
    }

    @Test
    void valid() {
        var config = new SniHostIdentifiesNodeIdentificationStrategy(GOOD_HOST_PORT.toString(), "broker$(nodeId)-good");
        assertThat(config).isNotNull();
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
    @MethodSource("getBrokerIdFromBrokerAddress")
    void getBrokerIdFromAdvertisedBrokerAddress(String brokerAddressPattern, String clusterName, @ConvertWith(HostPortConverter.class) HostPort address,
                                                Integer expected) {
        var strategy = new SniHostIdentifiesNodeIdentificationStrategy(GOOD_HOST_PORT.toString(), brokerAddressPattern).buildStrategy(clusterName);

        assertThat(strategy.getBrokerIdFromBrokerAddress(address)).isEqualTo(expected);
    }

    @Test
    void badNodeId() {
        var strategy = new SniHostIdentifiesNodeIdentificationStrategy(GOOD_HOST_PORT.toString(), "broker-$(nodeId).kafka").buildStrategy("cluster");

        assertThatThrownBy(() -> strategy.getBrokerAddress(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
