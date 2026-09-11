/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.operator.assertj;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.api.Assertions;

import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.NamedRange;
import io.kroxylicious.proxy.config.PortIdentifiesNodeIdentificationStrategy;
import io.kroxylicious.proxy.config.SniHostIdentifiesNodeIdentificationStrategy;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.config.VirtualClusterGateway;
import io.kroxylicious.proxy.service.HostPort;

import static java.util.stream.Collectors.toSet;

/**
 * Assertions on a proxy {@link Configuration}, allowing navigation into its virtual clusters,
 * gateways and node identification strategies.
 */
public class ProxyConfigAssert extends AbstractObjectAssert<ProxyConfigAssert, Configuration> {
    /**
     * Creates an assertion on the given configuration.
     *
     * @param config the configuration to assert on
     */
    public ProxyConfigAssert(Configuration config) {
        super(config, ProxyConfigAssert.class);
    }

    /**
     * Creates an assertion on the given configuration.
     *
     * @param actual the configuration to assert on
     * @return a new assertion
     */
    public static ProxyConfigAssert assertThat(Configuration actual) {
        return new ProxyConfigAssert(actual);
    }

    /**
     * Asserts that the configuration contains a virtual cluster with the given name and
     * returns an assertion on it.
     *
     * @param clusterName the virtual cluster name
     * @return an assertion on the named virtual cluster
     */
    public ProxyConfigClusterAssert cluster(String clusterName) {
        Set<String> names = Optional.ofNullable(this.actual.virtualClusters()).orElse(List.of()).stream().map(VirtualCluster::name).collect(toSet());
        Assertions.assertThat(names).withFailMessage("proxy config contains no virtual clusters").isNotEmpty()
                .withFailMessage("proxy config does not contain a virtual cluster named '" + clusterName + "', clusters in config: " + names).contains(clusterName);
        List<VirtualCluster> list = this.actual.virtualClusters().stream().filter(x -> x.name().equals(clusterName)).toList();
        Assertions.assertThat(list).hasSize(1);
        VirtualCluster virtualCluster = list.get(0);
        return new ProxyConfigClusterAssert(virtualCluster);
    }

    /**
     * Assertions on a {@link VirtualCluster} within a proxy configuration.
     */
    public static class ProxyConfigClusterAssert extends AbstractObjectAssert<ProxyConfigClusterAssert, VirtualCluster> {
        /**
         * Creates an assertion on the given virtual cluster.
         *
         * @param virtualCluster the virtual cluster to assert on
         */
        public ProxyConfigClusterAssert(VirtualCluster virtualCluster) {
            super(virtualCluster, ProxyConfigClusterAssert.class);
        }

        /**
         * Asserts that the virtual cluster contains a gateway with the given name and
         * returns an assertion on it.
         *
         * @param gateway the gateway name
         * @return an assertion on the named gateway
         */
        public ProxyConfigGatewayAssert gateway(String gateway) {
            Set<String> names = this.actual.gateways().stream().map(VirtualClusterGateway::name).collect(toSet());
            Assertions.assertThat(names).withFailMessage(
                    "gateways for cluster '" + this.actual.name() + "' does not contain a gateway named '" + gateway + "', gateways in cluster: " + names)
                    .contains(gateway);
            List<VirtualClusterGateway> list = this.actual.gateways().stream().filter(x -> x.name().equals(gateway)).toList();
            Assertions.assertThat(list).hasSize(1);
            VirtualClusterGateway virtualCluster = list.get(0);
            return new ProxyConfigGatewayAssert(virtualCluster);
        }
    }

    /**
     * Assertions on a {@link VirtualClusterGateway} within a proxy configuration.
     */
    public static class ProxyConfigGatewayAssert extends AbstractObjectAssert<ProxyConfigGatewayAssert, VirtualClusterGateway> {

        /**
         * Creates an assertion on the given gateway.
         *
         * @param virtualClusterGateway the gateway to assert on
         */
        public ProxyConfigGatewayAssert(VirtualClusterGateway virtualClusterGateway) {
            super(virtualClusterGateway, ProxyConfigGatewayAssert.class);
        }

        /**
         * Asserts that the gateway uses the port-identifies-node identification strategy and
         * returns an assertion on it.
         *
         * @return an assertion on the port-identifies-node strategy
         */
        public ProxyConfigPortIdentifiesNodeGatewayAssert portIdentifiesNode() {
            Assertions.assertThat(actual.portIdentifiesNode()).isNotNull();
            return new ProxyConfigPortIdentifiesNodeGatewayAssert(actual.portIdentifiesNode());
        }

        /**
         * Asserts that the gateway uses the SNI-host-identifies-node identification strategy and
         * returns an assertion on it.
         *
         * @return an assertion on the SNI-host-identifies-node strategy
         */
        public ProxyConfigSniHostIdentifiesNodeGatewayAssert sniHostIdentifiesNode() {
            Assertions.assertThat(actual.sniHostIdentifiesNode()).isNotNull();
            return new ProxyConfigSniHostIdentifiesNodeGatewayAssert(actual.sniHostIdentifiesNode());
        }

    }

    /**
     * Assertions on a {@link SniHostIdentifiesNodeIdentificationStrategy} within a proxy configuration.
     */
    public static class ProxyConfigSniHostIdentifiesNodeGatewayAssert
            extends AbstractObjectAssert<ProxyConfigSniHostIdentifiesNodeGatewayAssert, SniHostIdentifiesNodeIdentificationStrategy> {

        /**
         * Creates an assertion on the given strategy.
         *
         * @param sniHostIdentifiesNodeIdentificationStrategy the strategy to assert on
         */
        public ProxyConfigSniHostIdentifiesNodeGatewayAssert(SniHostIdentifiesNodeIdentificationStrategy sniHostIdentifiesNodeIdentificationStrategy) {
            super(sniHostIdentifiesNodeIdentificationStrategy, ProxyConfigSniHostIdentifiesNodeGatewayAssert.class);
        }

        /**
         * Asserts that the strategy has the given bootstrap address.
         *
         * @param bootstrapAddress the expected bootstrap address
         * @return this assertion
         */
        public ProxyConfigSniHostIdentifiesNodeGatewayAssert hasBootstrapAddress(String bootstrapAddress) {
            Assertions.assertThat(actual.getBootstrapAddress()).isEqualTo(bootstrapAddress);
            return this;
        }

        /**
         * Asserts that the strategy has the given advertised broker address pattern.
         *
         * @param advertisedBrokerAddressPattern the expected advertised broker address pattern
         * @return this assertion
         */
        public ProxyConfigSniHostIdentifiesNodeGatewayAssert hasAdvertisedBrokerAddressPattern(String advertisedBrokerAddressPattern) {
            Assertions.assertThat(actual.getAdvertisedBrokerAddressPattern()).isEqualTo(advertisedBrokerAddressPattern);
            return this;
        }
    }

    /**
     * Assertions on a {@link PortIdentifiesNodeIdentificationStrategy} within a proxy configuration.
     */
    public static class ProxyConfigPortIdentifiesNodeGatewayAssert
            extends AbstractObjectAssert<ProxyConfigPortIdentifiesNodeGatewayAssert, PortIdentifiesNodeIdentificationStrategy> {

        /**
         * Creates an assertion on the given strategy.
         *
         * @param virtualClusterGateway the strategy to assert on
         */
        public ProxyConfigPortIdentifiesNodeGatewayAssert(PortIdentifiesNodeIdentificationStrategy virtualClusterGateway) {
            super(virtualClusterGateway, ProxyConfigPortIdentifiesNodeGatewayAssert.class);
        }

        /**
         * Asserts that the strategy contains a node id range with the given name and
         * returns an assertion on it.
         *
         * @param name the node id range name
         * @return an assertion on the named node id range
         */
        public NamedRangeAssert namedRange(String name) {
            Set<String> names = Optional.ofNullable(this.actual.getNodeIdRanges()).orElse(List.of()).stream().map(NamedRange::name).collect(toSet());
            Assertions.assertThat(names)
                    .withFailMessage("gateway has no node id ranges configured").isNotEmpty()
                    .withFailMessage("node id ranges for gateway does not contain range named '" + name + "', ranges in gateway config: " + names)
                    .contains(name);
            List<NamedRange> namedRanges = Optional.ofNullable(actual.getNodeIdRanges()).orElse(List.of());
            List<NamedRange> ranges = namedRanges.stream().filter(r -> r.name().equals(name)).toList();
            Assertions.assertThat(ranges).hasSize(1);
            NamedRange namedRange = ranges.get(0);
            return new NamedRangeAssert(namedRange);
        }

        /**
         * Asserts that the strategy has the given bootstrap address.
         *
         * @param expected the expected bootstrap address
         * @return this assertion
         */
        public ProxyConfigPortIdentifiesNodeGatewayAssert hasBootstrapAddress(HostPort expected) {
            Assertions.assertThat(actual.getBootstrapAddress())
                    .withFailMessage("expected bootstrap address for gateway: '" + expected + "' but was '" + actual.getBootstrapAddress() + "'")
                    .isEqualTo(expected);
            return this;
        }

        /**
         * Asserts that the strategy has no node start port configured.
         *
         * @return this assertion
         */
        public ProxyConfigPortIdentifiesNodeGatewayAssert hasNullNodeStartPort() {
            Assertions.assertThat(actual.getNodeStartPort()).describedAs("node start port").isNull();
            return this;
        }
    }

    /**
     * Assertions on a {@link NamedRange} within a proxy configuration.
     */
    public static class NamedRangeAssert extends AbstractObjectAssert<NamedRangeAssert, NamedRange> {

        /**
         * Creates an assertion on the given range.
         *
         * @param namedRange the range to assert on
         */
        public NamedRangeAssert(NamedRange namedRange) {
            super(namedRange, NamedRangeAssert.class);
        }

        /**
         * Asserts that the range starts at the given node id.
         *
         * @param expected the expected range start
         * @return this assertion
         */
        public NamedRangeAssert hasStart(int expected) {
            Assertions.assertThat(actual.start()).withFailMessage("expected node id range start to be " + expected + " but was " + actual.start()).isEqualTo(expected);
            return this;
        }

        /**
         * Asserts that the range ends at the given node id.
         *
         * @param expected the expected range end
         * @return this assertion
         */
        public NamedRangeAssert hasEnd(int expected) {
            Assertions.assertThat(actual.end()).withFailMessage("expected node id range end to be " + expected + " but was " + actual.end()).isEqualTo(expected);
            return this;
        }

    }
}
