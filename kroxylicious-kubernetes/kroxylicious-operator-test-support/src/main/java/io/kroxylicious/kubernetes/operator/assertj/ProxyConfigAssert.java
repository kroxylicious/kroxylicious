/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.assertj;

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

public class ProxyConfigAssert extends AbstractObjectAssert<ProxyConfigAssert, Configuration> {
    public ProxyConfigAssert(Configuration config) {
        super(config, ProxyConfigAssert.class);
    }

    public static ProxyConfigAssert assertThat(Configuration actual) {
        return new ProxyConfigAssert(actual);
    }

    public ProxyConfigClusterAssert cluster(String clusterName) {
        Set<String> names = Optional.ofNullable(this.actual.virtualClusters()).orElse(List.of()).stream().map(VirtualCluster::name).collect(toSet());
        Assertions.assertThat(names).withFailMessage("proxy config contains no virtual clusters").isNotEmpty()
                .withFailMessage("proxy config does not contain a virtual cluster named '" + clusterName + "', clusters in config: " + names).contains(clusterName);
        List<VirtualCluster> list = this.actual.virtualClusters().stream().filter(x -> x.name().equals(clusterName)).toList();
        Assertions.assertThat(list).hasSize(1);
        VirtualCluster virtualCluster = list.get(0);
        return new ProxyConfigClusterAssert(virtualCluster);
    }

    public static class ProxyConfigClusterAssert extends AbstractObjectAssert<ProxyConfigClusterAssert, VirtualCluster> {
        public ProxyConfigClusterAssert(VirtualCluster virtualCluster) {
            super(virtualCluster, ProxyConfigClusterAssert.class);
        }

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

    public static class ProxyConfigGatewayAssert extends AbstractObjectAssert<ProxyConfigGatewayAssert, VirtualClusterGateway> {

        public ProxyConfigGatewayAssert(VirtualClusterGateway virtualClusterGateway) {
            super(virtualClusterGateway, ProxyConfigGatewayAssert.class);
        }

        public ProxyConfigPortIdentifiesNodeGatewayAssert portIdentifiesNode() {
            Assertions.assertThat(actual.portIdentifiesNode()).isNotNull();
            return new ProxyConfigPortIdentifiesNodeGatewayAssert(actual.portIdentifiesNode());
        }

        public ProxyConfigSniHostIdentifiesNodeGatewayAssert sniHostIdentifiesNode() {
            Assertions.assertThat(actual.sniHostIdentifiesNode()).isNotNull();
            return new ProxyConfigSniHostIdentifiesNodeGatewayAssert(actual.sniHostIdentifiesNode());
        }

    }

    public static class ProxyConfigSniHostIdentifiesNodeGatewayAssert
            extends AbstractObjectAssert<ProxyConfigSniHostIdentifiesNodeGatewayAssert, SniHostIdentifiesNodeIdentificationStrategy> {

        public ProxyConfigSniHostIdentifiesNodeGatewayAssert(SniHostIdentifiesNodeIdentificationStrategy sniHostIdentifiesNodeIdentificationStrategy) {
            super(sniHostIdentifiesNodeIdentificationStrategy, ProxyConfigSniHostIdentifiesNodeGatewayAssert.class);
        }

        public ProxyConfigSniHostIdentifiesNodeGatewayAssert hasBootstrapAddress(String bootstrapAddress) {
            Assertions.assertThat(actual.getBootstrapAddress()).isEqualTo(bootstrapAddress);
            return this;
        }

        public ProxyConfigSniHostIdentifiesNodeGatewayAssert hasAdvertisedBrokerAddressPattern(String advertisedBrokerAddressPattern) {
            Assertions.assertThat(actual.getAdvertisedBrokerAddressPattern()).isEqualTo(advertisedBrokerAddressPattern);
            return this;
        }
    }

    public static class ProxyConfigPortIdentifiesNodeGatewayAssert
            extends AbstractObjectAssert<ProxyConfigPortIdentifiesNodeGatewayAssert, PortIdentifiesNodeIdentificationStrategy> {

        public ProxyConfigPortIdentifiesNodeGatewayAssert(PortIdentifiesNodeIdentificationStrategy virtualClusterGateway) {
            super(virtualClusterGateway, ProxyConfigPortIdentifiesNodeGatewayAssert.class);
        }

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

        public ProxyConfigPortIdentifiesNodeGatewayAssert hasBootstrapAddress(HostPort expected) {
            Assertions.assertThat(actual.getBootstrapAddress())
                    .withFailMessage("expected bootstrap address for gateway: '" + expected + "' but was '" + actual.getBootstrapAddress() + "'")
                    .isEqualTo(expected);
            return this;
        }

        public ProxyConfigPortIdentifiesNodeGatewayAssert hasNullNodeStartPort() {
            Assertions.assertThat(actual.getNodeStartPort()).describedAs("node start port").isNull();
            return this;
        }
    }

    public static class NamedRangeAssert extends AbstractObjectAssert<NamedRangeAssert, NamedRange> {

        public NamedRangeAssert(NamedRange namedRange) {
            super(namedRange, NamedRangeAssert.class);
        }

        public NamedRangeAssert hasStart(int expected) {
            Assertions.assertThat(actual.start()).withFailMessage("expected node id range start to be " + expected + " but was " + actual.start()).isEqualTo(expected);
            return this;
        }

        public NamedRangeAssert hasEnd(int expected) {
            Assertions.assertThat(actual.end()).withFailMessage("expected node id range end to be " + expected + " but was " + actual.end()).isEqualTo(expected);
            return this;
        }

    }
}
