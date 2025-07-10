/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.assertj;

import java.util.List;
import java.util.Optional;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.NamedRange;
import io.kroxylicious.proxy.config.PortIdentifiesNodeIdentificationStrategy;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.config.VirtualClusterGateway;
import io.kroxylicious.proxy.service.HostPort;

class ProxyConfigAssertTest {

    @Test
    void virtualClusterWhenNotContainedInConfig() {
        VirtualClusterGateway virtualClusterGateway = new VirtualClusterGateway("default",
                new PortIdentifiesNodeIdentificationStrategy(new HostPort("localhost", 9292), null, null, null), null, Optional.empty());
        VirtualCluster virtualCluster = new VirtualCluster("cluster", new TargetCluster("localhost:9092", Optional.empty()),
                List.of(virtualClusterGateway), false,
                false, List.of());
        Configuration config = new Configuration(null, null, null, List.of(virtualCluster), List.of(), false, Optional.empty());

        Assertions.assertThatThrownBy(() -> {
            OperatorAssertions.assertThat(config).cluster("arbitrary");
        }).hasMessage("proxy config does not contain a virtual cluster named 'arbitrary', clusters in config: [cluster]");
    }

    @Test
    void virtualClusterWhenContainedInConfig() {
        VirtualClusterGateway virtualClusterGateway = new VirtualClusterGateway("default",
                new PortIdentifiesNodeIdentificationStrategy(new HostPort("localhost", 9292), null, null, null), null, Optional.empty());
        String clusterName = "cluster";
        VirtualCluster virtualCluster = new VirtualCluster(clusterName, new TargetCluster("localhost:9092", Optional.empty()),
                List.of(virtualClusterGateway), false,
                false, List.of());
        Configuration config = new Configuration(null, null, null, List.of(virtualCluster), List.of(), false, Optional.empty());

        ProxyConfigAssert.ProxyConfigClusterAssert cluster = OperatorAssertions.assertThat(config).cluster(clusterName);
        Assertions.assertThat(cluster.actual()).isNotNull().isSameAs(virtualCluster);
    }

    @Test
    void virtualClusterWhenGatewayNotContainedInConfig() {
        VirtualClusterGateway virtualClusterGateway = new VirtualClusterGateway("default",
                new PortIdentifiesNodeIdentificationStrategy(new HostPort("localhost", 9292), null, null, null), null, Optional.empty());
        VirtualCluster virtualCluster = new VirtualCluster("cluster", new TargetCluster("localhost:9092", Optional.empty()),
                List.of(virtualClusterGateway), false,
                false, List.of());
        ProxyConfigAssert.ProxyConfigClusterAssert proxyConfigClusterAssert = new ProxyConfigAssert.ProxyConfigClusterAssert(virtualCluster);
        Assertions.assertThatThrownBy(() -> {
            proxyConfigClusterAssert.gateway("anything");
        }).hasMessage("gateways for cluster 'cluster' does not contain a gateway named 'anything', gateways in cluster: [default]");
    }

    @Test
    void virtualClusterWhenGatewayContainedInConfig() {
        String gatewayName = "default";
        VirtualClusterGateway virtualClusterGateway = new VirtualClusterGateway(gatewayName,
                new PortIdentifiesNodeIdentificationStrategy(new HostPort("localhost", 9292), null, null, null), null, Optional.empty());
        VirtualCluster virtualCluster = new VirtualCluster("cluster", new TargetCluster("localhost:9092", Optional.empty()),
                List.of(virtualClusterGateway), false,
                false, List.of());
        ProxyConfigAssert.ProxyConfigClusterAssert proxyConfigClusterAssert = new ProxyConfigAssert.ProxyConfigClusterAssert(virtualCluster);
        ProxyConfigAssert.ProxyConfigGatewayAssert gatewayAssert = proxyConfigClusterAssert.gateway(gatewayName);
        Assertions.assertThat(gatewayAssert.actual()).isNotNull().isSameAs(virtualClusterGateway);
    }

    @Test
    void portIdentifiesNodeGateway() {
        PortIdentifiesNodeIdentificationStrategy strategy = new PortIdentifiesNodeIdentificationStrategy(new HostPort("localhost", 9292), null, null, null);
        VirtualClusterGateway virtualClusterGateway = new VirtualClusterGateway("default",
                strategy, null, Optional.empty());
        ProxyConfigAssert.ProxyConfigGatewayAssert gatewayAssert = new ProxyConfigAssert.ProxyConfigGatewayAssert(virtualClusterGateway);
        ProxyConfigAssert.ProxyConfigPortIdentifiesNodeGatewayAssert portIdentifiesNodeGatewayAssert = gatewayAssert.portIdentifiesNode();
        Assertions.assertThat(portIdentifiesNodeGatewayAssert.actual()).isNotNull().isSameAs(strategy);
    }

    @Test
    void portIdentifiesNodeGatewayBootstrap() {
        PortIdentifiesNodeIdentificationStrategy strategy = new PortIdentifiesNodeIdentificationStrategy(new HostPort("localhost", 9292), null, null, null);
        ProxyConfigAssert.ProxyConfigPortIdentifiesNodeGatewayAssert portIdentifiesNodeGatewayAssert = new ProxyConfigAssert.ProxyConfigPortIdentifiesNodeGatewayAssert(
                strategy);
        portIdentifiesNodeGatewayAssert.hasBootstrapAddress(new HostPort("localhost", 9292));
        Assertions.assertThatThrownBy(() -> {
            portIdentifiesNodeGatewayAssert.hasBootstrapAddress(new HostPort("another", 9392));
        }).hasMessage("expected bootstrap address for gateway: 'another:9392' but was 'localhost:9292'");
    }

    @Test
    void portIdentifiesNodeGatewayNamedRangeNotExist() {
        PortIdentifiesNodeIdentificationStrategy strategy = new PortIdentifiesNodeIdentificationStrategy(new HostPort("localhost", 9292), null, null, null);
        ProxyConfigAssert.ProxyConfigPortIdentifiesNodeGatewayAssert portIdentifiesNodeGatewayAssert = new ProxyConfigAssert.ProxyConfigPortIdentifiesNodeGatewayAssert(
                strategy);
        Assertions.assertThatThrownBy(() -> {
            portIdentifiesNodeGatewayAssert.namedRange("notfound");
        }).hasMessage("gateway has no node id ranges configured");
    }

    @Test
    void portIdentifiesNodeGatewayNamedRangeNotFound() {
        NamedRange range = new NamedRange("range", 1, 3);
        PortIdentifiesNodeIdentificationStrategy strategy = new PortIdentifiesNodeIdentificationStrategy(new HostPort("localhost", 9292), null, null, List.of(range));
        ProxyConfigAssert.ProxyConfigPortIdentifiesNodeGatewayAssert portIdentifiesNodeGatewayAssert = new ProxyConfigAssert.ProxyConfigPortIdentifiesNodeGatewayAssert(
                strategy);
        Assertions.assertThatThrownBy(() -> {
            portIdentifiesNodeGatewayAssert.namedRange("notfound");
        }).hasMessage("node id ranges for gateway does not contain range named 'notfound', ranges in gateway config: [range]");
    }

    @Test
    void portIdentifiesNodeGatewayNamedRangeFound() {
        String rangeName = "range";
        NamedRange range = new NamedRange(rangeName, 1, 3);
        PortIdentifiesNodeIdentificationStrategy strategy = new PortIdentifiesNodeIdentificationStrategy(new HostPort("localhost", 9292), null, null, List.of(range));
        ProxyConfigAssert.ProxyConfigPortIdentifiesNodeGatewayAssert portIdentifiesNodeGatewayAssert = new ProxyConfigAssert.ProxyConfigPortIdentifiesNodeGatewayAssert(
                strategy);
        ProxyConfigAssert.NamedRangeAssert namedRangeAssert = portIdentifiesNodeGatewayAssert.namedRange(rangeName);
        Assertions.assertThat(namedRangeAssert.actual()).isNotNull().isSameAs(range);
    }

    @Test
    void namedRangeStart() {
        NamedRange range = new NamedRange("range", 1, 3);
        ProxyConfigAssert.NamedRangeAssert namedRangeAssert = new ProxyConfigAssert.NamedRangeAssert(range);
        namedRangeAssert.hasStart(1);
        Assertions.assertThatThrownBy(() -> {
            namedRangeAssert.hasStart(2);
        }).hasMessage("expected node id range start to be 2 but was 1");
    }

    @Test
    void namedRangeEnd() {
        NamedRange range = new NamedRange("range", 1, 3);
        ProxyConfigAssert.NamedRangeAssert namedRangeAssert = new ProxyConfigAssert.NamedRangeAssert(range);
        namedRangeAssert.hasEnd(3);
        Assertions.assertThatThrownBy(() -> {
            namedRangeAssert.hasEnd(2);
        }).hasMessage("expected node id range end to be 2 but was 3");
    }

}
