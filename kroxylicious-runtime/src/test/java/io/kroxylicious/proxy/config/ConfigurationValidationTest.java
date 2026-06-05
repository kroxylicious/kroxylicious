/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConfigurationValidationTest {

    private static final VirtualCluster SIMPLE_VC = new VirtualCluster("demo",
            new TargetCluster("broker:9092", Optional.empty()),
            List.of(simpleGateway("gw")),
            false, false, null);

    private static VirtualClusterGateway simpleGateway(String name) {
        return new VirtualClusterGateway(name,
                new PortIdentifiesNodeIdentificationStrategy(
                        new io.kroxylicious.proxy.service.HostPort("localhost", 9192), null, null, null),
                null, Optional.empty());
    }

    private static Configuration config(List<VirtualCluster> vcs) {
        return new Configuration(null, null, null, null, null, vcs, null, false, Optional.empty(), null, null);
    }

    // Cluster definition validation

    @Test
    void shouldRejectDuplicateClusterDefinitionNames() {
        var clusters = List.of(
                new ClusterDefinition("dup", "broker1:9092", null),
                new ClusterDefinition("dup", "broker2:9092", null));

        assertThatThrownBy(() -> new Configuration(null, clusters, null, null, null,
                List.of(SIMPLE_VC), null, false, Optional.empty(), null, null))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("duplicate names")
                .hasMessageContaining("dup");
    }

    @Test
    void shouldAcceptNullClusterDefinitions() {
        assertThatCode(() -> config(List.of(SIMPLE_VC)))
                .doesNotThrowAnyException();
    }

    // Router definition validation

    @Test
    void shouldRejectDuplicateRouterDefinitionNames() {
        var cluster = new ClusterDefinition("c1", "broker:9092", null);
        var route = new RouteDefinition("route1", 0, null, new RouteTarget("c1", null));
        var routers = List.of(
                new RouterDefinition("dup", "Type", null, List.of(route)),
                new RouterDefinition("dup", "Type", null, List.of(route)));

        assertThatThrownBy(() -> new Configuration(null, List.of(cluster), null, null, routers,
                List.of(SIMPLE_VC), null, false, Optional.empty(), null, null))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("duplicate names")
                .hasMessageContaining("dup");
    }

    // Virtual cluster target validation

    @Test
    void shouldRejectUnknownNamedTargetCluster() {
        var vcWithNamedTarget = new VirtualCluster("demo", null,
                new RouteTarget("nonexistent", null),
                List.of(simpleGateway("gw")), false, false, null, null, null, null);

        assertThatThrownBy(() -> config(List.of(vcWithNamedTarget)))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("unknown target cluster")
                .hasMessageContaining("nonexistent");
    }

    @Test
    void shouldAcceptKnownNamedTargetCluster() {
        var cluster = new ClusterDefinition("known", "broker:9092", null);
        var vcWithNamedTarget = new VirtualCluster("demo", null,
                new RouteTarget("known", null),
                List.of(simpleGateway("gw")), false, false, null, null, null, null);

        assertThatCode(() -> new Configuration(null, List.of(cluster), null, null, null,
                List.of(vcWithNamedTarget), null, false, Optional.empty(), null, null))
                .doesNotThrowAnyException();
    }

    // TODO in future VCs will be allowed to target a router
    @Test
    void shouldRejectRouterTarget() {
        RouteTarget routerTarget = new RouteTarget(null, "nonexistent");
        List<VirtualClusterGateway> gateways = List.of(simpleGateway("gw"));
        assertThatThrownBy(() -> new VirtualCluster("demo", null,
                routerTarget,
                gateways, false, false, null, null, null, null))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("illegal target.router for virtual cluster 'demo'.");
    }

    // rejectUnsupportedRoutingConfig

    @Test
    void virtualClusterModelRejectsRouterDefinitions() {
        var cluster = new ClusterDefinition("c1", "broker:9092", null);
        var route = new RouteDefinition("r", 0, null, new RouteTarget("c1", null));
        var router = new RouterDefinition("myrouter", "Type", null, List.of(route));

        var config = new Configuration(null, List.of(cluster), null, null, List.of(router),
                List.of(SIMPLE_VC), null, false, Optional.empty(), null, null);

        assertThatThrownBy(() -> config.virtualClusterModel(null))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("Routing is not yet supported");
    }

    // Convenience methods

    @Test
    void getMicrometerReturnsEmptyListWhenNull() {
        var config = config(List.of(SIMPLE_VC));

        assertThat(config.getMicrometer()).isEmpty();
    }

    @Test
    void getMicrometerReturnsConfiguredValue() {
        var micrometer = List.of(new MicrometerDefinition("JmxMeterRegistry", null));
        var config = new Configuration(null, null, null, null, null, List.of(SIMPLE_VC), micrometer, false, Optional.empty(), null, null);

        assertThat(config.getMicrometer()).isEqualTo(micrometer);
    }

    @Test
    void isUseIoUringReturnsConfiguredValue() {
        var config = new Configuration(null, null, null, null, null, List.of(SIMPLE_VC), null, true, Optional.empty(), null, null);

        assertThat(config.isUseIoUring()).isTrue();
    }

    // Filter validation with routes

    @Test
    void shouldRejectFilterInRouteNotDefinedInFilterDefinitions() {
        var cluster = new ClusterDefinition("c1", "broker:9092", null);
        var filterDefs = List.of(new NamedFilterDefinition("f1", "Type1", null));
        var route = new RouteDefinition("r", 0, List.of("undefined-filter"), new RouteTarget("c1", null));
        var router = new RouterDefinition("myrouter", "Type", null, List.of(route));

        assertThatThrownBy(() -> new Configuration(null, List.of(cluster), filterDefs, null, List.of(router),
                List.of(SIMPLE_VC), null, false, Optional.empty(), null, null))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("references filters not defined")
                .hasMessageContaining("undefined-filter");
    }

    @Test
    void shouldAcceptFilterUsedInRouteAsUsed() {
        var cluster = new ClusterDefinition("c1", "broker:9092", null);
        var filterDefs = List.of(new NamedFilterDefinition("f1", "Type1", null));
        var route = new RouteDefinition("r", 0, List.of("f1"), new RouteTarget("c1", null));
        var router = new RouterDefinition("myrouter", "Type", null, List.of(route));

        assertThatCode(() -> new Configuration(null, List.of(cluster), filterDefs, null, List.of(router),
                List.of(SIMPLE_VC), null, false, Optional.empty(), null, null))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldRejectNoVirtualClusters() {
        assertThatThrownBy(() -> new Configuration(null, null, null, null, null, List.of(), null, false, Optional.empty(), null, null))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("At least one virtual cluster");
    }

    @Test
    void shouldRejectNullVirtualClusters() {
        assertThatThrownBy(() -> new Configuration(null, null, null, null, null, null, null, false, Optional.empty(), null, null))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("At least one virtual cluster");
    }

    @Test
    void shouldRejectDuplicateVirtualClusterNamesCaseInsensitive() {
        var vc1 = new VirtualCluster("demo", new TargetCluster("b1:9092", Optional.empty()),
                List.of(simpleGateway("gw1")), false, false, null);
        var vc2 = new VirtualCluster("DEMO", new TargetCluster("b2:9092", Optional.empty()),
                List.of(simpleGateway("gw2")), false, false, null);

        assertThatThrownBy(() -> config(List.of(vc1, vc2)))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("duplicated");
    }
}
