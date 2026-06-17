/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.internal.routing.RouteDescriptor;
import io.kroxylicious.proxy.model.VirtualClusterModel;

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

    @Test
    void virtualClusterWithRouterTarget() {
        RouteTarget routerTarget = new RouteTarget(null, "nonexistent");
        List<VirtualClusterGateway> gateways = List.of(simpleGateway("gw"));
        VirtualCluster demo = new VirtualCluster("demo", null,
                routerTarget,
                gateways, false, false, null, null, null, null);
        assertThat(demo.target()).isEqualTo(routerTarget);
    }

    @Test
    void virtualClusterModelWithRouterDefinitions() {
        var cluster = new ClusterDefinition("c1", "broker:9092", null);
        var route = new RouteDefinition("r", 0, null, new RouteTarget("c1", null));
        var router = new RouterDefinition("myrouter", "Type", null, List.of(route));
        VirtualCluster virtualCluster = new VirtualCluster("demo", null,
                new RouteTarget(null, "myrouter"),
                List.of(simpleGateway("gateway")), false, false, null, null, null, null);
        var config = new Configuration(null, List.of(cluster), null, null, List.of(router),
                List.of(virtualCluster), null, false, Optional.empty(), null, null);
        List<VirtualClusterModel> virtualClusterModels = config.virtualClusterModel(null);
        assertThat(virtualClusterModels).hasSize(1).singleElement().satisfies(vm -> {
            assertThat(vm.usesRouter()).isTrue();
            assertThat(vm.routerName()).isEqualTo("myrouter");
        });
    }

    @Test
    void routerResolvesRouteDescriptors() {
        var cluster = new ClusterDefinition("c1", "broker:9092", null);
        var route = new RouteDefinition("route1", 0, null, new RouteTarget("c1", null));
        var router = new RouterDefinition("myrouter", "Type", null, List.of(route));
        var vc = new VirtualCluster("demo", null,
                new RouteTarget(null, "myrouter"),
                List.of(simpleGateway("gw")), false, false, null, null, null, null);
        var config = new Configuration(null, List.of(cluster), null, null, List.of(router),
                List.of(vc), null, false, Optional.empty(), null, null);

        var model = config.virtualClusterModel(null).get(0);

        assertThat(model.routeDescriptors()).containsKey("route1");
        RouteDescriptor rd = model.routeDescriptors().get("route1");
        assertThat(rd.name()).isEqualTo("route1");
        assertThat(rd.id()).isZero();
        assertThat(rd.targetsCluster()).isTrue();
        assertThat(rd.targetCluster()).isNotNull();
    }

    @Test
    void routerResolvesPrimaryTargetClusterFromFirstRoute() {
        var c1 = new ClusterDefinition("c1", "broker1:9092", null);
        var c2 = new ClusterDefinition("c2", "broker2:9092", null);
        var route1 = new RouteDefinition("r1", 0, null, new RouteTarget("c1", null));
        var route2 = new RouteDefinition("r2", 1, null, new RouteTarget("c2", null));
        var router = new RouterDefinition("myrouter", "Type", null, List.of(route1, route2));
        var vc = new VirtualCluster("demo", null,
                new RouteTarget(null, "myrouter"),
                List.of(simpleGateway("gw")), false, false, null, null, null, null);
        var config = new Configuration(null, List.of(c1, c2), null, null, List.of(router),
                List.of(vc), null, false, Optional.empty(), null, null);

        var model = config.virtualClusterModel(null).get(0);

        assertThat(model.targetCluster()).isNotNull();
        assertThat(model.targetCluster().bootstrapServersList()).containsExactly(
                new io.kroxylicious.proxy.service.HostPort("broker1", 9092));
    }

    @Test
    void routerResolvesMultipleRoutes() {
        var c1 = new ClusterDefinition("c1", "broker1:9092", null);
        var c2 = new ClusterDefinition("c2", "broker2:9092", null);
        var route1 = new RouteDefinition("r1", 0, null, new RouteTarget("c1", null));
        var route2 = new RouteDefinition("r2", 1, null, new RouteTarget("c2", null));
        var router = new RouterDefinition("myrouter", "Type", null, List.of(route1, route2));
        var vc = new VirtualCluster("demo", null,
                new RouteTarget(null, "myrouter"),
                List.of(simpleGateway("gw")), false, false, null, null, null, null);
        var config = new Configuration(null, List.of(c1, c2), null, null, List.of(router),
                List.of(vc), null, false, Optional.empty(), null, null);

        var model = config.virtualClusterModel(null).get(0);

        assertThat(model.routeDescriptors()).hasSize(2).containsKeys("r1", "r2");
    }

    @Test
    void routerResolvesPerRouteFilters() {
        var cluster = new ClusterDefinition("c1", "broker:9092", null);
        var filterDefs = List.of(new NamedFilterDefinition("f1", "Type1", null));
        var route = new RouteDefinition("r1", 0, List.of("f1"), new RouteTarget("c1", null));
        var router = new RouterDefinition("myrouter", "Type", null, List.of(route));
        var vc = new VirtualCluster("demo", null,
                new RouteTarget(null, "myrouter"),
                List.of(simpleGateway("gw")), false, false, null, null, null, null);
        var config = new Configuration(null, List.of(cluster), filterDefs, null, List.of(router),
                List.of(vc), null, false, Optional.empty(), null, null);

        var model = config.virtualClusterModel(null).get(0);

        RouteDescriptor rd = model.routeDescriptors().get("r1");
        assertThat(rd.filters()).hasSize(1);
        assertThat(rd.filters().get(0).name()).isEqualTo("f1");
    }

    @Test
    void virtualClusterWithoutRouterHasNullRouteDescriptors() {
        var config = config(List.of(SIMPLE_VC));

        var model = config.virtualClusterModel(null).get(0);

        assertThat(model.usesRouter()).isFalse();
        assertThat(model.routeDescriptors()).isNull();
    }

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
    void virtualClusterModelByNameWithRouter() {
        var cluster = new ClusterDefinition("c1", "broker:9092", null);
        var route = new RouteDefinition("r1", 0, null, new RouteTarget("c1", null));
        var router = new RouterDefinition("myrouter", "Type", null, List.of(route));
        var vc = new VirtualCluster("demo", null,
                new RouteTarget(null, "myrouter"),
                List.of(simpleGateway("gw")), false, false, null, null, null, null);
        var config = new Configuration(null, List.of(cluster), null, null, List.of(router),
                List.of(vc), null, false, Optional.empty(), null, null);

        var model = config.virtualClusterModel(null, "demo");

        assertThat(model.usesRouter()).isTrue();
        assertThat(model.routerName()).isEqualTo("myrouter");
        assertThat(model.routeDescriptors()).containsKey("r1");
    }

    @Test
    void virtualClusterModelByNameThrowsForUnknown() {
        var config = config(List.of(SIMPLE_VC));

        assertThatThrownBy(() -> config.virtualClusterModel(null, "nonexistent"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nonexistent");
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

    @Test
    void shouldRejectUnknownRouterReference() {
        var vc = new VirtualCluster("demo", null,
                new RouteTarget(null, "nonexistent-router"),
                List.of(simpleGateway("gw")), false, false, null, null, null, null);

        assertThatThrownBy(() -> new Configuration(null, null, null, null, null,
                List.of(vc), null, false, Optional.empty(), null, null))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining("unknown router")
                .hasMessageContaining("nonexistent-router");
    }

    @Test
    void shouldAcceptKnownRouterReference() {
        var cluster = new ClusterDefinition("c1", "broker:9092", null);
        var route = new RouteDefinition("r1", 0, null, new RouteTarget("c1", null));
        var router = new RouterDefinition("myrouter", "Type", null, List.of(route));
        var vc = new VirtualCluster("demo", null,
                new RouteTarget(null, "myrouter"),
                List.of(simpleGateway("gw")), false, false, null, null, null, null);

        assertThatCode(() -> new Configuration(null, List.of(cluster), null, null, List.of(router),
                List.of(vc), null, false, Optional.empty(), null, null))
                .doesNotThrowAnyException();
    }

    @Test
    void routerRouteTargetingNestedRouterHasNullTargetCluster() {
        var cluster = new ClusterDefinition("c1", "broker:9092", null);
        var innerRoute = new RouteDefinition("inner-r", 0, null, new RouteTarget("c1", null));
        var innerRouter = new RouterDefinition("inner", "Type", null, List.of(innerRoute));
        var outerRoute = new RouteDefinition("outer-r", 0, null, new RouteTarget(null, "inner"));
        var outerRouter = new RouterDefinition("outer", "Type", null, List.of(outerRoute));
        var vc = new VirtualCluster("demo", null,
                new RouteTarget(null, "outer"),
                List.of(simpleGateway("gw")), false, false, null, null, null, null);
        var config = new Configuration(null, List.of(cluster), null, null, List.of(outerRouter, innerRouter),
                List.of(vc), null, false, Optional.empty(), null, null);

        var model = config.virtualClusterModel(null).get(0);

        RouteDescriptor rd = model.routeDescriptors().get("outer-r");
        assertThat(rd.targetsCluster()).isFalse();
        assertThat(rd.targetCluster()).isNull();
        assertThat(rd.routerName()).isEqualTo("inner");
    }

    @Test
    void routerPrimaryTargetClusterResolvedFromFirstClusterRoute() {
        var c1 = new ClusterDefinition("c1", "broker1:9092", null);
        var c2 = new ClusterDefinition("c2", "broker2:9092", null);
        var innerRoute = new RouteDefinition("ir", 0, null, new RouteTarget("c2", null));
        var innerRouter = new RouterDefinition("inner", "Type", null, List.of(innerRoute));
        var routeToRouter = new RouteDefinition("r1", 0, null, new RouteTarget(null, "inner"));
        var routeToCluster = new RouteDefinition("r2", 1, null, new RouteTarget("c1", null));
        var router = new RouterDefinition("myrouter", "Type", null, List.of(routeToRouter, routeToCluster));
        var vc = new VirtualCluster("demo", null,
                new RouteTarget(null, "myrouter"),
                List.of(simpleGateway("gw")), false, false, null, null, null, null);

        var config = new Configuration(null, List.of(c1, c2), null, null, List.of(router, innerRouter),
                List.of(vc), null, false, Optional.empty(), null, null);
        var model = config.virtualClusterModel(null).get(0);

        assertThat(model.targetCluster()).isNotNull();
        assertThat(model.targetCluster().bootstrapServersList()).containsExactly(
                new io.kroxylicious.proxy.service.HostPort("broker1", 9092));
    }

    @Test
    void routerWithOnlyNestedRouterRoutesResolvesNullPrimaryTarget() {
        var cluster = new ClusterDefinition("c1", "broker:9092", null);
        var innerRoute = new RouteDefinition("inner-r", 0, null, new RouteTarget("c1", null));
        var innerRouter = new RouterDefinition("inner", "Type", null, List.of(innerRoute));
        var outerRoute = new RouteDefinition("outer-r", 0, null, new RouteTarget(null, "inner"));
        var outerRouter = new RouterDefinition("outer", "Type", null, List.of(outerRoute));
        var vc = new VirtualCluster("demo", null,
                new RouteTarget(null, "outer"),
                List.of(simpleGateway("gw")), false, false, null, null, null, null);

        var config = new Configuration(null, List.of(cluster), null, null, List.of(outerRouter, innerRouter),
                List.of(vc), null, false, Optional.empty(), null, null);
        var model = config.virtualClusterModel(null).get(0);

        assertThat(model.targetCluster()).isNull();
    }
}
