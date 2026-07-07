/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.reload;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.ClusterDefinition;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.PortIdentifiesNodeIdentificationStrategy;
import io.kroxylicious.proxy.config.RouteDefinition;
import io.kroxylicious.proxy.config.RouteTarget;
import io.kroxylicious.proxy.config.RouterDefinition;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.config.VirtualClusterGateway;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;

class RouterChangeDetectorTest {

    private static final ClusterDefinition UPSTREAM = new ClusterDefinition("upstream", "kafka:9092", null);

    private final RouterChangeDetector detector = new RouterChangeDetector();

    @Test
    void emptyWhenNoRouterDefinitionsChanged() {
        // Given
        var rd = routerDef("r1", "v1", "upstream");
        var oldConfig = configWith(List.of(rd), vcWithRouter("cluster-a", "r1"));
        var newConfig = configWith(List.of(rd), vcWithRouter("cluster-a", "r1"));

        // When / Then
        assertThat(detector.detect(new ConfigurationChangeContext(oldConfig, newConfig)).isEmpty()).isTrue();
    }

    @Test
    void emptyWhenNoRouterDefinitionsExistInEitherConfig() {
        // Given: VCs using targetCluster, no router definitions
        var oldConfig = configWithNoRouters(vcWithTarget("cluster-a"));
        var newConfig = configWithNoRouters(vcWithTarget("cluster-a"));

        // When / Then
        assertThat(detector.detect(new ConfigurationChangeContext(oldConfig, newConfig)).isEmpty()).isTrue();
    }

    @Test
    void detectsClusterReferencingModifiedRouterDefinition() {
        // Given
        var oldRd = routerDef("r1", "v1", "upstream");
        var newRd = routerDef("r1", "v2", "upstream");
        var oldConfig = configWith(List.of(oldRd), vcWithRouter("cluster-a", "r1"));
        var newConfig = configWith(List.of(newRd), vcWithRouter("cluster-a", "r1"));

        // When
        var result = detector.detect(new ConfigurationChangeContext(oldConfig, newConfig));

        // Then
        assertThat(result.clustersToModify()).containsExactly("cluster-a");
        assertThat(result.clustersToAdd()).isEmpty();
        assertThat(result.clustersToRemove()).isEmpty();
    }

    @Test
    void ignoresClustersNotReferencingChangedRouter() {
        // Given: r1 changed, r2 unchanged; cluster-a uses r1, cluster-b uses r2
        var oldR1 = routerDef("r1", "v1", "upstream");
        var newR1 = routerDef("r1", "v2", "upstream");
        var r2 = routerDef("r2", "stable", "upstream");
        var oldConfig = configWith(List.of(oldR1, r2),
                vcWithRouter("cluster-a", "r1"),
                vcWithRouter("cluster-b", "r2"));
        var newConfig = configWith(List.of(newR1, r2),
                vcWithRouter("cluster-a", "r1"),
                vcWithRouter("cluster-b", "r2"));

        // When
        var result = detector.detect(new ConfigurationChangeContext(oldConfig, newConfig));

        // Then: only the cluster referencing the changed router is flagged
        assertThat(result.clustersToModify()).containsExactly("cluster-a");
    }

    @Test
    void ignoresClustersWithNoRouter() {
        // Given: a router definition changes but some clusters use a direct targetCluster
        var oldRd = routerDef("r1", "v1", "upstream");
        var newRd = routerDef("r1", "v2", "upstream");
        var oldConfig = configWith(List.of(oldRd),
                vcWithRouter("routed", "r1"),
                vcWithTarget("direct"));
        var newConfig = configWith(List.of(newRd),
                vcWithRouter("routed", "r1"),
                vcWithTarget("direct"));

        // When
        var result = detector.detect(new ConfigurationChangeContext(oldConfig, newConfig));

        // Then: only the routed cluster is affected
        assertThat(result.clustersToModify()).containsExactly("routed");
    }

    @Test
    void removedClustersAreNotIncluded() {
        // Given
        var oldRd = routerDef("r1", "v1", "upstream");
        var newRd = routerDef("r1", "v2", "upstream");
        var oldConfig = configWith(List.of(oldRd),
                vcWithRouter("stays", "r1"),
                vcWithRouter("goes-away", "r1"));
        var newConfig = configWith(List.of(newRd),
                vcWithRouter("stays", "r1"));

        // When
        var result = detector.detect(new ConfigurationChangeContext(oldConfig, newConfig));

        // Then: removed cluster is VirtualClusterChangeDetector's concern, not ours
        assertThat(result.clustersToModify()).containsExactly("stays");
        assertThat(result.clustersToRemove()).isEmpty();
    }

    @Test
    void reorderingRouterDefinitionsAtTopLevelDoesNotTriggerChange() {
        // Given: routerDefinitions reordered but content identical; a RouterDefinition is a
        // record so equals() is structural — reorder is a no-op semantically.
        var r1 = routerDef("r1", "cfg", "upstream");
        var r2 = routerDef("r2", "cfg", "upstream");
        var oldConfig = configWith(List.of(r1, r2),
                vcWithRouter("a", "r1"),
                vcWithRouter("b", "r2"));
        var newConfig = configWith(List.of(r2, r1),
                vcWithRouter("a", "r1"),
                vcWithRouter("b", "r2"));

        // When / Then
        assertThat(detector.detect(new ConfigurationChangeContext(oldConfig, newConfig)).isEmpty()).isTrue();
    }

    @Test
    void addedRouterDefinitionNotYetReferencedByAnyClusterProducesNoModify() {
        // Given: a new router definition appears but no VC references it
        var r1 = routerDef("r1", "cfg", "upstream");
        var r2 = routerDef("r2", "cfg", "upstream");
        var oldConfig = configWith(List.of(r1), vcWithRouter("cluster-a", "r1"));
        var newConfig = configWith(List.of(r1, r2), vcWithRouter("cluster-a", "r1"));

        // When / Then: r2 is new (changed) but no cluster references it
        assertThat(detector.detect(new ConfigurationChangeContext(oldConfig, newConfig)).isEmpty()).isTrue();
    }

    // ---- fixture helpers ----

    private static RouterDefinition routerDef(String name, String opaqueConfig, String clusterTarget) {
        var route = new RouteDefinition("route", 0, null, new RouteTarget(clusterTarget, null));
        return new RouterDefinition(name, "SomeRouterType", opaqueConfig, List.of(route));
    }

    private static VirtualCluster vcWithRouter(String name, String routerName) {
        return vc(name, new RouteTarget(null, routerName));
    }

    @SuppressWarnings("deprecation")
    private static VirtualCluster vcWithTarget(String name) {
        return new VirtualCluster(name, new TargetCluster("kafka:9092", Optional.empty()),
                List.of(gateway()), false, false, null);
    }

    private static VirtualCluster vc(String name, RouteTarget target) {
        return new VirtualCluster(name, null, target, List.of(gateway()), false, false, null, null, null, null);
    }

    private static VirtualClusterGateway gateway() {
        return new VirtualClusterGateway("default",
                new PortIdentifiesNodeIdentificationStrategy(new HostPort("localhost", 9192), null, null, null),
                null, Optional.empty());
    }

    private static Configuration configWith(@Nullable List<RouterDefinition> routerDefs, VirtualCluster... clusters) {
        return new Configuration(null, List.of(UPSTREAM), null, null, routerDefs, List.of(clusters), null, false,
                Optional.empty(), null, null);
    }

    private static Configuration configWithNoRouters(VirtualCluster... clusters) {
        return new Configuration(null, null, null, null, null, List.of(clusters), null, false,
                Optional.empty(), null, null);
    }

}
