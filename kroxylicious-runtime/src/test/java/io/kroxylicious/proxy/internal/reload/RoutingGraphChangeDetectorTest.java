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

class RoutingGraphChangeDetectorTest {

    private static final ClusterDefinition UPSTREAM = new ClusterDefinition("upstream", "kafka:9092", null);

    private final RoutingGraphChangeDetector detector = new RoutingGraphChangeDetector();

    // ---- router-change tests ----

    @Test
    void emptyWhenNoRouterDefinitionsChanged() {
        // Given
        var rd = routerDef("r1", "v1", "upstream");
        var oldConfig = config(List.of(UPSTREAM), List.of(rd), vcWithRouter("cluster-a", "r1"));
        var newConfig = config(List.of(UPSTREAM), List.of(rd), vcWithRouter("cluster-a", "r1"));

        // When / Then
        assertThat(detector.detect(ctx(oldConfig, newConfig)).isEmpty()).isTrue();
    }

    @Test
    void emptyWhenNoRouterOrClusterDefinitionsExistInEitherConfig() {
        // Given
        var oldConfig = configWithNoRoutersOrClusterDefs(vcWithInlineTarget("cluster-a"));
        var newConfig = configWithNoRoutersOrClusterDefs(vcWithInlineTarget("cluster-a"));

        // When / Then
        assertThat(detector.detect(ctx(oldConfig, newConfig)).isEmpty()).isTrue();
    }

    @Test
    void detectsClusterReferencingModifiedRouterDefinition() {
        // Given
        var oldRd = routerDef("r1", "v1", "upstream");
        var newRd = routerDef("r1", "v2", "upstream");
        var oldConfig = config(List.of(UPSTREAM), List.of(oldRd), vcWithRouter("cluster-a", "r1"));
        var newConfig = config(List.of(UPSTREAM), List.of(newRd), vcWithRouter("cluster-a", "r1"));

        // When
        var result = detector.detect(ctx(oldConfig, newConfig));

        // Then
        assertThat(result.clustersToModify()).containsExactly("cluster-a");
        assertThat(result.clustersToAdd()).isEmpty();
        assertThat(result.clustersToRemove()).isEmpty();
    }

    @Test
    void ignoresClustersNotReferencingChangedRouter() {
        // Given
        var oldR1 = routerDef("r1", "v1", "upstream");
        var newR1 = routerDef("r1", "v2", "upstream");
        var r2 = routerDef("r2", "stable", "upstream");
        var oldConfig = config(List.of(UPSTREAM), List.of(oldR1, r2),
                vcWithRouter("cluster-a", "r1"),
                vcWithRouter("cluster-b", "r2"));
        var newConfig = config(List.of(UPSTREAM), List.of(newR1, r2),
                vcWithRouter("cluster-a", "r1"),
                vcWithRouter("cluster-b", "r2"));

        // When
        var result = detector.detect(ctx(oldConfig, newConfig));

        // Then
        assertThat(result.clustersToModify()).containsExactly("cluster-a");
    }

    @Test
    void ignoresClustersWithNoRouter() {
        // Given
        var oldRd = routerDef("r1", "v1", "upstream");
        var newRd = routerDef("r1", "v2", "upstream");
        var oldConfig = config(List.of(UPSTREAM), List.of(oldRd),
                vcWithRouter("routed", "r1"),
                vcWithInlineTarget("direct"));
        var newConfig = config(List.of(UPSTREAM), List.of(newRd),
                vcWithRouter("routed", "r1"),
                vcWithInlineTarget("direct"));

        // When
        var result = detector.detect(ctx(oldConfig, newConfig));

        // Then
        assertThat(result.clustersToModify()).containsExactly("routed");
    }

    @Test
    void removedClustersAreNotIncludedForRouterChanges() {
        // Given
        var oldRd = routerDef("r1", "v1", "upstream");
        var newRd = routerDef("r1", "v2", "upstream");
        var oldConfig = config(List.of(UPSTREAM), List.of(oldRd),
                vcWithRouter("stays", "r1"),
                vcWithRouter("goes-away", "r1"));
        var newConfig = config(List.of(UPSTREAM), List.of(newRd),
                vcWithRouter("stays", "r1"));

        // When
        var result = detector.detect(ctx(oldConfig, newConfig));

        // Then
        assertThat(result.clustersToModify()).containsExactly("stays");
        assertThat(result.clustersToRemove()).isEmpty();
    }

    @Test
    void reorderingRouterDefinitionsAtTopLevelDoesNotTriggerChange() {
        // Given
        var r1 = routerDef("r1", "cfg", "upstream");
        var r2 = routerDef("r2", "cfg", "upstream");
        var oldConfig = config(List.of(UPSTREAM), List.of(r1, r2),
                vcWithRouter("a", "r1"),
                vcWithRouter("b", "r2"));
        var newConfig = config(List.of(UPSTREAM), List.of(r2, r1),
                vcWithRouter("a", "r1"),
                vcWithRouter("b", "r2"));

        // When / Then
        assertThat(detector.detect(ctx(oldConfig, newConfig)).isEmpty()).isTrue();
    }

    @Test
    void addedRouterDefinitionNotYetReferencedByAnyClusterProducesNoModify() {
        // Given
        var r1 = routerDef("r1", "cfg", "upstream");
        var r2 = routerDef("r2", "cfg", "upstream");
        var oldConfig = config(List.of(UPSTREAM), List.of(r1), vcWithRouter("cluster-a", "r1"));
        var newConfig = config(List.of(UPSTREAM), List.of(r1, r2), vcWithRouter("cluster-a", "r1"));

        // When / Then
        assertThat(detector.detect(ctx(oldConfig, newConfig)).isEmpty()).isTrue();
    }

    // ---- cluster-definition-change tests ----

    @Test
    void emptyWhenNoClusterDefinitionsChanged() {
        // Given
        var cd = clusterDef("upstream-a", "kafka:9092");
        var oldConfig = configWithClusterDefs(List.of(cd), vcWithNamedCluster("vc-a", "upstream-a"));
        var newConfig = configWithClusterDefs(List.of(cd), vcWithNamedCluster("vc-a", "upstream-a"));

        // When / Then
        assertThat(detector.detect(ctx(oldConfig, newConfig)).isEmpty()).isTrue();
    }

    @Test
    void emptyWhenNoClusterDefinitionsExistInEitherConfig() {
        // Given
        var oldConfig = configWithNoRoutersOrClusterDefs(vcWithInlineTarget("vc-a"));
        var newConfig = configWithNoRoutersOrClusterDefs(vcWithInlineTarget("vc-a"));

        // When / Then
        assertThat(detector.detect(ctx(oldConfig, newConfig)).isEmpty()).isTrue();
    }

    @Test
    void detectsVcReferencingModifiedClusterDefinition() {
        // Given
        var oldCd = clusterDef("upstream-a", "kafka:9092");
        var newCd = clusterDef("upstream-a", "kafka-new:9092");
        var oldConfig = configWithClusterDefs(List.of(oldCd), vcWithNamedCluster("vc-a", "upstream-a"));
        var newConfig = configWithClusterDefs(List.of(newCd), vcWithNamedCluster("vc-a", "upstream-a"));

        // When
        var result = detector.detect(ctx(oldConfig, newConfig));

        // Then
        assertThat(result.clustersToModify()).containsExactly("vc-a");
        assertThat(result.clustersToAdd()).isEmpty();
        assertThat(result.clustersToRemove()).isEmpty();
    }

    @Test
    void ignoresVcNotReferencingChangedClusterDefinition() {
        // Given
        var oldA = clusterDef("upstream-a", "kafka-a-old:9092");
        var newA = clusterDef("upstream-a", "kafka-a-new:9092");
        var b = clusterDef("upstream-b", "kafka-b:9092");
        var oldConfig = configWithClusterDefs(List.of(oldA, b),
                vcWithNamedCluster("vc-a", "upstream-a"),
                vcWithNamedCluster("vc-b", "upstream-b"));
        var newConfig = configWithClusterDefs(List.of(newA, b),
                vcWithNamedCluster("vc-a", "upstream-a"),
                vcWithNamedCluster("vc-b", "upstream-b"));

        // When
        var result = detector.detect(ctx(oldConfig, newConfig));

        // Then
        assertThat(result.clustersToModify()).containsExactly("vc-a");
    }

    @Test
    void detectsVcReferencingChangedClusterDefinitionViaRouter() {
        // Given
        var oldCd = clusterDef("upstream-a", "kafka:9092");
        var newCd = clusterDef("upstream-a", "kafka-new:9092");
        var router = routerDef("my-router", "cfg", "upstream-a");
        var oldConfig = config(List.of(oldCd), List.of(router),
                vcWithNamedCluster("direct", "upstream-a"),
                vcWithRouter("routed", "my-router"));
        var newConfig = config(List.of(newCd), List.of(router),
                vcWithNamedCluster("direct", "upstream-a"),
                vcWithRouter("routed", "my-router"));

        // When
        var result = detector.detect(ctx(oldConfig, newConfig));

        // Then
        assertThat(result.clustersToModify()).containsExactlyInAnyOrder("direct", "routed");
    }

    @Test
    void doesNotRestartVcWhenRouterTargetsUnchangedClusterDefinition() {
        // Given
        var cd = clusterDef("upstream-a", "kafka:9092");
        var router = routerDef("my-router", "cfg", "upstream-a");
        var oldConfig = config(List.of(cd), List.of(router), vcWithRouter("routed", "my-router"));
        var newConfig = config(List.of(cd), List.of(router), vcWithRouter("routed", "my-router"));

        // When / Then
        assertThat(detector.detect(ctx(oldConfig, newConfig)).isEmpty()).isTrue();
    }

    @Test
    void doesNotRestartRouterVcWhenUnrelatedClusterDefinitionChanges() {
        // Given
        var oldA = clusterDef("cluster-a", "kafka-a-old:9092");
        var newA = clusterDef("cluster-a", "kafka-a-new:9092");
        var b = clusterDef("cluster-b", "kafka-b:9092");
        var routerA = routerDef("router-a", "cfg", "cluster-a");
        var routerB = routerDef("router-b", "cfg", "cluster-b");
        var oldConfig = config(List.of(oldA, b), List.of(routerA, routerB),
                vcWithRouter("vc-a", "router-a"),
                vcWithRouter("vc-b", "router-b"));
        var newConfig = config(List.of(newA, b), List.of(routerA, routerB),
                vcWithRouter("vc-a", "router-a"),
                vcWithRouter("vc-b", "router-b"));

        // When
        var result = detector.detect(ctx(oldConfig, newConfig));

        // Then
        assertThat(result.clustersToModify()).containsExactly("vc-a");
    }

    @Test
    void ignoresVcUsingDeprecatedInlineTargetCluster() {
        // Given
        var oldCd = clusterDef("upstream-a", "kafka:9092");
        var newCd = clusterDef("upstream-a", "kafka-new:9092");
        var oldConfig = configWithClusterDefs(List.of(oldCd),
                vcWithNamedCluster("named", "upstream-a"),
                vcWithInlineTarget("inline"));
        var newConfig = configWithClusterDefs(List.of(newCd),
                vcWithNamedCluster("named", "upstream-a"),
                vcWithInlineTarget("inline"));

        // When
        var result = detector.detect(ctx(oldConfig, newConfig));

        // Then
        assertThat(result.clustersToModify()).containsExactly("named");
    }

    @Test
    void removedVcIsNotIncludedForClusterDefinitionChanges() {
        // Given
        var oldCd = clusterDef("upstream-a", "kafka:9092");
        var newCd = clusterDef("upstream-a", "kafka-new:9092");
        var oldConfig = configWithClusterDefs(List.of(oldCd),
                vcWithNamedCluster("stays", "upstream-a"),
                vcWithNamedCluster("goes-away", "upstream-a"));
        var newConfig = configWithClusterDefs(List.of(newCd),
                vcWithNamedCluster("stays", "upstream-a"));

        // When
        var result = detector.detect(ctx(oldConfig, newConfig));

        // Then
        assertThat(result.clustersToModify()).containsExactly("stays");
        assertThat(result.clustersToRemove()).isEmpty();
    }

    @Test
    void reorderingClusterDefinitionsDoesNotTriggerChange() {
        // Given
        var a = clusterDef("a", "kafka-a:9092");
        var b = clusterDef("b", "kafka-b:9092");
        var oldConfig = configWithClusterDefs(List.of(a, b),
                vcWithNamedCluster("vc-a", "a"),
                vcWithNamedCluster("vc-b", "b"));
        var newConfig = configWithClusterDefs(List.of(b, a),
                vcWithNamedCluster("vc-a", "a"),
                vcWithNamedCluster("vc-b", "b"));

        // When / Then
        assertThat(detector.detect(ctx(oldConfig, newConfig)).isEmpty()).isTrue();
    }

    @Test
    void addedClusterDefinitionNotReferencedByAnyVcProducesNoModify() {
        // Given
        var existing = clusterDef("existing", "kafka:9092");
        var added = clusterDef("new-cluster", "kafka-new:9092");
        var oldConfig = configWithClusterDefs(List.of(existing), vcWithNamedCluster("vc-a", "existing"));
        var newConfig = configWithClusterDefs(List.of(existing, added), vcWithNamedCluster("vc-a", "existing"));

        // When / Then
        assertThat(detector.detect(ctx(oldConfig, newConfig)).isEmpty()).isTrue();
    }

    // ---- combined router + cluster-definition tests ----

    @Test
    void detectsBothRouterAndClusterDefinitionChangesInSingleWalk() {
        // Given: both the router and the cluster definition it targets have changed
        var oldCd = clusterDef("upstream-a", "kafka:9092");
        var newCd = clusterDef("upstream-a", "kafka-new:9092");
        var oldRd = routerDef("r1", "v1", "upstream-a");
        var newRd = routerDef("r1", "v2", "upstream-a");
        var oldConfig = config(List.of(oldCd), List.of(oldRd), vcWithRouter("vc", "r1"));
        var newConfig = config(List.of(newCd), List.of(newRd), vcWithRouter("vc", "r1"));

        // When
        var result = detector.detect(ctx(oldConfig, newConfig));

        // Then
        assertThat(result.clustersToModify()).containsExactly("vc");
    }

    @Test
    void detectsClusterDefinitionChangeEvenWhenRouterIsUnchanged() {
        // Given: the router is unchanged but the cluster definition it targets has changed;
        // the single walk must still reach the leaf and detect the cluster-definition change
        var oldCd = clusterDef("upstream-a", "kafka:9092");
        var newCd = clusterDef("upstream-a", "kafka-new:9092");
        var router = routerDef("r1", "stable", "upstream-a");
        var oldConfig = config(List.of(oldCd), List.of(router), vcWithRouter("vc", "r1"));
        var newConfig = config(List.of(newCd), List.of(router), vcWithRouter("vc", "r1"));

        // When
        var result = detector.detect(ctx(oldConfig, newConfig));

        // Then
        assertThat(result.clustersToModify()).containsExactly("vc");
    }

    @Test
    void detectsRouterChangeEvenWhenClusterDefinitionIsUnchanged() {
        // Given: cluster definition is unchanged but the router referencing it has changed
        var cd = clusterDef("upstream-a", "kafka:9092");
        var oldRd = routerDef("r1", "v1", "upstream-a");
        var newRd = routerDef("r1", "v2", "upstream-a");
        var oldConfig = config(List.of(cd), List.of(oldRd), vcWithRouter("vc", "r1"));
        var newConfig = config(List.of(cd), List.of(newRd), vcWithRouter("vc", "r1"));

        // When
        var result = detector.detect(ctx(oldConfig, newConfig));

        // Then
        assertThat(result.clustersToModify()).containsExactly("vc");
    }

    @Test
    void emptyWhenNeitherRouterNorClusterDefinitionChanged() {
        // Given
        var cd = clusterDef("upstream-a", "kafka:9092");
        var rd = routerDef("r1", "stable", "upstream-a");
        var oldConfig = config(List.of(cd), List.of(rd), vcWithRouter("vc", "r1"));
        var newConfig = config(List.of(cd), List.of(rd), vcWithRouter("vc", "r1"));

        // When / Then
        assertThat(detector.detect(ctx(oldConfig, newConfig)).isEmpty()).isTrue();
    }

    @Test
    void mixedVcsWithDifferentConcernsDetectedInOnePass() {
        // Given: vc-a is affected by a router change, vc-b by a cluster-definition change
        var cd1 = clusterDef("cluster-1", "kafka-1:9092");
        var oldCd2 = clusterDef("cluster-2", "kafka-2-old:9092");
        var newCd2 = clusterDef("cluster-2", "kafka-2-new:9092");
        var oldR1 = routerDef("r1", "v1", "cluster-1");
        var newR1 = routerDef("r1", "v2", "cluster-1");
        var r2 = routerDef("r2", "stable", "cluster-2");

        var oldConfig = config(List.of(cd1, oldCd2), List.of(oldR1, r2),
                vcWithRouter("vc-a", "r1"),
                vcWithRouter("vc-b", "r2"));
        var newConfig = config(List.of(cd1, newCd2), List.of(newR1, r2),
                vcWithRouter("vc-a", "r1"),
                vcWithRouter("vc-b", "r2"));

        // When
        var result = detector.detect(ctx(oldConfig, newConfig));

        // Then
        assertThat(result.clustersToModify()).containsExactlyInAnyOrder("vc-a", "vc-b");
    }

    // ---- fixture helpers ----

    private static ConfigurationChangeContext ctx(Configuration oldConfig, Configuration newConfig) {
        return new ConfigurationChangeContext(oldConfig, newConfig);
    }

    private static ClusterDefinition clusterDef(String name, String bootstrapServers) {
        return new ClusterDefinition(name, bootstrapServers, null);
    }

    private static RouterDefinition routerDef(String name, String opaqueConfig, String clusterTarget) {
        var route = new RouteDefinition("route", 0, null, new RouteTarget(clusterTarget, null));
        return new RouterDefinition(name, "SomeRouterType", opaqueConfig, List.of(route));
    }

    private static VirtualCluster vcWithRouter(String name, String routerName) {
        return vc(name, new RouteTarget(null, routerName));
    }

    private static VirtualCluster vcWithNamedCluster(String name, String clusterDefName) {
        return vc(name, new RouteTarget(clusterDefName, null));
    }

    @SuppressWarnings("deprecation")
    private static VirtualCluster vcWithInlineTarget(String name) {
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

    private static Configuration config(@Nullable List<ClusterDefinition> clusterDefs,
                                        @Nullable List<RouterDefinition> routerDefs,
                                        VirtualCluster... clusters) {
        return new Configuration(null, clusterDefs, null, null, routerDefs, List.of(clusters), null, false,
                Optional.empty(), null, null);
    }

    private static Configuration configWithClusterDefs(List<ClusterDefinition> clusterDefs, VirtualCluster... clusters) {
        return new Configuration(null, clusterDefs, null, null, null, List.of(clusters), null, false,
                Optional.empty(), null, null);
    }

    private static Configuration configWithNoRoutersOrClusterDefs(VirtualCluster... clusters) {
        return new Configuration(null, null, null, null, null, List.of(clusters), null, false,
                Optional.empty(), null, null);
    }
}
