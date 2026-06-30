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

import static org.assertj.core.api.Assertions.assertThat;

class ClusterDefinitionChangeDetectorTest {

    private final ClusterDefinitionChangeDetector detector = new ClusterDefinitionChangeDetector();

    @Test
    void emptyWhenNoClusterDefinitionsChanged() {
        // Given
        var cd = clusterDef("upstream", "kafka:9092");
        var oldConfig = config(List.of(cd), vcWithNamedCluster("vc-a", "upstream"));
        var newConfig = config(List.of(cd), vcWithNamedCluster("vc-a", "upstream"));

        // When / Then
        assertThat(detector.detect(new ConfigurationChangeContext(oldConfig, newConfig)).isEmpty()).isTrue();
    }

    @Test
    void emptyWhenNoClusterDefinitionsExistInEitherConfig() {
        // Given: VCs using deprecated inline targetCluster, no cluster definitions
        var oldConfig = configWithNoClusterDefs(vcWithInlineTarget("vc-a"));
        var newConfig = configWithNoClusterDefs(vcWithInlineTarget("vc-a"));

        // When / Then
        assertThat(detector.detect(new ConfigurationChangeContext(oldConfig, newConfig)).isEmpty()).isTrue();
    }

    @Test
    void detectsVcReferencingModifiedClusterDefinition() {
        // Given
        var oldCd = clusterDef("upstream", "kafka:9092");
        var newCd = clusterDef("upstream", "kafka-new:9092");
        var oldConfig = config(List.of(oldCd), vcWithNamedCluster("vc-a", "upstream"));
        var newConfig = config(List.of(newCd), vcWithNamedCluster("vc-a", "upstream"));

        // When
        var result = detector.detect(new ConfigurationChangeContext(oldConfig, newConfig));

        // Then
        assertThat(result.clustersToModify()).containsExactly("vc-a");
        assertThat(result.clustersToAdd()).isEmpty();
        assertThat(result.clustersToRemove()).isEmpty();
    }

    @Test
    void ignoresVcNotReferencingChangedClusterDefinition() {
        // Given: upstream-a changed, upstream-b unchanged; vc-a uses upstream-a, vc-b uses upstream-b
        var oldA = clusterDef("upstream-a", "kafka-a-old:9092");
        var newA = clusterDef("upstream-a", "kafka-a-new:9092");
        var b = clusterDef("upstream-b", "kafka-b:9092");
        var oldConfig = config(List.of(oldA, b),
                vcWithNamedCluster("vc-a", "upstream-a"),
                vcWithNamedCluster("vc-b", "upstream-b"));
        var newConfig = config(List.of(newA, b),
                vcWithNamedCluster("vc-a", "upstream-a"),
                vcWithNamedCluster("vc-b", "upstream-b"));

        // When
        var result = detector.detect(new ConfigurationChangeContext(oldConfig, newConfig));

        // Then: only the VC referencing the changed cluster definition is flagged
        assertThat(result.clustersToModify()).containsExactly("vc-a");
    }

    @Test
    void ignoresVcUsingRouter() {
        // Given: a cluster definition changes but some VCs target a router, not a named cluster
        var oldCd = clusterDef("upstream", "kafka:9092");
        var newCd = clusterDef("upstream", "kafka-new:9092");
        var router = routerDef("my-router", "upstream");
        var oldConfig = configWithRouter(List.of(oldCd), List.of(router),
                vcWithNamedCluster("direct", "upstream"),
                vcWithRouter("routed", "my-router"));
        var newConfig = configWithRouter(List.of(newCd), List.of(router),
                vcWithNamedCluster("direct", "upstream"),
                vcWithRouter("routed", "my-router"));

        // When
        var result = detector.detect(new ConfigurationChangeContext(oldConfig, newConfig));

        // Then: only the directly-referencing VC is affected
        assertThat(result.clustersToModify()).containsExactly("direct");
    }

    @Test
    void ignoresVcUsingDeprecatedInlineTargetCluster() {
        // Given: cluster definition changes; one VC uses inline targetCluster (namedTargetCluster() is null)
        var oldCd = clusterDef("upstream", "kafka:9092");
        var newCd = clusterDef("upstream", "kafka-new:9092");
        var oldConfig = config(List.of(oldCd),
                vcWithNamedCluster("named", "upstream"),
                vcWithInlineTarget("inline"));
        var newConfig = config(List.of(newCd),
                vcWithNamedCluster("named", "upstream"),
                vcWithInlineTarget("inline"));

        // When
        var result = detector.detect(new ConfigurationChangeContext(oldConfig, newConfig));

        // Then: inline VC is not affected
        assertThat(result.clustersToModify()).containsExactly("named");
    }

    @Test
    void removedVcIsNotIncluded() {
        // Given: cluster definition changes; one VC present in old but not new
        var oldCd = clusterDef("upstream", "kafka:9092");
        var newCd = clusterDef("upstream", "kafka-new:9092");
        var oldConfig = config(List.of(oldCd),
                vcWithNamedCluster("stays", "upstream"),
                vcWithNamedCluster("goes-away", "upstream"));
        var newConfig = config(List.of(newCd),
                vcWithNamedCluster("stays", "upstream"));

        // When
        var result = detector.detect(new ConfigurationChangeContext(oldConfig, newConfig));

        // Then: removed cluster is VirtualClusterChangeDetector's concern, not ours
        assertThat(result.clustersToModify()).containsExactly("stays");
        assertThat(result.clustersToRemove()).isEmpty();
    }

    @Test
    void reorderingClusterDefinitionsDoesNotTriggerChange() {
        // Given: clusterDefinitions reordered but content identical
        var a = clusterDef("a", "kafka-a:9092");
        var b = clusterDef("b", "kafka-b:9092");
        var oldConfig = config(List.of(a, b),
                vcWithNamedCluster("vc-a", "a"),
                vcWithNamedCluster("vc-b", "b"));
        var newConfig = config(List.of(b, a),
                vcWithNamedCluster("vc-a", "a"),
                vcWithNamedCluster("vc-b", "b"));

        // When / Then
        assertThat(detector.detect(new ConfigurationChangeContext(oldConfig, newConfig)).isEmpty()).isTrue();
    }

    @Test
    void addedClusterDefinitionNotReferencedByAnyVcProducesNoModify() {
        // Given: a new cluster definition appears but no VC references it
        var existing = clusterDef("existing", "kafka:9092");
        var added = clusterDef("new-cluster", "kafka-new:9092");
        var oldConfig = config(List.of(existing), vcWithNamedCluster("vc-a", "existing"));
        var newConfig = config(List.of(existing, added), vcWithNamedCluster("vc-a", "existing"));

        // When / Then: new-cluster is added (changed) but no VC references it
        assertThat(detector.detect(new ConfigurationChangeContext(oldConfig, newConfig)).isEmpty()).isTrue();
    }

    // ---- fixture helpers ----

    private static ClusterDefinition clusterDef(String name, String bootstrapServers) {
        return new ClusterDefinition(name, bootstrapServers, null);
    }

    private static VirtualCluster vcWithNamedCluster(String name, String clusterDefName) {
        return vc(name, new RouteTarget(clusterDefName, null));
    }

    private static VirtualCluster vcWithRouter(String name, String routerName) {
        return vc(name, new RouteTarget(null, routerName));
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

    private static RouterDefinition routerDef(String name, String clusterTarget) {
        var route = new RouteDefinition("route", 0, null, new RouteTarget(clusterTarget, null));
        return new RouterDefinition(name, "SomeRouterType", "cfg", List.of(route));
    }

    private static Configuration config(List<ClusterDefinition> clusterDefs, VirtualCluster... clusters) {
        return new Configuration(null, clusterDefs, null, null, null, List.of(clusters), null, false,
                Optional.empty(), null, null);
    }

    private static Configuration configWithRouter(List<ClusterDefinition> clusterDefs,
                                                  List<RouterDefinition> routerDefs,
                                                  VirtualCluster... clusters) {
        return new Configuration(null, clusterDefs, null, null, routerDefs, List.of(clusters), null, false,
                Optional.empty(), null, null);
    }

    private static Configuration configWithNoClusterDefs(VirtualCluster... clusters) {
        return new Configuration(null, null, null, null, null, List.of(clusters), null, false,
                Optional.empty(), null, null);
    }
}
