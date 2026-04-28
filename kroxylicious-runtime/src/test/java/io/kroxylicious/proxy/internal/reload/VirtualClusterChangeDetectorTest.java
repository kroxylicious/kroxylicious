/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.reload;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.PortIdentifiesNodeIdentificationStrategy;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.config.VirtualClusterGateway;
import io.kroxylicious.proxy.service.HostPort;

import static org.assertj.core.api.Assertions.assertThat;

class VirtualClusterChangeDetectorTest {

    private final VirtualClusterChangeDetector detector = new VirtualClusterChangeDetector();

    @Test
    void identicalConfigsProduceEmptyResult() {
        var config = configWith(vc("cluster-a", "kafka-a:9092", 9192));
        var result = detector.detect(new ConfigurationChangeContext(config, config));
        assertThat(result.isEmpty()).isTrue();
    }

    @Test
    void detectsAddedCluster() {
        var oldConfig = configWith(vc("cluster-a", "kafka-a:9092", 9192));
        var newConfig = configWith(
                vc("cluster-a", "kafka-a:9092", 9192),
                vc("cluster-b", "kafka-b:9092", 9193));
        var result = detector.detect(new ConfigurationChangeContext(oldConfig, newConfig));
        assertThat(result.clustersToAdd()).containsExactly("cluster-b");
        assertThat(result.clustersToRemove()).isEmpty();
        assertThat(result.clustersToModify()).isEmpty();
    }

    @Test
    void detectsRemovedCluster() {
        var oldConfig = configWith(
                vc("cluster-a", "kafka-a:9092", 9192),
                vc("cluster-b", "kafka-b:9092", 9193));
        var newConfig = configWith(vc("cluster-a", "kafka-a:9092", 9192));
        var result = detector.detect(new ConfigurationChangeContext(oldConfig, newConfig));
        assertThat(result.clustersToAdd()).isEmpty();
        assertThat(result.clustersToRemove()).containsExactly("cluster-b");
        assertThat(result.clustersToModify()).isEmpty();
    }

    @Test
    void detectsModifiedClusterWhenTargetBootstrapChanges() {
        var oldConfig = configWith(vc("cluster-a", "kafka-old:9092", 9192));
        var newConfig = configWith(vc("cluster-a", "kafka-new:9092", 9192));
        var result = detector.detect(new ConfigurationChangeContext(oldConfig, newConfig));
        assertThat(result.clustersToAdd()).isEmpty();
        assertThat(result.clustersToRemove()).isEmpty();
        assertThat(result.clustersToModify()).containsExactly("cluster-a");
    }

    @Test
    void detectsModifiedClusterWhenGatewayPortChanges() {
        var oldConfig = configWith(vc("cluster-a", "kafka-a:9092", 9192));
        var newConfig = configWith(vc("cluster-a", "kafka-a:9092", 9999));
        var result = detector.detect(new ConfigurationChangeContext(oldConfig, newConfig));
        assertThat(result.clustersToModify()).containsExactly("cluster-a");
    }

    @Test
    void reorderingGatewaysIsNotTreatedAsModification() {
        var gatewayA = gateway("gw-a", 9192);
        var gatewayB = gateway("gw-b", 9193);
        var oldConfig = configWith(vcWithGateways("cluster", List.of(gatewayA, gatewayB)));
        var newConfig = configWith(vcWithGateways("cluster", List.of(gatewayB, gatewayA)));
        var result = detector.detect(new ConfigurationChangeContext(oldConfig, newConfig));
        assertThat(result.isEmpty()).isTrue();
    }

    @Test
    void reorderingVirtualClustersListDoesNotTriggerChange() {
        // Top-level virtualClusters is semantically a set keyed by name — reordering the YAML
        // list is a no-op. The detector indexes by name so this is safe today, but the test
        // pins the behaviour against future refactors.
        var oldConfig = configWith(
                vc("alpha", "kafka:9092", 9192),
                vc("beta", "kafka:9092", 9193));
        var newConfig = configWith(
                vc("beta", "kafka:9092", 9193),
                vc("alpha", "kafka:9092", 9192));
        assertThat(detector.detect(new ConfigurationChangeContext(oldConfig, newConfig)).isEmpty()).isTrue();
    }

    @Test
    void reorderingFiltersOnClusterIsTreatedAsModification() {
        // Negative: filter chain order IS semantically meaningful (execution is sequential),
        // so swapping filter order must be treated as a modification.
        var oldVc = new VirtualCluster("cluster",
                new TargetCluster("kafka:9092", Optional.empty()),
                List.of(gateway("default", 9192)),
                false, false,
                List.of("filter-a", "filter-b"));
        var newVc = new VirtualCluster("cluster",
                new TargetCluster("kafka:9092", Optional.empty()),
                List.of(gateway("default", 9192)),
                false, false,
                List.of("filter-b", "filter-a"));
        // Build configs with the matching named filter definitions so Configuration validation passes.
        var oldConfig = new Configuration(null,
                List.of(filterDef("filter-a"), filterDef("filter-b")),
                null, List.of(oldVc), null, false, Optional.empty(), null, null, null);
        var newConfig = new Configuration(null,
                List.of(filterDef("filter-a"), filterDef("filter-b")),
                null, List.of(newVc), null, false, Optional.empty(), null, null, null);
        var result = detector.detect(new ConfigurationChangeContext(oldConfig, newConfig));
        assertThat(result.clustersToModify()).containsExactly("cluster");
    }

    private static NamedFilterDefinition filterDef(String name) {
        return new NamedFilterDefinition(name, "io.kroxylicious.test.FakeFilter", "");
    }

    @Test
    void detectsCombinedAddRemoveModify() {
        var oldConfig = configWith(
                vc("keep", "kafka-a:9092", 9192),
                vc("remove-me", "kafka-b:9092", 9193),
                vc("modify-me", "kafka-c:9092", 9194));
        var newConfig = configWith(
                vc("keep", "kafka-a:9092", 9192),
                vc("modify-me", "kafka-c-new:9092", 9194),
                vc("add-me", "kafka-d:9092", 9195));
        var result = detector.detect(new ConfigurationChangeContext(oldConfig, newConfig));
        assertThat(result.clustersToAdd()).containsExactly("add-me");
        assertThat(result.clustersToRemove()).containsExactly("remove-me");
        assertThat(result.clustersToModify()).containsExactly("modify-me");
    }

    private static Configuration configWith(VirtualCluster... clusters) {
        return new Configuration(null, null, null, List.of(clusters), null, false,
                Optional.empty(), null, null, null);
    }

    private static VirtualCluster vc(String name, String bootstrap, int gatewayPort) {
        return new VirtualCluster(name,
                new TargetCluster(bootstrap, Optional.empty()),
                List.of(gateway("default", gatewayPort)),
                false,
                false,
                List.of());
    }

    private static VirtualCluster vcWithGateways(String name, List<VirtualClusterGateway> gateways) {
        return new VirtualCluster(name,
                new TargetCluster("kafka:9092", Optional.empty()),
                gateways,
                false,
                false,
                List.of());
    }

    private static VirtualClusterGateway gateway(String gatewayName, int port) {
        return new VirtualClusterGateway(gatewayName,
                new PortIdentifiesNodeIdentificationStrategy(new HostPort("localhost", port), null, null, null),
                null,
                Optional.empty());
    }
}
