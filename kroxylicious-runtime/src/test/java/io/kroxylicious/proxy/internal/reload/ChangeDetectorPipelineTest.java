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

import edu.umd.cs.findbugs.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cross-detector integration tests for the change-detection pipeline. Each individual
 * detector is unit-tested in its own file; this class verifies that running both detectors
 * over the same configuration and merging their results produces a well-formed
 * {@link ChangeResult} — specifically, that the pairwise-disjointness invariant
 * enforced by {@link ChangeResult#merge(ChangeResult)} holds across realistic scenarios
 * where the detectors' outputs overlap.
 */
class ChangeDetectorPipelineTest {

    private final VirtualClusterChangeDetector vccDetector = new VirtualClusterChangeDetector();
    private final FilterChangeDetector filterDetector = new FilterChangeDetector();

    @Test
    void clusterWithBothGatewayAndFilterChangeAppearsOnceInModify() {
        // Cluster has BOTH a gateway-port change (VCC's concern) AND a referenced
        // filter's definition changes (FCD's concern). Each detector independently
        // flags the cluster in clustersToModify; the merge must de-duplicate so the
        // cluster appears exactly once, and never spuriously in add/remove.
        var oldFilter = new NamedFilterDefinition("filter-a", "io.kroxylicious.test.FakeFilter", "config-v1");
        var newFilter = new NamedFilterDefinition("filter-a", "io.kroxylicious.test.FakeFilter", "config-v2");
        var oldVc = new VirtualCluster("cluster",
                new TargetCluster("kafka:9092", Optional.empty()),
                List.of(gateway("default", 9192)),
                false, false, List.of("filter-a"));
        var newVc = new VirtualCluster("cluster",
                new TargetCluster("kafka:9092", Optional.empty()),
                List.of(gateway("default", 9999)), // port changed
                false, false, List.of("filter-a"));
        var oldConfig = new Configuration(null, List.of(oldFilter), null,
                List.of(oldVc), null, false, Optional.empty(), null, null);
        var newConfig = new Configuration(null, List.of(newFilter), null,
                List.of(newVc), null, false, Optional.empty(), null, null);
        var context = new ConfigurationChangeContext(oldConfig, newConfig);

        // Both detectors independently flag the cluster
        var vccResult = vccDetector.detect(context);
        var filterResult = filterDetector.detect(context);
        assertThat(vccResult.clustersToModify()).containsExactly("cluster");
        assertThat(filterResult.clustersToModify()).containsExactly("cluster");

        // Merging de-duplicates: cluster appears in modify exactly once
        var merged = vccResult.merge(filterResult);
        assertThat(merged.clustersToModify()).containsExactly("cluster");
        assertThat(merged.clustersToAdd()).isEmpty();
        assertThat(merged.clustersToRemove()).isEmpty();
    }

    @Test
    void addedClusterReferencingChangedFilterAppearsInAddOnly() {
        // Cluster is newly added in newConfig and references filter-a whose definition
        // also changed. VCC flags it as added; FCD must NOT flag it as modified (it
        // iterates oldConfig clusters and pure additions are out of its scope). If FCD
        // misbehaved here the merge would put the cluster in BOTH add and modify, which
        // ChangeResult.merge() rejects via its disjointness check — so this test
        // exercises both the detector contracts AND the cross-bucket invariant together.
        var oldFilter = new NamedFilterDefinition("filter-a", "io.kroxylicious.test.FakeFilter", "v1");
        var newFilter = new NamedFilterDefinition("filter-a", "io.kroxylicious.test.FakeFilter", "v2");
        var existing = vc("existing", List.of("filter-a"));
        var newlyAdded = vc("newly-added", List.of("filter-a"));
        var oldConfig = new Configuration(null, List.of(oldFilter), null,
                List.of(existing), null, false, Optional.empty(), null, null);
        var newConfig = new Configuration(null, List.of(newFilter), null,
                List.of(existing, newlyAdded), null, false, Optional.empty(), null, null);
        var context = new ConfigurationChangeContext(oldConfig, newConfig);

        var vccResult = vccDetector.detect(context);
        var filterResult = filterDetector.detect(context);

        // VCC flags newly-added; FCD flags existing (filter-a changed and existing uses it)
        assertThat(vccResult.clustersToAdd()).containsExactly("newly-added");
        assertThat(filterResult.clustersToModify()).containsExactly("existing");

        // Merge succeeds (no disjointness violation) and partitions the two clusters
        var merged = vccResult.merge(filterResult);
        assertThat(merged.clustersToAdd()).containsExactly("newly-added");
        assertThat(merged.clustersToModify()).containsExactly("existing");
        assertThat(merged.clustersToRemove()).isEmpty();
    }

    @Test
    void removedClusterReferencingChangedFilterAppearsInRemoveOnly() {
        // Mirror of the add-side test: cluster is removed in newConfig but previously
        // referenced filter-a, whose definition also "changed" (it's gone from filterDefinitions).
        // VCC flags it as removed; FCD must skip it because newCluster is null. If FCD
        // misbehaved and flagged it as modified, the merge would throw on the
        // remove+modify overlap check.
        var oldFilter = new NamedFilterDefinition("filter-a", "io.kroxylicious.test.FakeFilter", "v1");
        var goingAway = vc("going-away", List.of("filter-a"));
        var staying = vcWithoutFilters("staying");
        var oldConfig = new Configuration(null, List.of(oldFilter), null,
                List.of(goingAway, staying), null, false, Optional.empty(), null, null);
        var newConfig = new Configuration(null, null, null,
                List.of(staying), null, false, Optional.empty(), null, null);
        var context = new ConfigurationChangeContext(oldConfig, newConfig);

        var vccResult = vccDetector.detect(context);
        var filterResult = filterDetector.detect(context);

        assertThat(vccResult.clustersToRemove()).containsExactly("going-away");
        assertThat(filterResult.clustersToModify()).isEmpty();

        var merged = vccResult.merge(filterResult);
        assertThat(merged.clustersToRemove()).containsExactly("going-away");
        assertThat(merged.clustersToAdd()).isEmpty();
        assertThat(merged.clustersToModify()).isEmpty();
    }

    @Test
    void fullSpectrumChangePartitionsClustersCorrectly() {
        // Realistic full-reload scenario: one cluster added, one removed, one modified by
        // a gateway change (VCC's concern), one modified by a referenced filter changing
        // (FCD's concern). The merged result must partition all four into the correct
        // buckets with no cross-bucket overlap.
        var oldFilter = new NamedFilterDefinition("filter-x", "io.kroxylicious.test.FakeFilter", "v1");
        var newFilter = new NamedFilterDefinition("filter-x", "io.kroxylicious.test.FakeFilter", "v2");

        var keepUnchanged = vcWithoutFilters("keep");
        var removedCluster = vcWithoutFilters("goes-away");
        var oldGatewayChanged = new VirtualCluster("gateway-changed",
                new TargetCluster("kafka:9092", Optional.empty()),
                List.of(gateway("default", 9192)),
                false, false, List.of());
        var newGatewayChanged = new VirtualCluster("gateway-changed",
                new TargetCluster("kafka:9092", Optional.empty()),
                List.of(gateway("default", 9999)), // gateway port changed
                false, false, List.of());
        var filterChangedCluster = vc("filter-changed", List.of("filter-x"));
        var addedCluster = vcWithoutFilters("added");

        var oldConfig = new Configuration(null, List.of(oldFilter), null,
                List.of(keepUnchanged, removedCluster, oldGatewayChanged, filterChangedCluster),
                null, false, Optional.empty(), null, null);
        var newConfig = new Configuration(null, List.of(newFilter), null,
                List.of(keepUnchanged, newGatewayChanged, filterChangedCluster, addedCluster),
                null, false, Optional.empty(), null, null);
        var context = new ConfigurationChangeContext(oldConfig, newConfig);

        var merged = vccDetector.detect(context).merge(filterDetector.detect(context));

        assertThat(merged.clustersToAdd()).containsExactly("added");
        assertThat(merged.clustersToRemove()).containsExactly("goes-away");
        assertThat(merged.clustersToModify()).containsExactlyInAnyOrder("gateway-changed", "filter-changed");
    }

    @Test
    void identicalConfigsProduceEmptyMergedResult() {
        // No-op case: when the two configs are identical, both detectors return EMPTY
        // and so does the merge. Pinning this against future refactors that might
        // accidentally introduce non-empty defaults in a detector's output.
        var filter = new NamedFilterDefinition("filter-a", "io.kroxylicious.test.FakeFilter", "v1");
        var cluster = vc("cluster", List.of("filter-a"));
        var config = new Configuration(null, List.of(filter), null,
                List.of(cluster), null, false, Optional.empty(), null, null);
        var context = new ConfigurationChangeContext(config, config);

        var vccResult = vccDetector.detect(context);
        var filterResult = filterDetector.detect(context);
        assertThat(vccResult.isEmpty()).isTrue();
        assertThat(filterResult.isEmpty()).isTrue();
        assertThat(vccResult.merge(filterResult).isEmpty()).isTrue();
    }

    @Test
    void modifyBucketUnionsAcrossDetectorsForDifferentClusters() {
        // Each detector flags a different cluster for modify; the merge's union must
        // contain both. A regression in merge's union logic (e.g. accidentally using
        // intersection) would lose one of them — this test pins the union semantic.
        var oldFilter = new NamedFilterDefinition("filter-x", "io.kroxylicious.test.FakeFilter", "v1");
        var newFilter = new NamedFilterDefinition("filter-x", "io.kroxylicious.test.FakeFilter", "v2");

        var oldGatewayChanged = new VirtualCluster("gateway-changed",
                new TargetCluster("kafka:9092", Optional.empty()),
                List.of(gateway("default", 9192)),
                false, false, List.of());
        var newGatewayChanged = new VirtualCluster("gateway-changed",
                new TargetCluster("kafka:9092", Optional.empty()),
                List.of(gateway("default", 9999)),
                false, false, List.of());
        var filterChangedCluster = vc("filter-changed", List.of("filter-x"));

        var oldConfig = new Configuration(null, List.of(oldFilter), null,
                List.of(oldGatewayChanged, filterChangedCluster), null, false, Optional.empty(), null, null);
        var newConfig = new Configuration(null, List.of(newFilter), null,
                List.of(newGatewayChanged, filterChangedCluster), null, false, Optional.empty(), null, null);
        var context = new ConfigurationChangeContext(oldConfig, newConfig);

        var vccResult = vccDetector.detect(context);
        var filterResult = filterDetector.detect(context);
        assertThat(vccResult.clustersToModify()).containsExactly("gateway-changed");
        assertThat(filterResult.clustersToModify()).containsExactly("filter-changed");

        var merged = vccResult.merge(filterResult);
        assertThat(merged.clustersToModify()).containsExactlyInAnyOrder("gateway-changed", "filter-changed");
        assertThat(merged.clustersToAdd()).isEmpty();
        assertThat(merged.clustersToRemove()).isEmpty();
    }

    /**
     * Build a virtual cluster fixture for cross-detector tests.
     *
     * <p>The {@code filters} parameter follows {@link VirtualCluster#filters()}'s
     * three-valued semantics; FCD's behaviour differs across the three cases (see
     * {@code FilterChangeDetectorTest.vc(...)} for the same convention documented
     * there):
     * <ul>
     *   <li>{@code null} &mdash; cluster relies on top-level {@code defaultFilters}.</li>
     *   <li>{@code List.of()} &mdash; cluster has no filter chain (also obtainable
     *       via {@link #vcWithoutFilters(String)} for clarity at the call site).</li>
     *   <li>non-empty &mdash; cluster has an explicit chain.</li>
     * </ul>
     */
    private static VirtualCluster vc(String name, @Nullable List<String> filters) {
        return new VirtualCluster(name,
                new TargetCluster("kafka:9092", Optional.empty()),
                List.of(gateway("default", 9192)),
                false, false, filters);
    }

    /**
     * Build a virtual cluster with no filter chain ({@code List.of()}). Equivalent to
     * {@code vc(name, List.of())}; the named factory makes the intent explicit at call
     * sites where the test is about VCC behaviour and the filter chain is incidental.
     */
    private static VirtualCluster vcWithoutFilters(String name) {
        return new VirtualCluster(name,
                new TargetCluster("kafka:9092", Optional.empty()),
                List.of(gateway("default", 9192)),
                false, false, List.of());
    }

    private static VirtualClusterGateway gateway(String gatewayName, int port) {
        return new VirtualClusterGateway(gatewayName,
                new PortIdentifiesNodeIdentificationStrategy(new HostPort("localhost", port), null, null, null),
                null,
                Optional.empty());
    }
}
