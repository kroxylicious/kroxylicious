/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.reload;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

class FilterChangeDetectorTest {

    private final FilterChangeDetector detector = new FilterChangeDetector();

    @Test
    void emptyWhenNeitherFiltersNorDefaultsChanged() {
        var filterA = filterDef("filter-a", "config-v1");
        var oldConfig = configWith(List.of(filterA), List.of("filter-a"), vc("cluster-a", null));
        var newConfig = configWith(List.of(filterA), List.of("filter-a"), vc("cluster-a", null));
        assertThat(detector.detect(new ConfigurationChangeContext(oldConfig, newConfig)).isEmpty()).isTrue();
    }

    @Test
    void detectsClusterReferencingModifiedFilter() {
        var oldFilter = filterDef("filter-a", "config-v1");
        var newFilter = filterDef("filter-a", "config-v2");
        var oldConfig = configWith(List.of(oldFilter), null, vc("cluster-a", List.of("filter-a")));
        var newConfig = configWith(List.of(newFilter), null, vc("cluster-a", List.of("filter-a")));
        var result = detector.detect(new ConfigurationChangeContext(oldConfig, newConfig));
        assertThat(result.clustersToModify()).containsExactly("cluster-a");
        assertThat(result.clustersToAdd()).isEmpty();
        assertThat(result.clustersToRemove()).isEmpty();
    }

    @Test
    void ignoresClustersNotReferencingChangedFilter() {
        var oldFilterA = filterDef("filter-a", "config-v1");
        var newFilterA = filterDef("filter-a", "config-v2");
        var filterB = filterDef("filter-b", "config-b");
        var oldConfig = configWith(List.of(oldFilterA, filterB), null,
                vc("referencing-a", List.of("filter-a")),
                vc("referencing-b", List.of("filter-b")));
        var newConfig = configWith(List.of(newFilterA, filterB), null,
                vc("referencing-a", List.of("filter-a")),
                vc("referencing-b", List.of("filter-b")));
        var result = detector.detect(new ConfigurationChangeContext(oldConfig, newConfig));
        assertThat(result.clustersToModify()).containsExactly("referencing-a");
    }

    @Test
    void detectsClusterRelyingOnChangedDefaultFilters() {
        var filterA = filterDef("filter-a", "config");
        var filterB = filterDef("filter-b", "config");
        // pinner-b references filter-b so the config validator is happy in both old/new.
        var oldConfig = configWith(List.of(filterA, filterB), List.of("filter-a"),
                vc("using-defaults", null),
                vc("pinner-b", List.of("filter-b")));
        var newConfig = configWith(List.of(filterA, filterB), List.of("filter-a", "filter-b"),
                vc("using-defaults", null),
                vc("pinner-b", List.of("filter-b")));
        var result = detector.detect(new ConfigurationChangeContext(oldConfig, newConfig));
        assertThat(result.clustersToModify()).containsExactly("using-defaults");
    }

    @Test
    void detectsDefaultFilterReorderingAsModification() {
        var filterA = filterDef("filter-a", "config");
        var filterB = filterDef("filter-b", "config");
        var oldConfig = configWith(List.of(filterA, filterB), List.of("filter-a", "filter-b"),
                vc("using-defaults", null));
        var newConfig = configWith(List.of(filterA, filterB), List.of("filter-b", "filter-a"),
                vc("using-defaults", null));
        var result = detector.detect(new ConfigurationChangeContext(oldConfig, newConfig));
        assertThat(result.clustersToModify()).containsExactly("using-defaults");
    }

    @Test
    void defaultFilterChangeDoesNotAffectClustersWithExplicitFilters() {
        var filterA = filterDef("filter-a", "config");
        var filterB = filterDef("filter-b", "config");
        // pinner-b references filter-b so the config validator is happy in both old/new.
        var oldConfig = configWith(List.of(filterA, filterB), List.of("filter-a"),
                vc("explicit", List.of("filter-a")),
                vc("pinner-b", List.of("filter-b")));
        var newConfig = configWith(List.of(filterA, filterB), List.of("filter-a", "filter-b"),
                vc("explicit", List.of("filter-a")),
                vc("pinner-b", List.of("filter-b")));
        var result = detector.detect(new ConfigurationChangeContext(oldConfig, newConfig));
        // "explicit" uses filter-a whose contents didn't change — unaffected even though
        // defaultFilters changed. "pinner-b" uses filter-b whose contents didn't change either.
        assertThat(result.isEmpty()).isTrue();
    }

    @Test
    void reorderingFilterDefinitionsAtTopLevelDoesNotTriggerChange() {
        // Top-level filterDefinitions is semantically a set keyed by name — reordering the YAML
        // list is a no-op. The detector indexes definitions by name before diffing, so the
        // comparison is order-insensitive.
        var filterA = filterDef("filter-a", "config-a");
        var filterB = filterDef("filter-b", "config-b");
        var oldConfig = configWith(List.of(filterA, filterB), null,
                vc("uses-a", List.of("filter-a")),
                vc("uses-b", List.of("filter-b")));
        var newConfig = configWith(List.of(filterB, filterA), null,
                vc("uses-a", List.of("filter-a")),
                vc("uses-b", List.of("filter-b")));
        assertThat(detector.detect(new ConfigurationChangeContext(oldConfig, newConfig)).isEmpty()).isTrue();
    }

    @Test
    void reorderingKeysInsideFilterConfigDoesNotTriggerChange() {
        // Filter plugin config is Jackson-deserialised to Map<String, Object>, whose equals() is
        // order-insensitive. Two YAML blocks with the same keys/values but different key order
        // should be equivalent from the change detector's perspective.
        var oldMap = new LinkedHashMap<String, Object>();
        oldMap.put("key-a", 1);
        oldMap.put("key-b", 2);
        var newMap = new LinkedHashMap<String, Object>();
        newMap.put("key-b", 2);
        newMap.put("key-a", 1);

        var oldFilter = new NamedFilterDefinition("filter-a", "io.kroxylicious.test.FakeFilter", oldMap);
        var newFilter = new NamedFilterDefinition("filter-a", "io.kroxylicious.test.FakeFilter", newMap);
        var oldConfig = configWith(List.of(oldFilter), null, vc("cluster", List.of("filter-a")));
        var newConfig = configWith(List.of(newFilter), null, vc("cluster", List.of("filter-a")));
        assertThat(detector.detect(new ConfigurationChangeContext(oldConfig, newConfig)).isEmpty()).isTrue();
    }

    @Test
    void changingAValueInsideFilterConfigTriggersChange() {
        // Sanity negative: same keys but a different value should flag the cluster as modified.
        var oldFilter = new NamedFilterDefinition("filter-a", "io.kroxylicious.test.FakeFilter",
                Map.of("key", "old-value"));
        var newFilter = new NamedFilterDefinition("filter-a", "io.kroxylicious.test.FakeFilter",
                Map.of("key", "new-value"));
        var oldConfig = configWith(List.of(oldFilter), null, vc("cluster", List.of("filter-a")));
        var newConfig = configWith(List.of(newFilter), null, vc("cluster", List.of("filter-a")));
        assertThat(detector.detect(new ConfigurationChangeContext(oldConfig, newConfig)).clustersToModify())
                .containsExactly("cluster");
    }

    @Test
    void removedClustersAreNotIncluded() {
        var oldFilterA = filterDef("filter-a", "v1");
        var newFilterA = filterDef("filter-a", "v2");
        var oldConfig = configWith(List.of(oldFilterA), null,
                vc("stays", List.of("filter-a")),
                vc("goes-away", List.of("filter-a")));
        var newConfig = configWith(List.of(newFilterA), null,
                vc("stays", List.of("filter-a")));
        var result = detector.detect(new ConfigurationChangeContext(oldConfig, newConfig));
        assertThat(result.clustersToModify()).containsExactly("stays");
        // "goes-away" is VirtualClusterChangeDetector's concern, not ours
        assertThat(result.clustersToRemove()).isEmpty();
    }

    private static Configuration configWith(@Nullable List<NamedFilterDefinition> filterDefs,
                                            @Nullable List<String> defaultFilters,
                                            VirtualCluster... clusters) {
        return new Configuration(null, filterDefs, defaultFilters, List.of(clusters), null, false,
                Optional.empty(), null, null);
    }

    private static NamedFilterDefinition filterDef(String name, String opaqueConfig) {
        return new NamedFilterDefinition(name, "io.kroxylicious.test.FakeFilter", opaqueConfig);
    }

    /**
     * Build a virtual cluster fixture.
     *
     * <p>The {@code filters} parameter follows {@link VirtualCluster#filters()}'s
     * three-valued semantics, which {@link FilterChangeDetector} treats distinctly:
     * <ul>
     *   <li>{@code null} &mdash; cluster relies on the top-level {@code defaultFilters};
     *       FCD's defaultFilters-changed path applies.</li>
     *   <li>{@code List.of()} &mdash; cluster has no filter chain at all; FCD will not
     *       flag the cluster regardless of filter-definition changes.</li>
     *   <li>non-empty list &mdash; cluster has an explicit chain; FCD flags the cluster
     *       if any of the named filters' definitions change.</li>
     * </ul>
     */
    private static VirtualCluster vc(String name, @Nullable List<String> filters) {
        var gateway = new VirtualClusterGateway("default",
                new PortIdentifiesNodeIdentificationStrategy(new HostPort("localhost", 9192), null, null, null),
                null,
                Optional.empty());
        return new VirtualCluster(name,
                new TargetCluster("kafka:9092", Optional.empty()),
                List.of(gateway),
                false,
                false,
                filters);
    }

    @Test
    void detectsAddedFilterDefinitionReferencedByCluster() {
        // Cluster's filters list grows to include a newly-added filter definition. The
        // cluster's own filter-list change is also picked up by VirtualClusterChangeDetector;
        // here we assert that FilterChangeDetector flags it via the "filter-b is in
        // changedFilterNames because it's newly defined" path.
        var filterA = filterDef("filter-a", "config");
        var filterB = filterDef("filter-b", "config");
        var oldConfig = configWith(List.of(filterA), null, vc("cluster", List.of("filter-a")));
        var newConfig = configWith(List.of(filterA, filterB), null,
                vc("cluster", List.of("filter-a", "filter-b")));
        var result = detector.detect(new ConfigurationChangeContext(oldConfig, newConfig));
        assertThat(result.clustersToModify()).containsExactly("cluster");
    }

    @Test
    void detectsRemovedFilterDefinitionReferencedViaDefaults() {
        // Cluster uses defaultFilters; defaults removes filter-b along with its definition.
        // The cluster is flagged because defaultFiltersChanged is true (which covers the
        // "removed filter definition that was in defaults" path).
        var filterA = filterDef("filter-a", "config");
        var filterB = filterDef("filter-b", "config");
        var oldConfig = configWith(List.of(filterA, filterB), List.of("filter-a", "filter-b"),
                vc("cluster", null));
        var newConfig = configWith(List.of(filterA), List.of("filter-a"),
                vc("cluster", null));
        var result = detector.detect(new ConfigurationChangeContext(oldConfig, newConfig));
        assertThat(result.clustersToModify()).containsExactly("cluster");
    }

    @Test
    void detectsFilterTypeChange() {
        // Same filter name, different class name (`type` field). Every cluster referencing
        // the filter must restart.
        var oldFilter = new NamedFilterDefinition("filter-a", "io.kroxylicious.test.OldFilter", "config");
        var newFilter = new NamedFilterDefinition("filter-a", "io.kroxylicious.test.NewFilter", "config");
        var oldConfig = configWith(List.of(oldFilter), null, vc("cluster", List.of("filter-a")));
        var newConfig = configWith(List.of(newFilter), null, vc("cluster", List.of("filter-a")));
        var result = detector.detect(new ConfigurationChangeContext(oldConfig, newConfig));
        assertThat(result.clustersToModify()).containsExactly("cluster");
    }

    @Test
    void ignoresClusterWithBothFiltersAndDefaultFiltersNull() {
        // A cluster with no filter chain at all (filters=null AND no defaultFilters in
        // either config) should never be flagged, even if other filter definitions change.
        // Configuration validation forbids unused filter definitions, so the only-used
        // filter is on a different cluster.
        var oldFilter = filterDef("filter-a", "v1");
        var newFilter = filterDef("filter-a", "v2");
        var oldConfig = configWith(List.of(oldFilter), null,
                vc("isolated", null),
                vc("uses-a", List.of("filter-a")));
        var newConfig = configWith(List.of(newFilter), null,
                vc("isolated", null),
                vc("uses-a", List.of("filter-a")));
        var result = detector.detect(new ConfigurationChangeContext(oldConfig, newConfig));
        // "uses-a" is flagged because filter-a's content changed; "isolated" is not — it
        // has no filter chain so a filter-definition change can't affect it.
        assertThat(result.clustersToModify()).containsExactly("uses-a");
    }

    @Test
    void newlyAddedClustersAreNotIncludedInModify() {
        // FilterChangeDetector iterates oldConfig clusters; pure additions are
        // VirtualClusterChangeDetector's concern. Even when an added cluster references a
        // filter whose definition changed, FilterChangeDetector must not flag it.
        var oldFilter = filterDef("filter-a", "v1");
        var newFilter = filterDef("filter-a", "v2");
        var oldConfig = configWith(List.of(oldFilter), null, vc("existing", List.of("filter-a")));
        var newConfig = configWith(List.of(newFilter), null,
                vc("existing", List.of("filter-a")),
                vc("newly-added", List.of("filter-a")));
        var result = detector.detect(new ConfigurationChangeContext(oldConfig, newConfig));
        // "existing" is flagged because filter-a changed and existing references it.
        // "newly-added" is NOT in clustersToModify even though it also references filter-a —
        // pure additions are out of FilterChangeDetector's scope.
        assertThat(result.clustersToModify()).containsExactly("existing");
        assertThat(result.clustersToAdd()).isEmpty();
    }
}
