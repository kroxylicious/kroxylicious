/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.plugin.HasPluginReferences;
import io.kroxylicious.proxy.plugin.PluginReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DependencyGraphTest {

    private static final String IFACE_A = "com.example.A";
    private static final String IFACE_B = "com.example.B";

    /** Config with no plugin references. */
    record LeafConfig(String value) {}

    /** Config that references other plugin instances. */
    record RefConfig(List<PluginReference<?>> refs) implements HasPluginReferences {
        @Override
        public Stream<PluginReference<?>> pluginReferences() {
            return refs.stream();
        }
    }

    // --- No dependencies ---

    @Test
    void emptyGraphReturnsEmptyList() {
        Map<String, Map<String, ResolvedPluginConfig>> resolved = Map.of();
        List<ResolvedPluginConfig> result = DependencyGraph.resolve(resolved);
        assertThat(result).isEmpty();
    }

    @Test
    void singleNodeNoDependencies() {
        var resolved = buildResolved(
                entry(IFACE_A, "alpha", new LeafConfig("v")));

        List<ResolvedPluginConfig> result = DependencyGraph.resolve(resolved);
        assertThat(result).hasSize(1);
        assertThat(result.get(0).pluginInstanceName()).isEqualTo("alpha");
    }

    @Test
    void multipleIndependentNodes() {
        var resolved = buildResolved(
                entry(IFACE_A, "a1", new LeafConfig("1")),
                entry(IFACE_A, "a2", new LeafConfig("2")),
                entry(IFACE_B, "b1", new LeafConfig("3")));

        List<ResolvedPluginConfig> result = DependencyGraph.resolve(resolved);
        assertThat(result).hasSize(3);
    }

    // --- Linear dependencies ---

    @Test
    void linearDependencyOrdersDependencyFirst() {
        // a1 -> a2 (a1 depends on a2)
        var resolved = buildResolved(
                entry(IFACE_A, "a1", new RefConfig(List.of(new PluginReference<>(IFACE_A, "a2")))),
                entry(IFACE_A, "a2", new LeafConfig("leaf")));

        List<ResolvedPluginConfig> result = DependencyGraph.resolve(resolved);
        assertThat(result).hasSize(2);
        assertThat(instanceNames(result)).containsExactly("a2", "a1");
    }

    @Test
    void chainOfThreeDependencies() {
        // a1 -> a2 -> a3
        var resolved = buildResolved(
                entry(IFACE_A, "a1", new RefConfig(List.of(new PluginReference<>(IFACE_A, "a2")))),
                entry(IFACE_A, "a2", new RefConfig(List.of(new PluginReference<>(IFACE_A, "a3")))),
                entry(IFACE_A, "a3", new LeafConfig("leaf")));

        List<ResolvedPluginConfig> result = DependencyGraph.resolve(resolved);
        assertThat(instanceNames(result)).containsExactly("a3", "a2", "a1");
    }

    // --- Cross-interface dependencies ---

    @Test
    void crossInterfaceDependency() {
        // IFACE_A/encrypt -> IFACE_B/kms
        var resolved = buildResolved(
                entry(IFACE_A, "encrypt", new RefConfig(List.of(new PluginReference<>(IFACE_B, "kms")))),
                entry(IFACE_B, "kms", new LeafConfig("aws")));

        List<ResolvedPluginConfig> result = DependencyGraph.resolve(resolved);
        assertThat(instanceNames(result)).containsExactly("kms", "encrypt");
    }

    // --- Diamond dependencies ---

    @Test
    void diamondDependency() {
        // a1 -> a2, a1 -> a3, a2 -> a4, a3 -> a4
        var resolved = buildResolved(
                entry(IFACE_A, "a1", new RefConfig(List.of(
                        new PluginReference<>(IFACE_A, "a2"),
                        new PluginReference<>(IFACE_A, "a3")))),
                entry(IFACE_A, "a2", new RefConfig(List.of(new PluginReference<>(IFACE_A, "a4")))),
                entry(IFACE_A, "a3", new RefConfig(List.of(new PluginReference<>(IFACE_A, "a4")))),
                entry(IFACE_A, "a4", new LeafConfig("leaf")));

        List<ResolvedPluginConfig> result = DependencyGraph.resolve(resolved);
        List<String> names = instanceNames(result);

        // a4 must come before a2 and a3; a2 and a3 must come before a1
        assertThat(names.indexOf("a4")).isLessThan(names.indexOf("a2"));
        assertThat(names.indexOf("a4")).isLessThan(names.indexOf("a3"));
        assertThat(names.indexOf("a2")).isLessThan(names.indexOf("a1"));
        assertThat(names.indexOf("a3")).isLessThan(names.indexOf("a1"));
    }

    // --- Cycle detection ---

    @Test
    void directCycleDetected() {
        // a1 -> a2 -> a1
        var resolved = buildResolved(
                entry(IFACE_A, "a1", new RefConfig(List.of(new PluginReference<>(IFACE_A, "a2")))),
                entry(IFACE_A, "a2", new RefConfig(List.of(new PluginReference<>(IFACE_A, "a1")))));

        assertThatThrownBy(() -> DependencyGraph.resolve(resolved))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dependency cycle");
    }

    @Test
    void selfCycleDetected() {
        // a1 -> a1
        var resolved = buildResolved(
                entry(IFACE_A, "a1", new RefConfig(List.of(new PluginReference<>(IFACE_A, "a1")))));

        assertThatThrownBy(() -> DependencyGraph.resolve(resolved))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dependency cycle");
    }

    @Test
    void transitiveCycleDetected() {
        // a1 -> a2 -> a3 -> a1
        var resolved = buildResolved(
                entry(IFACE_A, "a1", new RefConfig(List.of(new PluginReference<>(IFACE_A, "a2")))),
                entry(IFACE_A, "a2", new RefConfig(List.of(new PluginReference<>(IFACE_A, "a3")))),
                entry(IFACE_A, "a3", new RefConfig(List.of(new PluginReference<>(IFACE_A, "a1")))));

        assertThatThrownBy(() -> DependencyGraph.resolve(resolved))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dependency cycle");
    }

    @Test
    void cycleInSubgraphDetected() {
        // a1 (no deps), a2 -> a3 -> a2
        var resolved = buildResolved(
                entry(IFACE_A, "a1", new LeafConfig("ok")),
                entry(IFACE_A, "a2", new RefConfig(List.of(new PluginReference<>(IFACE_A, "a3")))),
                entry(IFACE_A, "a3", new RefConfig(List.of(new PluginReference<>(IFACE_A, "a2")))));

        assertThatThrownBy(() -> DependencyGraph.resolve(resolved))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dependency cycle");
    }

    // --- Dangling references ---

    @Test
    void danglingReferenceRejected() {
        var resolved = buildResolved(
                entry(IFACE_A, "a1", new RefConfig(List.of(new PluginReference<>(IFACE_B, "missing")))));

        assertThatThrownBy(() -> DependencyGraph.resolve(resolved))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not exist")
                .hasMessageContaining("missing");
    }

    @Test
    void multipleDanglingReferencesAllReported() {
        var resolved = buildResolved(
                entry(IFACE_A, "a1", new RefConfig(List.of(
                        new PluginReference<>(IFACE_B, "missing1"),
                        new PluginReference<>(IFACE_B, "missing2")))));

        assertThatThrownBy(() -> DependencyGraph.resolve(resolved))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("missing1")
                .hasMessageContaining("missing2");
    }

    // --- Helpers ---

    private record Entry(String iface, String name, Object config) {}

    private static Entry entry(String iface, String name, Object config) {
        return new Entry(iface, name, config);
    }

    private static Map<String, Map<String, ResolvedPluginConfig>> buildResolved(Entry... entries) {
        Map<String, Map<String, ResolvedPluginConfig>> result = new HashMap<>();
        for (Entry e : entries) {
            result.computeIfAbsent(e.iface(), k -> new HashMap<>())
                    .put(e.name(), new ResolvedPluginConfig(
                            e.iface(), e.name(),
                            new PluginConfig(e.name(), "TestType", "v1", e.config())));
        }
        return result;
    }

    private static List<String> instanceNames(List<ResolvedPluginConfig> configs) {
        return configs.stream()
                .map(ResolvedPluginConfig::pluginInstanceName)
                .toList();
    }
}
