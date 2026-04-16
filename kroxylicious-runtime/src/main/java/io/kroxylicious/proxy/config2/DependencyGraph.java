/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config2;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.kroxylicious.proxy.plugin.HasPluginReferences;

/**
 * Builds a dependency graph between plugin instances and computes a valid initialisation order
 * via topological sort. Detects cycles and dangling references, rejecting the entire
 * configuration atomically on error.
 */
class DependencyGraph {

    /** Identifies a plugin instance within the configuration. */
    record PluginInstanceId(String pluginInterfaceName,
                            String pluginInstanceName) {
        @Override
        public String toString() {
            return pluginInterfaceName + "/" + pluginInstanceName;
        }
    }

    private enum Colour {
        WHITE,
        GREY,
        BLACK
    }

    private final Map<PluginInstanceId, ResolvedPluginConfig> nodes;
    private final Map<PluginInstanceId, List<PluginInstanceId>> edges;

    private DependencyGraph(
                            Map<PluginInstanceId, ResolvedPluginConfig> nodes,
                            Map<PluginInstanceId, List<PluginInstanceId>> edges) {
        this.nodes = nodes;
        this.edges = edges;
    }

    /**
     * Builds the graph and computes initialisation order.
     *
     * @param resolved the resolved plugin configs, keyed by interface then instance name
     * @return the plugin instances in dependency order (dependencies before dependents)
     * @throws IllegalArgumentException if there are dangling references or cycles
     */
    static List<ResolvedPluginConfig> resolve(Map<String, Map<String, ResolvedPluginConfig>> resolved) {
        var graph = build(resolved);
        return graph.topologicalSort();
    }

    private static DependencyGraph build(Map<String, Map<String, ResolvedPluginConfig>> resolved) {
        Map<PluginInstanceId, ResolvedPluginConfig> nodes = new LinkedHashMap<>();
        Map<PluginInstanceId, List<PluginInstanceId>> edges = new HashMap<>();
        List<String> errors = new ArrayList<>();

        // Register all nodes
        for (var entry : resolved.entrySet()) {
            for (var instanceEntry : entry.getValue().entrySet()) {
                var id = new PluginInstanceId(entry.getKey(), instanceEntry.getKey());
                nodes.put(id, instanceEntry.getValue());
                edges.put(id, new ArrayList<>());
            }
        }

        // Build edges from HasPluginReferences
        for (var entry : nodes.entrySet()) {
            PluginInstanceId source = entry.getKey();
            Object config = entry.getValue().pc().config();
            if (config instanceof HasPluginReferences hasRefs) {
                hasRefs.pluginReferences().forEach(ref -> {
                    var target = new PluginInstanceId(ref.type(), ref.name());
                    if (!nodes.containsKey(target)) {
                        errors.add(source + " references " + target + " which does not exist");
                    }
                    else {
                        edges.get(source).add(target);
                    }
                });
            }
        }

        if (!errors.isEmpty()) {
            throw new IllegalArgumentException(
                    "Configuration has unresolved plugin references:\n  " + String.join("\n  ", errors));
        }

        return new DependencyGraph(nodes, edges);
    }

    /**
     * DFS-based topological sort with cycle detection. Dependencies appear before dependents
     * in the returned list.
     */
    private List<ResolvedPluginConfig> topologicalSort() {
        Map<PluginInstanceId, Colour> colour = new HashMap<>();
        for (PluginInstanceId id : nodes.keySet()) {
            colour.put(id, Colour.WHITE);
        }

        // Stack tracks the current DFS path for cycle reporting
        Deque<PluginInstanceId> path = new ArrayDeque<>();
        List<ResolvedPluginConfig> result = new ArrayList<>();

        for (PluginInstanceId id : nodes.keySet()) {
            if (colour.get(id) == Colour.WHITE) {
                dfs(id, colour, path, result);
            }
        }

        return Collections.unmodifiableList(result);
    }

    private void dfs(
                     PluginInstanceId node,
                     Map<PluginInstanceId, Colour> colour,
                     Deque<PluginInstanceId> path,
                     List<ResolvedPluginConfig> result) {
        colour.put(node, Colour.GREY);
        path.push(node);

        for (PluginInstanceId neighbour : edges.get(node)) {
            Colour neighbourColour = colour.get(neighbour);
            if (neighbourColour == Colour.GREY) {
                throw new IllegalArgumentException("Configuration has a dependency cycle: " + describeCycle(path, neighbour));
            }
            if (neighbourColour == Colour.WHITE) {
                dfs(neighbour, colour, path, result);
            }
        }

        path.pop();
        colour.put(node, Colour.BLACK);
        result.add(nodes.get(node));
    }

    private static String describeCycle(Deque<PluginInstanceId> path, PluginInstanceId cycleTarget) {
        List<PluginInstanceId> cycle = new ArrayList<>();
        // path is a stack (LIFO), iterate from top to find the cycle
        for (PluginInstanceId id : path) {
            cycle.add(id);
            if (id.equals(cycleTarget)) {
                break;
            }
        }
        // Reverse so the cycle reads in dependency order
        Collections.reverse(cycle);
        return cycle.stream()
                .map(PluginInstanceId::toString)
                .collect(Collectors.joining(" -> "))
                + " -> " + cycleTarget;
    }
}
