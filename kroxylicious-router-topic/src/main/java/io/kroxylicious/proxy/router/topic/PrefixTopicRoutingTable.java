/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.router.topic;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A {@link TopicRoutingTable} that maps topics to routes by exact name match
 * or prefix matching. Explicit topic names take precedence over prefix matches.
 * Prefixes must be disjoint: no prefix may be a prefix of another prefix
 * assigned to a different route. Prefix lookup is O(log n + k) where n is the
 * number of prefixes and k is the length of the longest prefix.
 */
public class PrefixTopicRoutingTable implements TopicRoutingTable {

    private final Map<String, String> topicToRoute;
    private final String[] sortedPrefixes;
    private final String[] routesByIndex;
    private final Set<String> allRoutes;
    @Nullable
    private final String defaultRoute;

    private PrefixTopicRoutingTable(Map<String, String> topicToRoute,
                                    String[] sortedPrefixes,
                                    String[] routesByIndex,
                                    Set<String> allRoutes,
                                    @Nullable String defaultRoute) {
        this.topicToRoute = topicToRoute;
        this.sortedPrefixes = sortedPrefixes;
        this.routesByIndex = routesByIndex;
        this.allRoutes = allRoutes;
        this.defaultRoute = defaultRoute;
    }

    /**
     * Creates a new router table with prefix matching only.
     *
     * @param prefixToRoute map from topic name prefix to route name
     * @param defaultRoute route for topics matching no prefix, or null to reject them
     * @throws IllegalArgumentException if any prefix is a prefix of another prefix on a different route
     */
    public static PrefixTopicRoutingTable create(Map<String, String> prefixToRoute,
                                                 @Nullable String defaultRoute) {
        return create(prefixToRoute, Map.of(), defaultRoute);
    }

    /**
     * Creates a new router table with both explicit topic names and prefix matching.
     * Explicit names take precedence over prefix matches.
     *
     * @param prefixToRoute map from topic name prefix to route name
     * @param topicToRoute map from explicit topic name to route name
     * @param defaultRoute route for topics matching no prefix or explicit name, or null to reject them
     * @throws IllegalArgumentException if prefixes are not disjoint across routes,
     *         or if the same explicit topic name is assigned to different routes
     */
    public static PrefixTopicRoutingTable create(Map<String, String> prefixToRoute,
                                                 Map<String, String> topicToRoute,
                                                 @Nullable String defaultRoute) {
        Objects.requireNonNull(prefixToRoute, "prefixToRoute");
        Objects.requireNonNull(topicToRoute, "topicToRoute");
        if (prefixToRoute.isEmpty() && topicToRoute.isEmpty() && defaultRoute == null) {
            throw new IllegalArgumentException(
                    "At least one prefix, explicit topic, or a default route must be configured");
        }

        var sorted = new LinkedHashMap<String, String>();
        prefixToRoute.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(e -> sorted.put(e.getKey(), e.getValue()));

        validateDisjointness(sorted);

        String[] prefixes = sorted.keySet().toArray(String[]::new);
        String[] routes = sorted.values().toArray(String[]::new);
        var routeSet = new HashSet<>(sorted.values());
        routeSet.addAll(topicToRoute.values());
        if (defaultRoute != null) {
            routeSet.add(defaultRoute);
        }
        Set<String> allRoutes = Set.copyOf(routeSet);

        return new PrefixTopicRoutingTable(
                Map.copyOf(topicToRoute), prefixes, routes, allRoutes, defaultRoute);
    }

    private static void validateDisjointness(LinkedHashMap<String, String> sorted) {
        String[] keys = sorted.keySet().toArray(String[]::new);
        String[] values = sorted.values().toArray(String[]::new);
        for (int i = 0; i < keys.length - 1; i++) {
            if (keys[i + 1].startsWith(keys[i]) && !values[i].equals(values[i + 1])) {
                throw new IllegalArgumentException(
                        "Prefix '" + keys[i] + "' (route: " + values[i]
                                + ") is a prefix of '" + keys[i + 1] + "' (route: " + values[i + 1]
                                + "). Prefixes on different routes must be disjoint.");
            }
        }
    }

    @Override
    @Nullable
    public String routeForTopic(String topicName) {
        if (topicName == null) {
            return null;
        }
        String exactRoute = topicToRoute.get(topicName);
        if (exactRoute != null) {
            return exactRoute;
        }
        int idx = Arrays.binarySearch(sortedPrefixes, topicName);
        if (idx >= 0) {
            return routesByIndex[idx];
        }
        int insertionPoint = -idx - 1;
        if (insertionPoint > 0) {
            String candidate = sortedPrefixes[insertionPoint - 1];
            if (topicName.startsWith(candidate)) {
                return routesByIndex[insertionPoint - 1];
            }
        }
        return defaultRoute;
    }

    @Override
    public Set<String> allRoutes() {
        return allRoutes;
    }
}
