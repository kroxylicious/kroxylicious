/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.reload;

import java.util.HashSet;
import java.util.Set;

/**
 * Aggregated output of a {@link ChangeDetector}: which virtual clusters should be added,
 * removed, or modified to bring the running proxy from the old configuration to the new one.
 * <p>
 * The three sets are expected to be disjoint for a single detector's output — a cluster
 * cannot simultaneously be added and removed. When aggregating results from multiple detectors
 * via {@link #merge(ChangeResult)} the union is taken for each bucket; overlap across buckets
 * is not expected in practice (detectors operate on the same old/new pair).
 *
 * @param clustersToAdd    names of virtual clusters present in new but not in old
 * @param clustersToRemove names of virtual clusters present in old but not in new
 * @param clustersToModify names of virtual clusters whose configuration differs between old and new
 */
public record ChangeResult(Set<String> clustersToAdd,
                           Set<String> clustersToRemove,
                           Set<String> clustersToModify) {

    public static final ChangeResult EMPTY = new ChangeResult(Set.of(), Set.of(), Set.of());

    public ChangeResult {
        clustersToAdd = Set.copyOf(clustersToAdd);
        clustersToRemove = Set.copyOf(clustersToRemove);
        clustersToModify = Set.copyOf(clustersToModify);
    }

    /**
     * Whether this result represents any change at all.
     */
    public boolean isEmpty() {
        return clustersToAdd.isEmpty() && clustersToRemove.isEmpty() && clustersToModify.isEmpty();
    }

    /**
     * Union-merges another result into this one. Used by the orchestrator to aggregate
     * results from multiple detectors.
     */
    public ChangeResult merge(ChangeResult other) {
        return new ChangeResult(
                union(clustersToAdd, other.clustersToAdd),
                union(clustersToRemove, other.clustersToRemove),
                union(clustersToModify, other.clustersToModify));
    }

    private static Set<String> union(Set<String> a, Set<String> b) {
        if (a.isEmpty()) {
            return b;
        }
        if (b.isEmpty()) {
            return a;
        }
        var merged = new HashSet<>(a);
        merged.addAll(b);
        return Set.copyOf(merged);
    }
}
