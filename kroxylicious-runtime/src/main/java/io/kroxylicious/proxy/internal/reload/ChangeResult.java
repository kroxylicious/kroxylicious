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
     *
     * <p>Each individual detector's output is disjoint by construction (names are partitioned
     * by set membership in old/new configurations), but the union of two disjoint results can
     * produce cross-bucket overlap &mdash; e.g. detector A flags {@code vc-1} as
     * {@code modify} while detector B independently flags {@code vc-1} as {@code add}.
     * {@code merge} is the place where such overlap can arise in this code's flow, so the
     * disjointness invariant is enforced here.
     *
     * @throws IllegalArgumentException if the union of the two results would place the same
     *         cluster name in more than one bucket; the message names the offending pair and
     *         the overlapping cluster names
     */
    public ChangeResult merge(ChangeResult other) {
        Set<String> mergedAdd = union(clustersToAdd, other.clustersToAdd);
        Set<String> mergedRemove = union(clustersToRemove, other.clustersToRemove);
        Set<String> mergedModify = union(clustersToModify, other.clustersToModify);

        validateDisjoint("clustersToAdd", mergedAdd, "clustersToRemove", mergedRemove);
        validateDisjoint("clustersToAdd", mergedAdd, "clustersToModify", mergedModify);
        validateDisjoint("clustersToRemove", mergedRemove, "clustersToModify", mergedModify);

        return new ChangeResult(mergedAdd, mergedRemove, mergedModify);
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

    private static void validateDisjoint(String nameA, Set<String> a, String nameB, Set<String> b) {
        if (a.isEmpty() || b.isEmpty()) {
            return;
        }
        Set<String> overlap = new HashSet<>(a);
        overlap.retainAll(b);
        if (!overlap.isEmpty()) {
            throw new IllegalArgumentException(
                    "merge produced overlapping buckets: " + nameA + " and " + nameB
                            + " must be disjoint; overlapping clusters: " + overlap);
        }
    }
}
