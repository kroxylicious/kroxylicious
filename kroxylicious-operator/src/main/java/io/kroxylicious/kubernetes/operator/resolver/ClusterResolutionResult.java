/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.resolver;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.kroxylicious.kubernetes.api.common.LocalRef;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;

/**
 * The result of a deep resolution of the dependencies of a single VirtualKafkaCluster. Describes
 * whether there were any resolution problems like dangling references, or referenced resources
 * that have a ResolvedRefs: False condition.
 */
public record ClusterResolutionResult(VirtualKafkaCluster cluster,
                                      Set<DanglingReference> danglingReferences,
                                      Set<LocalRef<?>> resourcesWithResolvedRefsFalse,
                                      Set<LocalRef<?>> referentsWithStaleStatus) {
    public ClusterResolutionResult {
        Objects.requireNonNull(danglingReferences);
    }

    // using wildcards intentionally
    @SuppressWarnings("java:S1452")
    public Stream<LocalRef<?>> findDanglingReferences(LocalRef<?> from, String kindTo) {
        Objects.requireNonNull(from);
        Objects.requireNonNull(kindTo);
        return danglingReferences.stream().filter(r -> r.from().equals(from) && kindTo.equals(r.to.getKind())).map(DanglingReference::to);
    }

    // using wildcards intentionally
    @SuppressWarnings("java:S1452")
    public Stream<LocalRef<?>> findDanglingReferences(String fromKind, String toKind) {
        Objects.requireNonNull(fromKind);
        Objects.requireNonNull(toKind);
        return danglingReferences.stream().filter(r -> fromKind.equals(r.from().getKind()) && toKind.equals(r.to().getKind()))
                .map(DanglingReference::to);
    }

    /**
     * A result is fully resolved if:
     * <ol>
     *     <li>All references can be retrieved from kubernetes (we have no dangling refs)</li>
     *     <li>No referent has a ResolvedRefs=False condition, which declares that it has unresolved dependencies</li>
     * </ol>
     */
    public boolean isFullyResolved() {
        return danglingReferences.isEmpty() && resourcesWithResolvedRefsFalse.isEmpty();
    }

    /**
     * A result is fully reconciled if all referents have been reconciled.
     * @return true if all referents have been reconciled
     */
    public boolean allReferentsHaveFreshStatus() {
        return referentsWithStaleStatus.isEmpty();
    }

    public boolean anyDependenciesNotFoundFor(LocalRef<?> fromResource) {
        return danglingReferences.stream().anyMatch(u -> u.from().equals(fromResource));
    }

    public boolean anyResolvedRefsConditionsFalse() {
        return !resourcesWithResolvedRefsFalse.isEmpty();
    }

    // using wildcards intentionally
    @SuppressWarnings("java:S1452")
    public Stream<LocalRef<?>> findResourcesWithResolvedRefsFalse() {
        return resourcesWithResolvedRefsFalse.stream();
    }

    // using wildcards intentionally
    @SuppressWarnings("java:S1452")
    public Stream<LocalRef<?>> findReferentsWithStaleStatus() {
        return referentsWithStaleStatus.stream();
    }

    // using wildcards intentionally
    @SuppressWarnings("java:S1452")
    public Stream<LocalRef<?>> findResourcesWithResolvedRefsFalse(String kind) {
        return findResourcesWithResolvedRefsFalse().filter(r -> kind.equals(r.getKind()));
    }

    ClusterResolutionResult addAllResourcesHavingResolvedRefsFalse(Set<LocalRef<?>> resolvedRefsFalse) {
        return new ClusterResolutionResult(cluster, danglingReferences, Stream.concat(resourcesWithResolvedRefsFalse.stream(), resolvedRefsFalse.stream()).collect(
                Collectors.toSet()), referentsWithStaleStatus);
    }

    ClusterResolutionResult addReferentsWithStaleStatus(Set<LocalRef<?>> referentsWithStaleStatus) {
        return new ClusterResolutionResult(cluster, danglingReferences, resourcesWithResolvedRefsFalse,
                Stream.concat(this.referentsWithStaleStatus.stream(), referentsWithStaleStatus.stream()).collect(
                        Collectors.toSet()));
    }

    /**
     * Describes the case where an entity A references an entity B but we can not find B.
     * @param from
     * @param to
     */
    public record DanglingReference(LocalRef<?> from, LocalRef<?> to) {
        public DanglingReference {
            Objects.requireNonNull(from);
            Objects.requireNonNull(to);
        }
    }
}
