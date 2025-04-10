/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.resolver;

import java.util.Objects;
import java.util.Set;
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
                                      Set<LocalRef<?>> resourcesWithResolvedRefsFalse) {
    public ClusterResolutionResult {
        Objects.requireNonNull(danglingReferences);
    }

    public Stream<LocalRef<?>> findDanglingReferences(LocalRef<?> from, String kindTo) {
        Objects.requireNonNull(from);
        Objects.requireNonNull(kindTo);
        return danglingReferences.stream().filter(r -> r.from().equals(from) && r.to.getKind().equals(kindTo)).map(DanglingReference::to);
    }

    public Stream<LocalRef<?>> findDanglingReferences(String fromKind, String toKind) {
        Objects.requireNonNull(fromKind);
        Objects.requireNonNull(toKind);
        return danglingReferences.stream().filter(r -> r.from().getKind().equals(fromKind) && r.to().getKind().equals(toKind))
                .map(DanglingReference::to);
    }

    public boolean isFullyResolved() {
        return danglingReferences.isEmpty() && resourcesWithResolvedRefsFalse.isEmpty();
    }

    public boolean anyDependenciesNotFoundFor(LocalRef<?> fromResource) {
        return danglingReferences.stream().anyMatch(u -> u.from().equals(fromResource));
    }

    public boolean anyResolvedRefsConditionsFalse() {
        return !resourcesWithResolvedRefsFalse.isEmpty();
    }

    public Stream<LocalRef<?>> findResourcesWithResolvedRefsFalse() {
        return resourcesWithResolvedRefsFalse.stream();
    }

    public Stream<LocalRef<?>> findResourcesWithResolvedRefsFalse(String kind) {
        return findResourcesWithResolvedRefsFalse().filter(r -> r.getKind().equals(kind));
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
