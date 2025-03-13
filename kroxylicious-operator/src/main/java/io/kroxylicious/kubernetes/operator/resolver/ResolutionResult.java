/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.resolver;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaClusterRef;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;

import edu.umd.cs.findbugs.annotations.NonNull;

import static java.util.Comparator.comparing;

/**
 * The result of a deep resolution of the dependencies of a single KafkaProxy. Contains all
 * Filters, KafkaProxyIngresses and KafkaClusterRefs that were successfully resolved. Contains
 * a list of unresolved references per VirtualKafkaCluster.
 */
public class ResolutionResult {
    private final Map<String, GenericKubernetesResource> filters;
    private final Map<String, KafkaProxyIngress> kafkaProxyIngresses;
    private final Map<String, KafkaClusterRef> kafkaClusterRefs;
    private final Map<String, ClusterResolutionResult> clusterResolutionResults;

    ResolutionResult(@NonNull Map<String, GenericKubernetesResource> filters,
                     @NonNull Map<String, KafkaProxyIngress> kafkaProxyIngresses,
                     @NonNull Map<String, KafkaClusterRef> kafkaClusterRefs,
                     @NonNull Map<String, ClusterResolutionResult> clusterResolutionResults) {
        Objects.requireNonNull(filters);
        Objects.requireNonNull(kafkaProxyIngresses);
        Objects.requireNonNull(kafkaClusterRefs);
        Objects.requireNonNull(clusterResolutionResults);
        this.filters = filters;
        this.kafkaProxyIngresses = kafkaProxyIngresses;
        this.kafkaClusterRefs = kafkaClusterRefs;
        this.clusterResolutionResults = clusterResolutionResults;
    }

    public Set<KafkaProxyIngress> getIngresses() {
        return new HashSet<>(kafkaProxyIngresses.values());
    }

    public Stream<ClusterResolutionResult> clusterResult() {
        return clusterResolutionResults.values().stream();
    }

    public List<VirtualKafkaCluster> allClustersInNameOrder() {
        return clusterResolutionResults.values().stream().map(ClusterResolutionResult::cluster).sorted(comparing(ResourcesUtil::name)).toList();
    }

    public Collection<GenericKubernetesResource> filters() {
        return filters.values();
    }

    public record UnresolvedDependency(@NonNull Dependency type, @NonNull String name) {
        public UnresolvedDependency {
            Objects.requireNonNull(type);
            Objects.requireNonNull(name);
        }
    }

    public record ClusterResolutionResult(@NonNull VirtualKafkaCluster cluster, @NonNull Set<UnresolvedDependency> unresolvedDependencySet) {
        public ClusterResolutionResult {
            Objects.requireNonNull(cluster);
            Objects.requireNonNull(unresolvedDependencySet);
        }

        public boolean isAnyDependencyUnresolved() {
            return !unresolvedDependencySet.isEmpty();
        }

        public boolean isFullyResolved() {
            return !isAnyDependencyUnresolved();
        }

    }

    public Optional<KafkaClusterRef> kafkaClusterRef(String name) {
        return Optional.ofNullable(kafkaClusterRefs.get(name));
    }

    public Optional<KafkaClusterRef> kafkaClusterRefFor(VirtualKafkaCluster cluster) {
        return kafkaClusterRef(cluster.getSpec().getTargetCluster().getClusterRef().getName());
    }

    public Optional<GenericKubernetesResource> filter(String name) {
        return Optional.ofNullable(filters.get(name));
    }

    public List<VirtualKafkaCluster> fullyResolvedClustersInNameOrder() {
        return clusterResolutionResults.values().stream().filter(ClusterResolutionResult::isFullyResolved).map(ClusterResolutionResult::cluster)
                .sorted(comparing(ResourcesUtil::name)).toList();
    }

}
