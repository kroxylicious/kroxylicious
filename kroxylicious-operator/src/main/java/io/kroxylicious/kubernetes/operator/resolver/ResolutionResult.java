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
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaClusterRef;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;

import static java.util.Comparator.comparing;

public class ResolutionResult {
    private final Map<String, GenericKubernetesResource> filters;
    private final Map<String, KafkaProxyIngress> kafkaProxyIngresses;
    private final Map<String, KafkaClusterRef> kafkaClusterRefs;
    private final Map<String, ClusterResolutionResult> clusterResolutionResults;

    ResolutionResult(Map<String, GenericKubernetesResource> filters,
                     Map<String, KafkaProxyIngress> kafkaProxyIngresses,
                     Map<String, KafkaClusterRef> kafkaClusterRefs,
                     Map<String, ClusterResolutionResult> clusterResolutionResults) {

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

    public record UnresolvedDependency(Dependency type, String name) {}

    public record ClusterResolutionResult(VirtualKafkaCluster cluster, Set<UnresolvedDependency> unresolvedDependencySet) {
        public boolean isAnyDependencyUnresolved() {
            return !unresolvedDependencySet.isEmpty();
        }

        public Optional<UnresolvedDependency> firstUnresolvedDependency() {
            return unresolvedDependencySet.stream().findFirst();
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
        return clusterResolutionResults.values().stream().filter(v -> !v.isAnyDependencyUnresolved()).map(ClusterResolutionResult::cluster)
                .sorted(comparing(ResourcesUtil::name)).toList();
    }

}
