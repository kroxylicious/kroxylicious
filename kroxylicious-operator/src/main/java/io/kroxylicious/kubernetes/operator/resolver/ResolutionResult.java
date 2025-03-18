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
import java.util.function.Predicate;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;

import io.kroxylicious.kubernetes.api.common.FilterRef;
import io.kroxylicious.kubernetes.api.common.LocalRef;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;

import static java.util.Comparator.comparing;

/**
 * The result of a deep resolution of the dependencies of a single KafkaProxy. Contains all
 * Filters, KafkaProxyIngresses and KafkaServices that were successfully resolved. It also
 * describes which dependencies could not be resolved per VirtualKafkaCluster.
 */
public class ResolutionResult {
    private final Map<LocalRef<GenericKubernetesResource>, GenericKubernetesResource> filters;
    private final Map<LocalRef<KafkaProxyIngress>, KafkaProxyIngress> kafkaProxyIngresses;
    private final Map<LocalRef<KafkaService>, KafkaService> kafkaServiceRefs;
    private final Map<String, ClusterResolutionResult> clusterResolutionResults;

    public record ClusterResolutionResult(VirtualKafkaCluster cluster, Set<LocalRef<?>> unresolvedDependencySet) {
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

    ResolutionResult(Map<LocalRef<GenericKubernetesResource>, GenericKubernetesResource> filters,
                     Map<LocalRef<KafkaProxyIngress>, KafkaProxyIngress> kafkaProxyIngresses,
                     Map<LocalRef<KafkaService>, KafkaService> kafkaServiceRefs,
                     Map<String, ClusterResolutionResult> clusterResolutionResults) {
        Objects.requireNonNull(filters);
        Objects.requireNonNull(kafkaProxyIngresses);
        Objects.requireNonNull(kafkaServiceRefs);
        Objects.requireNonNull(clusterResolutionResults);
        this.filters = filters;
        this.kafkaProxyIngresses = kafkaProxyIngresses;
        this.kafkaServiceRefs = kafkaServiceRefs;
        this.clusterResolutionResults = clusterResolutionResults;
    }

    /**
     * Get all VirtualKafkaClusters that could have all their dependencies resolved sorted by the VirtualKafkaCluster's metadata.name
     * @return non-null list of VirtualKafkaClusters sorted by metadata.name
     */
    public List<VirtualKafkaCluster> fullyResolvedClustersInNameOrder() {
        return clusterResults(ClusterResolutionResult::isFullyResolved);
    }

    /**
     * Get all VirtualKafkaClusters, even if they have unresolved dependencies, sorted by the VirtualKafkaCluster's metadata.name
     * @return non-null list of VirtualKafkaClusters sorted by metadata.name
     */
    public List<VirtualKafkaCluster> allClustersInNameOrder() {
        return clusterResults(result -> true);
    }

    private List<VirtualKafkaCluster> clusterResults(Predicate<ClusterResolutionResult> include) {
        return clusterResolutionResults.values().stream().filter(include).map(ClusterResolutionResult::cluster)
                .sorted(comparing(ResourcesUtil::name)).toList();
    }

    /**
     * Get all ClusterResolutionResult
     * @return all ClusterResolutionResult
     */
    public Collection<ClusterResolutionResult> clusterResults() {
        return clusterResolutionResults.values();
    }

    /**
     * Get all KafkaProxyIngresses
     * @return all KafkaProxyIngresses
     */
    public Set<KafkaProxyIngress> ingresses() {
        return new HashSet<>(kafkaProxyIngresses.values());
    }

    /**
     * Get KafkaProxyIngress for this reference
     * @param localRef reference
     * @return optional containing ingress if present, else empty
     */
    public Optional<KafkaProxyIngress> ingress(LocalRef<KafkaProxyIngress> localRef) {
        Objects.requireNonNull(localRef);
        return Optional.ofNullable(kafkaProxyIngresses.get(localRef));
    }

    /**
     * Get the resolved KafkaService for a cluster
     * @return optional containing the cluster ref if resolved, else empty
     */
    public Optional<KafkaService> kafkaServiceRef(VirtualKafkaCluster cluster) {
        var ref = cluster.getSpec().getTargetCluster().getClusterRef();
        return Optional.ofNullable(kafkaServiceRefs.get(ref));
    }

    /**
     * Get all resolved Filters
     * @return filters
     */
    public Collection<GenericKubernetesResource> filters() {
        return filters.values();
    }

    /**
     * Get the resolved GenericKubernetesResource for a filterRef
     * @return optional containing the resource if resolved, else empty
     */
    public Optional<GenericKubernetesResource> filter(FilterRef filterRef) {
        return filters().stream()
                .filter(filterResource -> {
                    String apiVersion = filterResource.getApiVersion();
                    var filterResourceGroup = apiVersion.substring(0, apiVersion.indexOf("/"));
                    return filterResourceGroup.equals(filterRef.getGroup())
                            && filterResource.getKind().equals(filterRef.getKind())
                            && ResourcesUtil.name(filterResource).equals(filterRef.getName());
                })
                .findFirst();
    }

}
