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
import java.util.stream.Stream;

import io.kroxylicious.kubernetes.api.common.FilterRef;
import io.kroxylicious.kubernetes.api.common.LocalRef;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;

import static java.util.Comparator.comparing;

/**
 * The result of a deep resolution of the dependencies of a single KafkaProxy. Contains all
 * Filters, KafkaProxyIngresses and KafkaServices that were successfully resolved. It also
 * describes which dependencies could not be resolved per VirtualKafkaCluster.
 */
public class ResolutionResult {
    private final Map<LocalRef<KafkaProtocolFilter>, KafkaProtocolFilter> filters;
    private final Map<LocalRef<KafkaProxyIngress>, KafkaProxyIngress> kafkaProxyIngresses;
    private final Map<LocalRef<KafkaService>, KafkaService> kafkaServiceRefs;
    private final Set<ClusterResolutionResult> clusterResolutionResults;

    public Optional<ClusterResolutionResult> clusterResult(VirtualKafkaCluster cluster) {
        return clusterResolutionResults.stream()
                .filter(r -> r.cluster == cluster)
                .findFirst();
    }

    public record UnresolvedReferences(Set<DanglingReference> danglingReferences,
                                       Set<LocalRef<?>> resourcesWithResolvedRefsFalse) {
        public UnresolvedReferences {
            Objects.requireNonNull(danglingReferences);
        }

        public Stream<LocalRef<?>> findDanglingReferences(LocalRef<?> from, String kindTo) {
            Objects.requireNonNull(from);
            Objects.requireNonNull(kindTo);
            return danglingReferences.stream().filter(r -> r.from.equals(from) && r.to.getKind().equals(kindTo)).map(DanglingReference::to);
        }

        public Stream<LocalRef<?>> findDanglingReferences(String fromKind, String toKind) {
            Objects.requireNonNull(fromKind);
            Objects.requireNonNull(toKind);
            return danglingReferences.stream().filter(r -> r.from.getKind().equals(fromKind) && r.to.getKind().equals(toKind)).map(DanglingReference::to);
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

    public record ClusterResolutionResult(VirtualKafkaCluster cluster, UnresolvedReferences unresolvedReferences) {
        public ClusterResolutionResult {
            Objects.requireNonNull(cluster);
            Objects.requireNonNull(unresolvedReferences);
        }

        public boolean isFullyResolved() {
            return unresolvedReferences.isFullyResolved();
        }

    }

    ResolutionResult(Map<LocalRef<KafkaProtocolFilter>, KafkaProtocolFilter> filters,
                     Map<LocalRef<KafkaProxyIngress>, KafkaProxyIngress> kafkaProxyIngresses,
                     Map<LocalRef<KafkaService>, KafkaService> kafkaServiceRefs,
                     Set<ClusterResolutionResult> clusterResolutionResults) {
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
        return clusterResolutionResults.stream().filter(include).map(ClusterResolutionResult::cluster)
                .sorted(comparing(ResourcesUtil::name)).toList();
    }

    /**
     * Get all ClusterResolutionResult
     * @return all ClusterResolutionResult
     */
    public Collection<ClusterResolutionResult> clusterResults() {
        return clusterResolutionResults;
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
        var ref = cluster.getSpec().getTargetKafkaServiceRef();
        return Optional.ofNullable(kafkaServiceRefs.get(ref));
    }

    /**
     * Get all resolved Filters
     * @return filters
     */
    public Collection<KafkaProtocolFilter> filters() {
        return filters.values();
    }

    /**
     * Get the resolved GenericKubernetesResource for a filterRef
     * @return optional containing the resource if resolved, else empty
     */
    public Optional<KafkaProtocolFilter> filter(FilterRef filterRef) {
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
