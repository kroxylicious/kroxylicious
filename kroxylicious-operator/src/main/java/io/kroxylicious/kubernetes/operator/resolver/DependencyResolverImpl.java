/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.resolver;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.javaoperatorsdk.operator.api.reconciler.Context;

import io.kroxylicious.kubernetes.api.common.LocalRef;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaClusterRef;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterSpec;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.Filters;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;
import io.kroxylicious.kubernetes.operator.resolver.ResolutionResult.ClusterResolutionResult;
import io.kroxylicious.kubernetes.operator.resolver.ResolutionResult.UnresolvedDependency;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.toByNameMap;
import static io.kroxylicious.kubernetes.operator.resolver.Dependency.FILTER;
import static io.kroxylicious.kubernetes.operator.resolver.Dependency.KAFKA_CLUSTER_REF;
import static io.kroxylicious.kubernetes.operator.resolver.Dependency.KAFKA_PROXY_INGRESS;

public class DependencyResolverImpl implements DependencyResolver {

    private static final ResolutionResult EMPTY_RESOLUTION_RESULT = new ResolutionResult(Map.of(), Map.of(), Map.of(), Map.of());

    private DependencyResolverImpl() {
    }

    public static DependencyResolver create() {
        return new DependencyResolverImpl();
    }

    @Override
    public ResolutionResult deepResolve(Context<KafkaProxy> context, UnresolvedDependencyReporter unresolvedDependencyReporter) {
        Objects.requireNonNull(context);
        Set<VirtualKafkaCluster> virtualKafkaClusters = context.getSecondaryResources(VirtualKafkaCluster.class);
        if (virtualKafkaClusters.isEmpty()) {
            return EMPTY_RESOLUTION_RESULT;
        }
        Map<LocalRef<?>, KafkaProxyIngress> ingresses = context.getSecondaryResources(KafkaProxyIngress.class).stream()
                .collect(ResourcesUtil.toByLocalRefMap());
        Map<String, KafkaClusterRef> clusterRefs = context.getSecondaryResources(KafkaClusterRef.class).stream().collect(toByNameMap());
        Map<String, GenericKubernetesResource> filters = context.getSecondaryResources(GenericKubernetesResource.class).stream().collect(toByNameMap());
        var resolutionResult = virtualKafkaClusters.stream().map(cluster -> determineUnresolvedDependencies(cluster, ingresses, clusterRefs, filters))
                .collect(Collectors.toMap(result -> ResourcesUtil.name(result.cluster()), r -> r));
        ResolutionResult result = new ResolutionResult(filters, ingresses, clusterRefs, resolutionResult);
        reportClustersThatDidNotFullyResolve(result, unresolvedDependencyReporter);
        return result;
    }

    private ClusterResolutionResult determineUnresolvedDependencies(VirtualKafkaCluster cluster,
                                                                    Map<LocalRef<?>, KafkaProxyIngress> ingresses,
                                                                    Map<String, KafkaClusterRef> clusterRefs,
                                                                    Map<String, GenericKubernetesResource> filters) {
        VirtualKafkaClusterSpec spec = cluster.getSpec();
        Set<UnresolvedDependency> unresolvedDependencies = new HashSet<>();
        determineUnresolvedIngresses(spec, ingresses).forEach(unresolvedDependencies::add);
        determineUnresolvedKafkaClusterRef(spec, clusterRefs).ifPresent(unresolvedDependencies::add);
        determineUnresolvedFilters(spec, filters).forEach(unresolvedDependencies::add);
        return new ClusterResolutionResult(cluster, unresolvedDependencies);
    }

    private Stream<UnresolvedDependency> determineUnresolvedFilters(VirtualKafkaClusterSpec spec, Map<String, GenericKubernetesResource> filters) {
        List<Filters> filtersList = spec.getFilters();
        if (filtersList == null) {
            return Stream.empty();
        }
        else {
            return filtersList.stream()
                    .filter(filterRef -> filters.values().stream().noneMatch(filterResource -> filterResourceMatchesRef(filterRef, filterResource)))
                    .map(ref -> new UnresolvedDependency(FILTER, ref.getName()));
        }
    }

    private Optional<UnresolvedDependency> determineUnresolvedKafkaClusterRef(VirtualKafkaClusterSpec spec, Map<String, KafkaClusterRef> clusterRefs) {
        String clusterRef = spec.getTargetCluster().getClusterRef().getName();
        if (!clusterRefs.containsKey(clusterRef)) {
            return Optional.of(new UnresolvedDependency(KAFKA_CLUSTER_REF, clusterRef));
        }
        else {
            return Optional.empty();
        }
    }

    private static Stream<UnresolvedDependency> determineUnresolvedIngresses(VirtualKafkaClusterSpec spec, Map<LocalRef<?>, KafkaProxyIngress> ingresses) {
        return spec.getIngressRefs().stream()
                .filter(ref -> !ingresses.containsKey(ref))
                .map(ref -> new UnresolvedDependency(KAFKA_PROXY_INGRESS, ref.getName()));
    }

    private static boolean filterResourceMatchesRef(Filters filterRef, GenericKubernetesResource filterResource) {
        String apiVersion = filterResource.getApiVersion();
        var filterResourceGroup = apiVersion.substring(0, apiVersion.indexOf("/"));
        return filterResourceGroup.equals(filterRef.getGroup())
                && filterResource.getKind().equals(filterRef.getKind())
                && name(filterResource).equals(filterRef.getName());
    }

    private static void reportClustersThatDidNotFullyResolve(ResolutionResult resolutionResult,
                                                             UnresolvedDependencyReporter unresolvedDependencyReporter) {
        resolutionResult.clusterResults().stream()
                .filter(ClusterResolutionResult::isAnyDependencyUnresolved)
                .forEach(clusterResolutionResult -> unresolvedDependencyReporter.reportUnresolvedDependencies(clusterResolutionResult.cluster(),
                        clusterResolutionResult.unresolvedDependencySet()));
    }

}
