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

import io.javaoperatorsdk.operator.api.reconciler.Context;

import io.kroxylicious.kubernetes.api.common.FilterRef;
import io.kroxylicious.kubernetes.api.common.LocalRef;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterSpec;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;
import io.kroxylicious.kubernetes.operator.resolver.ResolutionResult.ClusterResolutionResult;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;

public class DependencyResolverImpl implements DependencyResolver {

    private static final ResolutionResult EMPTY_RESOLUTION_RESULT = new ResolutionResult(Map.of(), Map.of(), Map.of(), Set.of());

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
        Map<LocalRef<KafkaProxyIngress>, KafkaProxyIngress> ingresses = context.getSecondaryResources(KafkaProxyIngress.class).stream()
                .collect(ResourcesUtil.toByLocalRefMap());
        Map<LocalRef<KafkaService>, KafkaService> clusterRefs = context.getSecondaryResources(KafkaService.class).stream()
                .collect(ResourcesUtil.toByLocalRefMap());
        Map<LocalRef<KafkaProtocolFilter>, KafkaProtocolFilter> filters = context.getSecondaryResources(KafkaProtocolFilter.class).stream()
                .collect(ResourcesUtil.toByLocalRefMap());
        var resolutionResult = virtualKafkaClusters.stream().map(cluster -> determineUnresolvedDependencies(cluster, ingresses, clusterRefs, filters))
                .collect(Collectors.toSet());
        ResolutionResult result = new ResolutionResult(filters, ingresses, clusterRefs, resolutionResult);
        reportClustersThatDidNotFullyResolve(result, unresolvedDependencyReporter);
        return result;
    }

    private ClusterResolutionResult determineUnresolvedDependencies(VirtualKafkaCluster cluster,
                                                                    Map<LocalRef<KafkaProxyIngress>, KafkaProxyIngress> ingresses,
                                                                    Map<LocalRef<KafkaService>, KafkaService> clusterRefs,
                                                                    Map<LocalRef<KafkaProtocolFilter>, KafkaProtocolFilter> filters) {
        VirtualKafkaClusterSpec spec = cluster.getSpec();
        Set<LocalRef<?>> unresolvedDependencies = new HashSet<>();
        determineUnresolvedIngresses(spec, ingresses).forEach(unresolvedDependencies::add);
        determineUnresolvedKafkaService(spec, clusterRefs).ifPresent(unresolvedDependencies::add);
        determineUnresolvedFilters(spec, filters).forEach(unresolvedDependencies::add);
        return new ClusterResolutionResult(cluster, unresolvedDependencies);
    }

    private Stream<? extends LocalRef<?>> determineUnresolvedFilters(VirtualKafkaClusterSpec spec,
                                                                     Map<LocalRef<KafkaProtocolFilter>, KafkaProtocolFilter> filters) {
        List<FilterRef> filtersList = spec.getFilterRefs();
        if (filtersList == null) {
            return Stream.empty();
        }
        else {
            return filtersList.stream()
                    .filter(filterRef -> filters.values().stream().noneMatch(filterResource -> filterResourceMatchesRef(filterRef, filterResource)));
        }
    }

    private Optional<LocalRef<?>> determineUnresolvedKafkaService(VirtualKafkaClusterSpec spec,
                                                                  Map<LocalRef<KafkaService>, KafkaService> clusterRefs) {
        var clusterRef = spec.getTargetKafkaServiceRef();
        if (!clusterRefs.containsKey(clusterRef)) {
            return Optional.of(clusterRef);
        }
        else {
            return Optional.empty();
        }
    }

    private static Stream<? extends LocalRef<?>> determineUnresolvedIngresses(VirtualKafkaClusterSpec spec,
                                                                              Map<LocalRef<KafkaProxyIngress>, KafkaProxyIngress> ingresses) {
        return spec.getIngressRefs().stream()
                .filter(ref -> !ingresses.containsKey(ref));
    }

    private static boolean filterResourceMatchesRef(FilterRef filterRef, KafkaProtocolFilter filterResource) {
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
