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

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.FilterRef;
import io.kroxylicious.kubernetes.api.common.LocalRef;
import io.kroxylicious.kubernetes.api.common.ProxyRef;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressStatus;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceStatus;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterSpec;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilterStatus;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;
import io.kroxylicious.kubernetes.operator.resolver.ResolutionResult.ClusterResolutionResult;
import io.kroxylicious.kubernetes.operator.resolver.ResolutionResult.UnresolvedReference;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.toLocalRef;
import static io.kroxylicious.kubernetes.operator.resolver.ResolutionResult.DependencyType.DIRECT;
import static io.kroxylicious.kubernetes.operator.resolver.ResolutionResult.DependencyType.TRANSITIVE;

public class DependencyResolverImpl implements DependencyResolver {

    private static final ResolutionResult EMPTY_RESOLUTION_RESULT = new ResolutionResult(Map.of(), Map.of(), Map.of(), Set.of());

    private DependencyResolverImpl() {
    }

    public static DependencyResolver create() {
        return new DependencyResolverImpl();
    }

    @Override
    public ResolutionResult resolveProxyRefs(KafkaProxy proxy, Context<?> context) {
        Objects.requireNonNull(proxy);
        Objects.requireNonNull(context);
        Set<VirtualKafkaCluster> virtualKafkaClusters = context.getSecondaryResources(VirtualKafkaCluster.class);
        return resolve(virtualKafkaClusters, Set.of(proxy), context);
    }

    @Override
    public ResolutionResult resolveClusterRefs(VirtualKafkaCluster cluster, Context<?> context) {
        Objects.requireNonNull(cluster);
        Objects.requireNonNull(context);
        Set<KafkaProxy> proxies = context.getSecondaryResources(KafkaProxy.class);
        return resolve(Set.of(cluster), proxies, context);
    }

    private @NonNull ResolutionResult resolve(Set<VirtualKafkaCluster> virtualKafkaClusters, Set<KafkaProxy> proxies, Context<?> context) {
        if (virtualKafkaClusters.isEmpty()) {
            return EMPTY_RESOLUTION_RESULT;
        }

        // TODO Replace this with the code from the VKCReconciler
        Map<LocalRef<KafkaProxyIngress>, KafkaProxyIngress> ingresses = context.getSecondaryResources(KafkaProxyIngress.class).stream()
                .collect(ResourcesUtil.toByLocalRefMap());
        Map<LocalRef<KafkaService>, KafkaService> clusterRefs = context.getSecondaryResources(KafkaService.class).stream()
                .collect(ResourcesUtil.toByLocalRefMap());
        Map<LocalRef<KafkaProtocolFilter>, KafkaProtocolFilter> filters = context.getSecondaryResources(KafkaProtocolFilter.class).stream()
                .collect(ResourcesUtil.toByLocalRefMap());
        var resolutionResult = virtualKafkaClusters.stream()
                .map(cluster -> determineUnresolvedDependencies(cluster, ingresses, clusterRefs, filters, proxies))
                .collect(Collectors.toSet());
        return new ResolutionResult(filters, ingresses, clusterRefs, resolutionResult);
    }

    private ClusterResolutionResult determineUnresolvedDependencies(VirtualKafkaCluster cluster,
                                                                    Map<LocalRef<KafkaProxyIngress>, KafkaProxyIngress> ingresses,
                                                                    Map<LocalRef<KafkaService>, KafkaService> services,
                                                                    Map<LocalRef<KafkaProtocolFilter>, KafkaProtocolFilter> filters,
                                                                    Set<KafkaProxy> proxies) {
        VirtualKafkaClusterSpec spec = cluster.getSpec();
        LocalRef<VirtualKafkaCluster> clusterRef = toLocalRef(cluster);
        Set<UnresolvedReference> unresolved = new HashSet<>();
        determineUnresolvedIngresses(clusterRef, spec, ingresses).forEach(unresolved::add);
        determineIngressesWithUnresolvedProxies(ingresses, proxies).forEach(unresolved::add);
        determineUnresolvedKafkaProxies(clusterRef, spec, proxies).ifPresent(unresolved::add);
        determineUnresolvedKafkaService(clusterRef, spec, services).ifPresent(unresolved::add);
        determineUnresolvedFilters(clusterRef, spec, filters).forEach(unresolved::add);
        Set<KafkaProxyIngress> ingressesWithResolvedRefsFalse = ingresses.values().stream()
                .filter(i -> hasAnyResolvedRefsFalse(Optional.ofNullable(i.getStatus()).map(KafkaProxyIngressStatus::getConditions).orElse(List.of())))
                .collect(Collectors.toSet());
        Set<KafkaProtocolFilter> filtersWithResolvedRefsFalse = filters.values().stream()
                .filter(i -> hasAnyResolvedRefsFalse(Optional.ofNullable(i.getStatus()).map(KafkaProtocolFilterStatus::getConditions).orElse(List.of())))
                .collect(Collectors.toSet());
        Set<KafkaService> kafkaServicesWithResolvedRefsFalse = services.values().stream()
                .filter(i -> hasAnyResolvedRefsFalse(Optional.ofNullable(i.getStatus()).map(KafkaServiceStatus::getConditions).orElse(List.of())))
                .collect(Collectors.toSet());
        return new ClusterResolutionResult(cluster,
                new ResolutionResult.UnresolvedReferences(unresolved, ingressesWithResolvedRefsFalse, filtersWithResolvedRefsFalse, kafkaServicesWithResolvedRefsFalse));
    }

    private static boolean hasAnyResolvedRefsFalse(List<Condition> conditions) {
        return conditions.stream().anyMatch(Condition::isResolvedRefsFalse);
    }

    private Stream<UnresolvedReference> determineIngressesWithUnresolvedProxies(Map<LocalRef<KafkaProxyIngress>, KafkaProxyIngress> ingresses,
                                                                                Set<KafkaProxy> proxies) {
        Set<LocalRef<KafkaProxy>> localRefs = proxies.stream().map(ResourcesUtil::toLocalRef).collect(Collectors.toSet());
        return ingresses.values().stream().flatMap(ingress -> {
            ProxyRef proxyRef = ingress.getSpec().getProxyRef();
            if (!localRefs.contains(proxyRef)) {
                return Stream.of(new UnresolvedReference(toLocalRef(ingress), proxyRef, TRANSITIVE));
            }
            else {
                return Stream.of();
            }
        });
    }

    private Optional<UnresolvedReference> determineUnresolvedKafkaProxies(LocalRef<VirtualKafkaCluster> clusterRef, VirtualKafkaClusterSpec spec,
                                                                          Set<KafkaProxy> proxies) {
        Set<LocalRef<KafkaProxy>> localRefs = proxies.stream().map(ResourcesUtil::toLocalRef).collect(Collectors.toSet());
        ProxyRef proxyRef = spec.getProxyRef();
        if (!localRefs.contains(proxyRef)) {
            return Optional.of(new UnresolvedReference(clusterRef, proxyRef, DIRECT));
        }
        else {
            return Optional.empty();
        }
    }

    private Stream<UnresolvedReference> determineUnresolvedFilters(LocalRef<VirtualKafkaCluster> clusterRef, VirtualKafkaClusterSpec spec,
                                                                   Map<LocalRef<KafkaProtocolFilter>, KafkaProtocolFilter> filters) {
        List<FilterRef> filtersList = spec.getFilterRefs();
        if (filtersList == null) {
            return Stream.empty();
        }
        else {
            return filtersList.stream()
                    .filter(filterRef -> filters.values().stream().noneMatch(filterResource -> filterResourceMatchesRef(filterRef, filterResource)))
                    .map(ref -> new UnresolvedReference(clusterRef, ref, DIRECT));
        }
    }

    private Optional<UnresolvedReference> determineUnresolvedKafkaService(LocalRef<VirtualKafkaCluster> clusterRef, VirtualKafkaClusterSpec spec,
                                                                          Map<LocalRef<KafkaService>, KafkaService> clusterRefs) {
        var serviceRef = spec.getTargetKafkaServiceRef();
        if (!clusterRefs.containsKey(serviceRef)) {
            return Optional.of(new UnresolvedReference(clusterRef, serviceRef, DIRECT));
        }
        else {
            return Optional.empty();
        }
    }

    private static Stream<UnresolvedReference> determineUnresolvedIngresses(LocalRef<?> clusterRef, VirtualKafkaClusterSpec spec,
                                                                            Map<LocalRef<KafkaProxyIngress>, KafkaProxyIngress> ingresses) {
        return spec.getIngressRefs().stream()
                .filter(ref -> !ingresses.containsKey(ref)).map(ingressRef -> new UnresolvedReference(clusterRef, ingressRef, DIRECT));
    }

    private static boolean filterResourceMatchesRef(FilterRef filterRef, KafkaProtocolFilter filterResource) {
        String apiVersion = filterResource.getApiVersion();
        var filterResourceGroup = apiVersion.substring(0, apiVersion.indexOf("/"));
        return filterResourceGroup.equals(filterRef.getGroup())
                && filterResource.getKind().equals(filterRef.getKind())
                && name(filterResource).equals(filterRef.getName());
    }

}
